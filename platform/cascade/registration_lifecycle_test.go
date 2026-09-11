package cascade

import (
	"context"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ghettovoice/gosip/sip"
	"github.com/mickeyzzc/gb28181-go/platform"
	"github.com/stretchr/testify/require"
)

func TestRegisterRetryJitterIsInjectableAndBounded(t *testing.T) {
	cfg := testCfg()
	cfg.RegisterRetryBase = "100ms"
	cfg.RegisterRetryMax = "150ms"
	svc := New(cfg, fakeSource{}, nil)
	var calls atomic.Int32
	svc.SetRegisterRetryRandomSource(func(n int64) int64 {
		calls.Add(1)
		return n - 1
	})

	require.Equal(t, 110*time.Millisecond, svc.jitterRetry(100*time.Millisecond))
	require.Equal(t, 150*time.Millisecond, svc.jitterRetry(150*time.Millisecond),
		"jitter must never exceed the configured retry ceiling")
	require.Equal(t, int32(2), calls.Load(), "capped waits retain bounded jitter")
}

func TestStaleRegistrationCannotRestoreOnlineState(t *testing.T) {
	svc := New(testCfg(), fakeSource{}, nil)
	u := svc.uppers[0]
	u.generation.Add(1)

	require.False(t, svc.setOnlineForGeneration(u, true, 0))
	require.False(t, upperOnline(svc, u))
}

func TestRegistrationAgeUsesInjectableClock(t *testing.T) {
	svc := New(testCfg(), fakeSource{}, nil)
	u := svc.uppers[0]
	base := time.Unix(100, 0)
	svc.now = func() time.Time { return base }
	svc.setOnline(u, true)
	svc.now = func() time.Time { return base.Add(3 * time.Second) }

	age, ok := svc.RegistrationSince()
	require.True(t, ok)
	require.Equal(t, 3*time.Second, age)
}

func TestRetryCeilingBelowBaseIsNormalized(t *testing.T) {
	cfg := testCfg()
	cfg.RegisterRetryBase = "200ms"
	cfg.RegisterRetryMax = "100ms"
	svc := New(cfg, fakeSource{}, nil)

	require.Equal(t, 200*time.Millisecond, svc.retryBase)
	require.Equal(t, svc.retryBase, svc.retryMax)
	require.GreaterOrEqual(t, svc.jitterRetry(svc.retryBase), 180*time.Millisecond)
	require.LessOrEqual(t, svc.jitterRetry(svc.retryBase), svc.retryMax)
}

func TestOfflineUpperConvergesOnlyItsDialogsAndSubscription(t *testing.T) {
	cfg := testCfg()
	cfg.Upstreams = []Upstream{{
		ServerDomain: "34020000002000000002",
		ServerAddr:   "127.0.0.2:5060",
	}}
	svc := New(cfg, fakeSource{}, nil)
	u1, u2 := svc.uppers[0], svc.uppers[1]
	hub1, hub2 := platform.NewFrameHub(), platform.NewFrameHub()
	conn1, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4zero, Port: 0})
	require.NoError(t, err)
	conn2, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4zero, Port: 0})
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn1.Close(); _ = conn2.Close() })

	ms1 := &mediaSession{svc: svc, callID: "live-1", upper: u1, hub: hub1, subID: "sub-live-1", conn: conn1}
	ms2 := &mediaSession{svc: svc, callID: "live-2", upper: u2, hub: hub2, subID: "sub-live-2", conn: conn2}
	require.NoError(t, hub1.Subscribe(ms1.subID, func(int64, [][]byte, bool) {}))
	require.NoError(t, hub2.Subscribe(ms2.subID, func(int64, [][]byte, bool) {}))
	ps1 := &playbackSession{svc: svc, callID: "play-1", upper: u1, conn: conn1, done: make(chan struct{})}
	ps2 := &playbackSession{svc: svc, callID: "play-2", upper: u2, conn: conn2, done: make(chan struct{})}

	svc.mu.Lock()
	u1.online, u2.online = true, true
	u1.regTS, u2.regTS = time.Now(), time.Now()
	svc.sessions[ms1.callID], svc.sessions[ms2.callID] = ms1, ms2
	svc.playbacks[ps1.callID], svc.playbacks[ps2.callID] = ps1, ps2
	svc.subs["catalog-1"] = &catalogSub{upper: u1, callID: "catalog-1"}
	svc.subs["catalog-2"] = &catalogSub{upper: u2, callID: "catalog-2"}
	svc.mu.Unlock()

	svc.setOnline(u1, false)

	require.Eventually(t, func() bool {
		svc.mu.Lock()
		defer svc.mu.Unlock()
		return !u1.online && u1.regTS.IsZero() && u2.online &&
			len(svc.sessions) == 1 && svc.sessions["live-2"] == ms2 &&
			len(svc.playbacks) == 1 && svc.playbacks["play-2"] == ps2 &&
			len(svc.subs) == 1 && svc.subs["catalog-2"] != nil
	}, time.Second, time.Millisecond)
	require.Equal(t, 0, hub1.ConsumerCount())
	require.Equal(t, 1, hub2.ConsumerCount())
}

func TestNetworkChangeConvergesAllOldDialogsAndWakesRegistration(t *testing.T) {
	svc := New(testCfg(), fakeSource{}, nil)
	u := svc.uppers[0]
	hub := platform.NewFrameHub()
	conn1, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4zero, Port: 0})
	require.NoError(t, err)
	conn2, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4zero, Port: 0})
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn1.Close(); _ = conn2.Close() })
	ms := &mediaSession{svc: svc, callID: "network-live", upper: u, hub: hub, subID: "network-sub", conn: conn1}
	require.NoError(t, hub.Subscribe(ms.subID, func(int64, [][]byte, bool) {}))
	ps := &playbackSession{svc: svc, callID: "network-playback", upper: u, conn: conn2, done: make(chan struct{})}
	svc.mu.Lock()
	u.online = true
	svc.sessions[ms.callID] = ms
	svc.playbacks[ps.callID] = ps
	svc.subs["network-catalog"] = &catalogSub{upper: u, callID: "network-catalog"}
	svc.mu.Unlock()

	svc.NotifyNetworkChange()

	require.Eventually(t, func() bool {
		return !upperOnline(svc, u) && svc.ForwardCount() == 0 && subCount(svc) == 0 && hub.ConsumerCount() == 0
	}, time.Second, time.Millisecond)
	select {
	case <-u.wake:
	default:
		t.Fatal("network change must wake the registration loop")
	}
}

func upperOnline(svc *Service, u *upper) bool {
	svc.mu.Lock()
	defer svc.mu.Unlock()
	return u.online
}

func TestNetworkChangeForcesFreshRegistration(t *testing.T) {
	cfg := testCfg()
	cfg.HeartbeatInterval = "10s"
	cfg.RegisterRetryBase = "20ms"
	cfg.RegisterRetryMax = "100ms"
	cfg.SIPListen = net.JoinHostPort(lbLocalHost, strconv.Itoa(freeUDPPort(t)))

	up := newUpperSocket(t, cfg.SIPListen)
	cfg.ServerAddr = up.conn.LocalAddr().String()
	svc := New(cfg, fakeSource{}, nil)
	require.NoError(t, svc.Start(context.Background()))
	t.Cleanup(func() { _ = svc.Stop() })
	reg := startRejectingRegistrar(t, up, 0)

	require.Eventually(t, func() bool { return svc.Online() }, 5*time.Second, 10*time.Millisecond)
	reg.mu.Lock()
	before := reg.authorized
	reg.mu.Unlock()

	svc.NotifyNetworkChange()
	require.Eventually(t, func() bool {
		reg.mu.Lock()
		count := reg.authorized
		reg.mu.Unlock()
		return count > before && svc.Online()
	}, time.Second, 10*time.Millisecond, "network change must trigger a fresh REGISTER")
}

func TestStartUsesCallerContextCancellation(t *testing.T) {
	cfg := testCfg()
	cfg.SIPListen = net.JoinHostPort(lbLocalHost, "0")
	up := newUpperSocket(t, cfg.SIPListen)
	cfg.ServerAddr = up.conn.LocalAddr().String()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	svc := New(cfg, fakeSource{}, nil)
	require.NoError(t, svc.Start(ctx))
	done := make(chan struct{})
	go func() {
		svc.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("cancelled caller context must stop registration and notify loops")
	}
	require.False(t, svc.Online())
	require.NoError(t, svc.Stop())
}

func TestInviteRejectsWhenStopHasStarted(t *testing.T) {
	svc, up := startLoopbackService(t, fakeSource{}, nil)
	svc.stopping.Store(true)

	res := up.roundTrip(up.request(sip.INVITE, lbChannelOne, playSDP(t, "Play", false), "application/sdp"))
	require.Equal(t, 503, int(res.StatusCode()))
}

func TestSubscribeRejectsWhenStopHasStarted(t *testing.T) {
	svc, up := startLoopbackService(t, fakeSource{}, nil)
	svc.stopping.Store(true)

	sub := up.request(sip.SUBSCRIBE, testCfg().LocalDeviceID, "", "")
	sub.AppendHeader(&sip.GenericHeader{HeaderName: "Event", Contents: "Catalog"})
	res := up.roundTrip(sub)
	require.Equal(t, 503, int(res.StatusCode()))
	require.Empty(t, subCount(svc))
}

func TestInviteRejectsUntilNetworkRegistrationConverges(t *testing.T) {
	svc, up := startLoopbackService(t, fakeSource{}, nil)
	svc.NotifyNetworkChange()

	res := up.roundTrip(up.request(sip.INVITE, lbChannelOne, playSDP(t, "Play", false), "application/sdp"))
	require.Equal(t, 503, int(res.StatusCode()))
}

func TestDialogOwnershipIsolatedAcrossUppers(t *testing.T) {
	cfg := testCfg()
	cfg.ServerDomain = lbUpperDevice
	cfg.Upstreams = []Upstream{{
		ServerDomain: "34020000002000000003",
		ServerAddr:   "127.0.0.2:5060",
	}}
	hub := platform.NewFrameHub()
	svc, up := startLoopbackServiceWithConfig(t, cfg, hubSource{fakeSource{cams: []CameraInfo{{ID: "cam-1", Name: "Front"}}}, hub}, nil)
	_, err := svc.catalogItems()
	require.NoError(t, err)

	invite := up.request(sip.INVITE, lbChannelOne, playSDP(t, "Play", false), "application/sdp")
	res := up.roundTrip(invite)
	require.Equal(t, 200, int(res.StatusCode()))
	id, ok := invite.CallID()
	require.True(t, ok)

	foreign := up.requestDialog(sip.INVITE, lbChannelOne, playSDP(t, "Play", false), "application/sdp", id)
	from, ok := foreign.From()
	require.True(t, ok)
	uri, ok := from.Address.(*sip.SipUri)
	require.True(t, ok)
	uri.SetUser(sip.String{Str: "34020000002000000003"})
	res = up.roundTrip(foreign)
	require.Equal(t, 403, int(res.StatusCode()))
	require.Len(t, sessionIDs(svc), 1)

	bye := up.requestDialog(sip.BYE, lbChannelOne, "", "", id)
	from, ok = bye.From()
	require.True(t, ok)
	uri, ok = from.Address.(*sip.SipUri)
	require.True(t, ok)
	uri.SetUser(sip.String{Str: "34020000002000000003"})
	res = up.roundTrip(bye)
	require.Equal(t, 403, int(res.StatusCode()))
	require.Len(t, sessionIDs(svc), 1)
}

func TestInvalidatedSubscriptionSuppressesNotify(t *testing.T) {
	cfg := testCfg()
	cfg.SIPListen = net.JoinHostPort(lbLocalHost, "0")
	up := newUpperSocket(t, cfg.SIPListen)
	cfg.ServerAddr = up.conn.LocalAddr().String()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	svc := New(cfg, fakeSource{}, nil)
	require.NoError(t, svc.Start(ctx))
	t.Cleanup(func() { _ = svc.Stop() })
	sub := &catalogSub{upper: svc.uppers[0], callID: "stale-catalog"}
	svc.mu.Lock()
	svc.subs[sub.callID] = sub
	delete(svc.subs, sub.callID)
	svc.mu.Unlock()

	svc.sendCatalogNotify(sub)
	require.NoError(t, up.conn.SetReadDeadline(time.Now().Add(100*time.Millisecond)))
	buf := make([]byte, 65535)
	_, _, err := up.conn.ReadFromUDP(buf)
	require.Error(t, err, "an invalidated subscription must not receive a NOTIFY")
}

func TestSubscriptionInvalidationSerializesWithNotify(t *testing.T) {
	cfg := testCfg()
	cfg.SIPListen = net.JoinHostPort(lbLocalHost, "0")
	up := newUpperSocket(t, cfg.SIPListen)
	cfg.ServerAddr = up.conn.LocalAddr().String()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	svc := New(cfg, fakeSource{}, nil)
	require.NoError(t, svc.Start(ctx))
	t.Cleanup(func() { _ = svc.Stop() })
	sub := &catalogSub{upper: svc.uppers[0], callID: "serialized-catalog"}
	svc.mu.Lock()
	svc.subs[sub.callID] = sub
	svc.mu.Unlock()
	sub.sendMu.Lock()
	done := make(chan struct{})
	go func() {
		svc.sendCatalogNotify(sub)
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("NOTIFY must serialize with subscription invalidation")
	case <-time.After(20 * time.Millisecond):
	}
	svc.mu.Lock()
	delete(svc.subs, sub.callID)
	svc.mu.Unlock()
	sub.sendMu.Unlock()
	<-done
	require.NoError(t, up.conn.SetReadDeadline(time.Now().Add(100*time.Millisecond)))
	buf := make([]byte, 65535)
	_, _, err := up.conn.ReadFromUDP(buf)
	require.Error(t, err, "an invalidated subscription must not receive a NOTIFY")
}

func TestStopIsSafeWhenCalledConcurrently(t *testing.T) {
	svc := New(testCfg(), fakeSource{}, nil)
	svc.ctx, svc.cancel = context.WithCancel(context.Background())
	svc.mu.Lock()
	svc.uppers[0].online = true
	svc.uppers[0].regTS = time.Unix(100, 0)
	svc.subs["catalog"] = &catalogSub{upper: svc.uppers[0], callID: "catalog"}
	svc.mu.Unlock()
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- svc.Stop()
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.False(t, upperOnline(svc, svc.uppers[0]))
	require.True(t, svc.uppers[0].regTS.IsZero())
	require.Empty(t, subCount(svc))
}

func TestRegisterLoopCancellationConvergesUpperDialogs(t *testing.T) {
	svc := New(testCfg(), fakeSource{}, nil)
	u := svc.uppers[0]
	ctx, cancel := context.WithCancel(context.Background())
	svc.ctx, svc.cancel = context.WithCancel(ctx)
	u.online = true
	ms := &mediaSession{svc: svc, callID: "cancelled-live", upper: u}
	svc.mu.Lock()
	svc.sessions[ms.callID] = ms
	svc.mu.Unlock()

	svc.wg.Add(1)
	go svc.registerLoop(u)
	cancel()
	svc.wg.Wait()

	require.False(t, u.online)
	require.Empty(t, sessionIDs(svc))
}

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

func TestRegistrationGenerationBarrierPreservesStaleState(t *testing.T) {
	svc := New(testCfg(), fakeSource{}, nil)
	u := svc.uppers[0]
	u.stale.Store(true)
	generation := u.generation.Load()
	svc.mu.Lock()
	done := make(chan bool)
	go func() { done <- svc.setOnlineForGeneration(u, true, generation) }()
	u.generation.Add(1)
	svc.mu.Unlock()

	require.False(t, <-done)
	require.True(t, u.stale.Load(), "an invalidated registration must remain stale")
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
	cfg.SIPListen = net.JoinHostPort(lbLocalHost, strconv.Itoa(freeUDPPort(t)))
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
	select {
	case <-svc.stopDone:
	case <-time.After(time.Second):
		t.Fatal("cancelled caller context must finish service shutdown")
	}
	request := up.request(sip.OPTIONS, testCfg().LocalDeviceID, "", "")
	up.send(request)
	require.NoError(t, up.conn.SetReadDeadline(time.Now().Add(100*time.Millisecond)))
	buf := make([]byte, 65535)
	_, _, err := up.conn.ReadFromUDP(buf)
	require.Error(t, err, "caller cancellation must close the SIP listener")
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
	secondUp := newUpperSocketOn(t, up.sip.String(), "127.0.0.2")
	_, err := svc.catalogItems()
	require.NoError(t, err)

	invite := up.request(sip.INVITE, lbChannelOne, playSDP(t, "Play", false), "application/sdp")
	res := up.roundTrip(invite)
	require.Equal(t, 200, int(res.StatusCode()))
	id, ok := invite.CallID()
	require.True(t, ok)

	foreign := secondUp.requestDialog(sip.INVITE, lbChannelOne, playSDP(t, "Play", false), "application/sdp", id)
	from, ok := foreign.From()
	require.True(t, ok)
	uri, ok := from.Address.(*sip.SipUri)
	require.True(t, ok)
	uri.SetUser(sip.String{Str: "34020000002000000003"})
	res = secondUp.roundTrip(foreign)
	require.Equal(t, 403, int(res.StatusCode()))
	require.Len(t, sessionIDs(svc), 1)

	info := secondUp.requestDialog(sip.INFO, lbChannelOne, "", "", id)
	from, ok = info.From()
	require.True(t, ok)
	uri, ok = from.Address.(*sip.SipUri)
	require.True(t, ok)
	uri.SetUser(sip.String{Str: "34020000002000000003"})
	res = secondUp.roundTrip(info)
	require.Equal(t, 403, int(res.StatusCode()))

	bye := secondUp.requestDialog(sip.BYE, lbChannelOne, "", "", id)
	from, ok = bye.From()
	require.True(t, ok)
	uri, ok = from.Address.(*sip.SipUri)
	require.True(t, ok)
	uri.SetUser(sip.String{Str: "34020000002000000003"})
	res = secondUp.roundTrip(bye)
	require.Equal(t, 403, int(res.StatusCode()))
	require.Len(t, sessionIDs(svc), 1)
}

func TestSameChannelInvitesFromDifferentUppersCoexist(t *testing.T) {
	cfg := testCfg()
	cfg.ServerDomain = lbUpperDevice
	cfg.Upstreams = []Upstream{{
		ServerDomain: "34020000002000000003",
		ServerAddr:   "127.0.0.2:5060",
	}}
	hub := platform.NewFrameHub()
	svc, up := startLoopbackServiceWithConfig(t, cfg, hubSource{fakeSource{cams: []CameraInfo{{ID: "cam-1", Name: "Front"}}}, hub}, nil)
	secondUp := newUpperSocketOn(t, up.sip.String(), "127.0.0.2")
	_, err := svc.catalogItems()
	require.NoError(t, err)
	first := up.request(sip.INVITE, lbChannelOne, playSDP(t, "Play", false), "application/sdp")
	res := up.roundTrip(first)
	require.Equal(t, 200, int(res.StatusCode()))

	second := secondUp.request(sip.INVITE, lbChannelOne, playSDP(t, "Play", false), "application/sdp")
	from, ok := second.From()
	require.True(t, ok)
	uri, ok := from.Address.(*sip.SipUri)
	require.True(t, ok)
	uri.SetUser(sip.String{Str: "34020000002000000003"})
	res = secondUp.roundTrip(second)
	require.Equal(t, 200, int(res.StatusCode()))
	require.Eventually(t, func() bool { return len(sessionIDs(svc)) == 2 }, time.Second, time.Millisecond)
	require.Equal(t, 2, hub.ConsumerCount(), "different uppers must retain both same-channel live leases")
}

func TestPlaybackInviteCannotTakeLiveDialogFromAnotherUpper(t *testing.T) {
	cfg := testCfg()
	cfg.ServerDomain = lbUpperDevice
	cfg.Upstreams = []Upstream{{
		ServerDomain: "34020000002000000003",
		ServerAddr:   "127.0.0.2:5060",
	}}
	hub := platform.NewFrameHub()
	svc, up := startLoopbackServiceWithConfig(t, cfg, hubSource{fakeSource{cams: []CameraInfo{{ID: "cam-1", Name: "Front"}}}, hub}, newCascadeTestDB(t))
	secondUp := newUpperSocketOn(t, up.sip.String(), "127.0.0.2")
	_, err := svc.catalogItems()
	require.NoError(t, err)

	live := up.request(sip.INVITE, lbChannelOne, playSDP(t, "Play", false), "application/sdp")
	require.Equal(t, 200, int(up.roundTrip(live).StatusCode()))
	callID, ok := live.CallID()
	require.True(t, ok)

	playback := secondUp.requestDialog(sip.INVITE, lbChannelOne, playSDP(t, "Playback", true), "application/sdp", callID)
	from, ok := playback.From()
	require.True(t, ok)
	uri, ok := from.Address.(*sip.SipUri)
	require.True(t, ok)
	uri.SetUser(sip.String{Str: "34020000002000000003"})
	res := secondUp.roundTrip(playback)
	require.Equal(t, 403, int(res.StatusCode()))
	require.Len(t, sessionIDs(svc), 1, "cross-upper playback must not replace the live dialog")
	require.Empty(t, playbackIDs(svc))
}

func TestPlaybackDialogOwnershipAcrossUppers(t *testing.T) {
	cfg := testCfg()
	cfg.ServerDomain = lbUpperDevice
	cfg.Upstreams = []Upstream{{
		ServerDomain: "34020000002000000003",
		ServerAddr:   "127.0.0.2:5060",
	}}
	hub := platform.NewFrameHub()
	db := newCascadeTestDB(t)
	svc, up := startLoopbackServiceWithConfig(t, cfg, hubSource{fakeSource{cams: []CameraInfo{{ID: "cam-1", Name: "Front"}}}, hub}, db)
	secondUp := newUpperSocketOn(t, up.sip.String(), "127.0.0.2")
	_, err := svc.catalogItems()
	require.NoError(t, err)
	createPacedPlaybackSegment(t, db, "cam-1", time.Now().UTC().Add(-10*time.Minute))

	first := up.request(sip.INVITE, lbChannelOne, playSDP(t, "Playback", true), "application/sdp")
	require.Equal(t, 200, int(up.roundTrip(first).StatusCode()))
	require.Eventually(t, func() bool { return len(playbackIDs(svc)) == 1 }, time.Second, time.Millisecond)
	callID, ok := first.CallID()
	require.True(t, ok)
	callKey := callID.String()
	svc.mu.Lock()
	original := svc.playbacks[callKey]
	svc.mu.Unlock()
	require.NotNil(t, original)

	foreignPlayback := secondUp.requestDialog(sip.INVITE, lbChannelOne, playSDP(t, "Playback", true), "application/sdp", callID)
	from, ok := foreignPlayback.From()
	require.True(t, ok)
	uri, ok := from.Address.(*sip.SipUri)
	require.True(t, ok)
	uri.SetUser(sip.String{Str: "34020000002000000003"})
	res := secondUp.roundTrip(foreignPlayback)
	require.Equal(t, 403, int(res.StatusCode()))
	svc.mu.Lock()
	got := svc.playbacks[callKey]
	svc.mu.Unlock()
	require.Same(t, original, got)

	foreignInfo := secondUp.requestDialog(sip.INFO, lbChannelOne, "PAUSE\r\n", "application/MANSRTSP+rtsp", callID)
	from, ok = foreignInfo.From()
	require.True(t, ok)
	uri, ok = from.Address.(*sip.SipUri)
	require.True(t, ok)
	uri.SetUser(sip.String{Str: "34020000002000000003"})
	res = secondUp.roundTrip(foreignInfo)
	require.Equal(t, 403, int(res.StatusCode()))
	foreignBye := secondUp.requestDialog(sip.BYE, lbChannelOne, "", "", callID)
	from, ok = foreignBye.From()
	require.True(t, ok)
	uri, ok = from.Address.(*sip.SipUri)
	require.True(t, ok)
	uri.SetUser(sip.String{Str: "34020000002000000003"})
	res = secondUp.roundTrip(foreignBye)
	require.Equal(t, 403, int(res.StatusCode()))
	svc.mu.Lock()
	got = svc.playbacks[callKey]
	svc.mu.Unlock()
	require.Same(t, original, got, "foreign BYE/INFO must not operate the playback")
}

type blockingPlaybackStore struct {
	*fakeCascadeStore
	entered chan struct{}
	done    chan struct{}
	once    sync.Once
}

type releasePlaybackStore struct {
	*fakeCascadeStore
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (s *releasePlaybackStore) ListRecordings(context.Context, RecordingFilter) ([]Recording, error) {
	s.once.Do(func() { close(s.entered) })
	<-s.release
	start := time.Now().Add(-10 * time.Minute)
	return []Recording{{
		CameraID:  "cam-1",
		FilePath:  "blocked-segment",
		Format:    FormatH264,
		StartedAt: start,
		EndedAt:   start.Add(5 * time.Minute),
	}}, nil
}

func (s *blockingPlaybackStore) ListRecordings(ctx context.Context, _ RecordingFilter) ([]Recording, error) {
	s.once.Do(func() { close(s.entered) })
	<-ctx.Done()
	close(s.done)
	return nil, ctx.Err()
}

func TestPlaybackStoreCancellationLetsStopReleaseAdmission(t *testing.T) {
	store := &blockingPlaybackStore{
		fakeCascadeStore: newFakeCascadeStore(),
		entered:          make(chan struct{}),
		done:             make(chan struct{}),
	}
	hub := platform.NewFrameHub()
	svc, up := startLoopbackService(t, hubSource{fakeSource{cams: []CameraInfo{{ID: "cam-1", Name: "Front"}}}, hub}, store)
	_, err := svc.catalogItems()
	require.NoError(t, err)

	req := up.request(sip.INVITE, lbChannelOne, playSDP(t, "Playback", true), "application/sdp")
	up.send(req)
	select {
	case <-store.entered:
	case <-time.After(time.Second):
		t.Fatal("playback handler did not reach the blocking Store")
	}

	stopped := make(chan error, 1)
	go func() { stopped <- svc.Stop() }()
	select {
	case err := <-stopped:
		require.NoError(t, err, "Stop must cancel a blocked playback query")
	case <-time.After(time.Second):
		t.Fatal("Stop remained blocked behind playback admission")
	}
	select {
	case <-store.done:
	case <-time.After(time.Second):
		t.Fatal("cancelled Store query did not return")
	}
	require.Empty(t, playbackIDs(svc), "cancelled playback must not be admitted")
}

func TestByeAdmissionPreventsBlockedPlaybackReplacement(t *testing.T) {
	store := &releasePlaybackStore{
		fakeCascadeStore: newFakeCascadeStore(),
		entered:          make(chan struct{}),
		release:          make(chan struct{}),
	}
	hub := platform.NewFrameHub()
	svc, up := startLoopbackService(t, hubSource{fakeSource{cams: []CameraInfo{{ID: "cam-1", Name: "Front"}}}, hub}, store)
	_, err := svc.catalogItems()
	require.NoError(t, err)

	call := up.request(sip.INVITE, lbChannelOne, playSDP(t, "Playback", true), "application/sdp")
	callID, ok := call.CallID()
	require.True(t, ok)
	oldConn, oldPeer := net.Pipe()
	liveConn, livePeer := net.Pipe()
	t.Cleanup(func() {
		_ = oldPeer.Close()
		_ = livePeer.Close()
	})
	old := &playbackSession{
		svc: svc, callID: callID.String(), upper: svc.uppers[0], conn: oldConn,
		start: time.Unix(100, 0), end: time.Unix(200, 0), done: make(chan struct{}),
	}
	live := &mediaSession{svc: svc, callID: callID.String(), upper: svc.uppers[0], conn: liveConn}
	svc.mu.Lock()
	svc.playbacks[old.callID] = old
	svc.sessions[live.callID] = live
	svc.mu.Unlock()
	parserRelease := make(chan struct{})
	t.Cleanup(func() { close(parserRelease) })
	svc.SetSegmentParser(func(string) (*SegmentInfo, error) {
		<-parserRelease
		return &SegmentInfo{Codec: "h264", Timescale: 1000, Samples: []SegmentSample{{Size: 1, Duration: 1, IsKeyFrame: true}}}, nil
	})

	reinvite := up.requestDialog(sip.INVITE, lbChannelOne, playSDP(t, "Playback", true), "application/sdp", callID)
	up.send(reinvite)
	select {
	case <-store.entered:
	case <-time.After(time.Second):
		t.Fatal("replacement INVITE did not reach the blocking Store")
	}

	bye := up.requestDialog(sip.BYE, lbChannelOne, "", "", callID)
	cseq, ok := bye.CSeq()
	require.True(t, ok)
	cseq.SeqNo = 3
	up.send(bye)
	close(store.release)
	responses := make(map[uint]sip.Response)
	for len(responses) < 2 {
		msg := up.readMessage()
		res, ok := msg.(sip.Response)
		if !ok {
			continue
		}
		gotID, idOK := res.CallID()
		gotSeq, seqOK := res.CSeq()
		if idOK && seqOK && gotID.String() == callID.String() && res.StatusCode() >= 200 {
			responses[uint(gotSeq.SeqNo)] = res
		}
	}
	require.Equal(t, 200, int(responses[2].StatusCode()))
	require.Equal(t, 200, int(responses[3].StatusCode()))
	require.Eventually(t, func() bool { return len(playbackIDs(svc)) == 0 }, time.Second, time.Millisecond,
		"BYE must remove the replacement before returning")
	require.Empty(t, sessionIDs(svc), "BYE must also remove the same Call-ID live dialog")
}

func TestStartWithoutUpperReleasesListener(t *testing.T) {
	port := freeUDPPort(t)
	cfg := testCfg()
	cfg.ServerAddr = ""
	cfg.Upstreams = nil
	cfg.SIPListen = net.JoinHostPort(lbLocalHost, strconv.Itoa(port))
	svc := New(cfg, fakeSource{}, newCascadeTestDB(t))
	require.Error(t, svc.Start(context.Background()))

	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP(lbLocalHost), Port: port})
	require.NoError(t, err, "failed Start must shut down its listener")
	_ = conn.Close()
}

func TestUnknownMultiUpperSenderIsRejectedByEveryHandler(t *testing.T) {
	cfg := testCfg()
	cfg.ServerDomain = lbUpperDevice
	cfg.Upstreams = []Upstream{{
		ServerDomain: "34020000002000000003",
		ServerAddr:   "127.0.0.2:5060",
	}}
	hub := platform.NewFrameHub()
	svc, up := startLoopbackServiceWithConfig(t, cfg, hubSource{fakeSource{cams: []CameraInfo{{ID: "cam-1", Name: "Front"}}}, hub}, nil)
	unknown := func(req sip.Request) sip.Request {
		from, ok := req.From()
		require.True(t, ok)
		uri, ok := from.Address.(*sip.SipUri)
		require.True(t, ok)
		uri.SetUser(sip.String{Str: "unknown-upper"})
		return req
	}

	res := up.roundTrip(unknown(up.request(sip.INVITE, lbChannelOne, playSDP(t, "Play", false), "application/sdp")))
	require.Equal(t, 403, int(res.StatusCode()))
	require.Empty(t, sessionIDs(svc))
	sub := up.request(sip.SUBSCRIBE, testCfg().LocalDeviceID, "", "", &sip.GenericHeader{HeaderName: "Event", Contents: "Catalog"})
	res = up.roundTrip(unknown(sub))
	require.Equal(t, 403, int(res.StatusCode()))
	require.Empty(t, subCount(svc))
	res = up.roundTrip(unknown(up.request(sip.INFO, lbChannelOne, "", "")))
	require.Equal(t, 403, int(res.StatusCode()))
	res = up.roundTrip(unknown(up.request(sip.BYE, lbChannelOne, "", "")))
	require.Equal(t, 403, int(res.StatusCode()))
	res = up.roundTrip(unknown(up.request(sip.MESSAGE, testCfg().LocalDeviceID, "not-manscdp", "Application/MANSCDP+xml")))
	require.Equal(t, 403, int(res.StatusCode()))
	res = up.roundTrip(unknown(up.request(sip.OPTIONS, testCfg().LocalDeviceID, "", "")))
	require.Equal(t, 403, int(res.StatusCode()))
	nonCatalog := up.request(sip.SUBSCRIBE, testCfg().LocalDeviceID, "", "")
	nonCatalog.AppendHeader(&sip.GenericHeader{HeaderName: "Event", Contents: "Alarm"})
	res = up.roundTrip(unknown(nonCatalog))
	require.Equal(t, 403, int(res.StatusCode()))
}

func TestSingleUpperEveryEntryPointRequiresIDAndSource(t *testing.T) {
	svc, up := startLoopbackService(t, fakeSource{}, newCascadeTestDB(t))
	badFrom := func(req sip.Request) sip.Request {
		from, ok := req.From()
		require.True(t, ok)
		uri, ok := from.Address.(*sip.SipUri)
		require.True(t, ok)
		uri.SetUser(sip.String{Str: "wrong-upper"})
		return req
	}
	requests := []sip.Request{
		up.request(sip.INVITE, lbChannelOne, playSDP(t, "Play", false), "application/sdp"),
		up.request(sip.BYE, lbChannelOne, "", ""),
		up.request(sip.INFO, lbChannelOne, "", ""),
		up.request(sip.OPTIONS, testCfg().LocalDeviceID, "", ""),
		up.request(sip.MESSAGE, testCfg().LocalDeviceID, "not-manscdp", "Application/MANSCDP+xml"),
	}
	nonCatalog := up.request(sip.SUBSCRIBE, testCfg().LocalDeviceID, "", "")
	nonCatalog.AppendHeader(&sip.GenericHeader{HeaderName: "Event", Contents: "Alarm"})
	requests = append(requests, nonCatalog)
	for _, req := range requests {
		res := up.roundTrip(badFrom(req))
		require.Equal(t, 403, int(res.StatusCode()), req.Method())
	}

	wrongSource := newUpperSocketOn(t, up.sip.String(), "127.0.0.2")
	res := wrongSource.roundTrip(wrongSource.request(sip.OPTIONS, testCfg().LocalDeviceID, "", ""))
	require.Equal(t, 403, int(res.StatusCode()))
	nonCatalog = wrongSource.request(sip.SUBSCRIBE, testCfg().LocalDeviceID, "", "")
	nonCatalog.AppendHeader(&sip.GenericHeader{HeaderName: "Event", Contents: "Alarm"})
	res = wrongSource.roundTrip(nonCatalog)
	require.Equal(t, 403, int(res.StatusCode()))
	require.Empty(t, sessionIDs(svc))
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

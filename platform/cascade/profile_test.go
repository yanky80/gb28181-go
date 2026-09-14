package cascade

import (
	"bytes"
	"context"
	"errors"
	"net"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ghettovoice/gosip/sip"
	"github.com/mickeyzzc/gb28181-go/platform"
	"github.com/stretchr/testify/require"
)

func TestProtocolProfileDefaultsAndMatrix(t *testing.T) {
	tests := []struct {
		name, version, codec string
		wantVersion          string
		wantCodec            string
		wantErr              bool
	}{
		{name: "default h265", wantVersion: "2022", wantCodec: "h265"},
		{name: "2022 h265", version: "2022", codec: "h265", wantVersion: "2022", wantCodec: "h265"},
		{name: "2022 h264", version: "2022", codec: "h264", wantVersion: "2022", wantCodec: "h264"},
		{name: "2016 h264", version: "2016", codec: "h264", wantVersion: "2016", wantCodec: "h264"},
		{name: "2011", version: "2011", codec: "h264", wantErr: true},
		{name: "2016 h265", version: "2016", codec: "h265", wantErr: true},
		{name: "unknown version", version: "2020", codec: "h264", wantErr: true},
		{name: "unknown codec", version: "2022", codec: "vp9", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			version, codec, err := resolveProtocolProfile(tt.version, tt.codec)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tt.wantVersion, version)
			require.Equal(t, tt.wantCodec, codec)
		})
	}

	require.Equal(t, "2022", (Config{}).EffectiveProtocolVersion())
}

func TestStartRejectsUnsupportedProtocolProfiles(t *testing.T) {
	for _, tt := range []struct {
		name, version, codec string
	}{
		{name: "2011", version: "2011", codec: "h264"},
		{name: "2011 without cameras", version: "2011"},
		{name: "2016 h265", version: "2016", codec: "h265"},
		{name: "unknown codec", version: "2022", codec: "vp9"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testCfg()
			cfg.ServerDomain = lbUpperDevice
			cfg.ProtocolVersion = tt.version
			cfg.SIPListen = freeSIPListenAddress(t)
			cams := []CameraInfo{{ID: "cam-1", Encoding: tt.codec}}
			if tt.codec == "" {
				cams = nil
			}
			svc := New(cfg, fakeSource{cams: cams}, nil)
			require.Error(t, svc.Start(context.Background()))
		})
	}
}

func TestRegisterProtocolVersionHeaderAndResponse(t *testing.T) {
	for _, tt := range []struct{ name, configVersion, marker string }{
		{name: "2022 default", marker: "3.0"},
		{name: "2016", configVersion: "2016", marker: "2.0"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testCfg()
			cfg.ProtocolVersion = tt.configVersion
			cfg.SIPListen = freeSIPListenAddress(t)
			up := newUpperSocket(t, cfg.SIPListen)
			cfg.ServerAddr = up.conn.LocalAddr().String()

			svc := New(cfg, fakeSource{}, nil)
			received := make(chan string, 2)
			stop := make(chan struct{})
			go serveProfileRegistration(t, up, tt.marker, received, stop)
			t.Cleanup(func() {
				_ = svc.Stop()
				close(stop)
			})
			require.NoError(t, svc.Start(context.Background()))

			require.Eventually(t, func() bool { return svc.Online() }, 5*time.Second, 20*time.Millisecond)
			require.Equal(t, tt.marker, <-received)
			require.Equal(t, tt.marker, <-received)
			require.Equal(t, tt.marker, svc.UpperProtocolVersion())
		})
	}
}

func TestRegisterStatusDistinguishesMissingPeerVersion(t *testing.T) {
	cfg := testCfg()
	cfg.ServerDomain = lbUpperDevice
	cfg.SIPListen = freeSIPListenAddress(t)
	up := newUpperSocket(t, cfg.SIPListen)
	cfg.ServerAddr = up.conn.LocalAddr().String()

	svc := New(cfg, fakeSource{cams: []CameraInfo{{ID: "cam-1", Encoding: "h265"}}}, nil)
	stop := make(chan struct{})
	go serveProfileRegistration(t, up, "", nil, stop)
	t.Cleanup(func() {
		_ = svc.Stop()
		close(stop)
	})
	require.NoError(t, svc.Start(context.Background()))
	require.Eventually(t, func() bool { return svc.Online() }, 5*time.Second, 20*time.Millisecond)

	require.Equal(t, StatusVersionMissing, svc.Status())
	require.Empty(t, svc.UpperProtocolVersion())
}

func TestRegisterWireCarriesProfileOnInitialAndDigestRetry(t *testing.T) {
	for _, tt := range []struct{ name, configVersion, marker string }{
		{name: "2022", marker: "3.0"},
		{name: "2016", configVersion: "2016", marker: "2.0"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testCfg()
			cfg.ProtocolVersion = tt.configVersion
			cfg.SIPListen = freeSIPListenAddress(t)
			up := newUpperSocket(t, cfg.SIPListen)
			cfg.ServerAddr = up.conn.LocalAddr().String()

			wire := make(chan string, 2)
			stop := make(chan struct{})
			go serveProfileRegistration(t, up, tt.marker, nil, stop, wire)
			svc := New(cfg, fakeSource{}, nil)
			t.Cleanup(func() {
				_ = svc.Stop()
				close(stop)
			})
			require.NoError(t, svc.Start(context.Background()))
			require.Eventually(t, func() bool { return svc.Online() }, 5*time.Second, 20*time.Millisecond)

			first, second := <-wire, <-wire
			requestLine := "REGISTER sip:" + cfg.ServerDomain + "@" + cfg.ServerAddr + " SIP/2.0"
			require.Equal(t, registerWireGoldenExpected(requestLine, tt.marker, false), registerWireGolden(first, requestLine, tt.marker, false))
			require.Equal(t, registerWireGoldenExpected(requestLine, tt.marker, true), registerWireGolden(second, requestLine, tt.marker, true))
		})
	}
}

func TestDynamicH265CameraUsesSavedPerUpperVersion(t *testing.T) {
	for _, responseVersion := range []string{"", "2.0"} {
		t.Run("response-"+responseVersion, func(t *testing.T) {
			cfg := testCfg()
			cfg.ServerDomain = lbUpperDevice
			cfg.SIPListen = freeSIPListenAddress(t)
			up := newUpperSocket(t, cfg.SIPListen)
			cfg.ServerAddr = up.conn.LocalAddr().String()
			src := &mutableStatusSource{fakeSource: fakeSource{cams: []CameraInfo{{ID: "cam-1", Encoding: "h264"}}}}
			stop := make(chan struct{})
			go serveProfileRegistration(t, up, responseVersion, nil, stop)
			svc := New(cfg, src, nil)
			t.Cleanup(func() {
				_ = svc.Stop()
				close(stop)
			})
			require.NoError(t, svc.Start(context.Background()))
			require.Eventually(t, func() bool { return svc.Online() }, 5*time.Second, 20*time.Millisecond)

			src.SetCamera(CameraInfo{ID: "cam-1", Encoding: "h265"})
			wantStatus := StatusVersionMismatch
			if responseVersion == "" {
				wantStatus = StatusVersionMissing
			}
			require.Equal(t, wantStatus, svc.Status())
			_, err := svc.catalogItems()
			require.NoError(t, err)
			res := up.roundTrip(up.request(sip.INVITE, lbChannelOne, playSDP(t, "Play", false), "application/sdp"))
			require.Equal(t, 488, int(res.StatusCode()))
			require.Contains(t, res.Reason(), StatusVersionMismatch)
		})
	}
}

func TestLiveReinviteRechecksCurrentVersionGate(t *testing.T) {
	hub := platform.NewFrameHub()
	svc, up := startLoopbackService(t, hubSource{fakeSource{cams: []CameraInfo{{ID: "cam-1", Encoding: "h265"}}}, hub}, nil)
	_, err := svc.catalogItems()
	require.NoError(t, err)

	invite := up.request(sip.INVITE, lbChannelOne, playSDP(t, "Play", false), "application/sdp")
	res := up.roundTrip(invite)
	require.Equal(t, 200, int(res.StatusCode()))
	callID, ok := invite.CallID()
	require.True(t, ok)
	svc.saveUpperProtocolVersion(svc.uppers[0], "2.0")

	res = up.roundTrip(up.requestDialog(sip.INVITE, lbChannelOne, playSDP(t, "Play", false), "application/sdp", callID))
	require.Equal(t, 488, int(res.StatusCode()))
	require.Contains(t, res.Reason(), StatusVersionMismatch)
}

func TestVersionAdmissionIsIsolatedPerUpper(t *testing.T) {
	cfg := testCfg()
	cfg.Upstreams = []Upstream{{ServerDomain: "34020000002000000003", ServerAddr: "127.0.0.1:5061"}}
	svc := New(cfg, fakeSource{cams: []CameraInfo{{ID: "cam-1", Encoding: "h265"}}}, nil)
	require.Len(t, svc.uppers, 2)
	svc.saveUpperProtocolVersion(svc.uppers[0], "2.0")
	svc.saveUpperProtocolVersion(svc.uppers[1], "3.0")

	cam := CameraInfo{ID: "cam-1", Encoding: "h265"}
	require.False(t, svc.mediaVersionAllowed(svc.uppers[0], cam))
	require.True(t, svc.mediaVersionAllowed(svc.uppers[1], cam))
}

func TestServiceStartsForEverySupportedProtocolProfile(t *testing.T) {
	for _, tt := range []struct {
		name, version, encoding string
	}{
		{name: "2022 h265", encoding: "h265"},
		{name: "2022 h264", encoding: "h264"},
		{name: "2016 h264", version: "2016", encoding: "h264"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testCfg()
			cfg.ServerDomain = lbUpperDevice
			cfg.ProtocolVersion = tt.version
			hub := platform.NewFrameHub()
			svc, up := startLoopbackServiceWithConfig(t, cfg,
				hubSource{fakeSource{cams: []CameraInfo{{ID: "cam-1", Encoding: tt.encoding}}}, hub}, nil)
			_, err := svc.catalogItems()
			require.NoError(t, err)
			res := up.roundTrip(up.request(sip.INVITE, lbChannelOne, playSDP(t, "Play", false), "application/sdp"))
			require.Equal(t, 200, int(res.StatusCode()))
		})
	}
}

func TestLiveAndSubstreamCodecMismatchTearsDown(t *testing.T) {
	for _, wantSub := range []bool{false, true} {
		t.Run(map[bool]string{false: "live", true: "substream"}[wantSub], func(t *testing.T) {
			mainHub, subHub := platform.NewFrameHub(), platform.NewFrameHub()
			svc, up := startLoopbackService(t, hubSource{fakeSource{cams: []CameraInfo{{ID: "cam-1", Encoding: "h265", SubStream: wantSub}}}, mainHub}, nil)
			acq := &fakeSubAcquirer{hub: subHub, release: make(chan struct{}, 1)}
			svc.SetSubStreamAcquirer(acq)
			_, err := svc.catalogItems()
			require.NoError(t, err)
			res := up.roundTrip(up.request(sip.INVITE, lbChannelOne, playSDP(t, "Play", false), "application/sdp"))
			require.Equal(t, 200, int(res.StatusCode()))
			target := mainHub
			if wantSub {
				target = subHub
			}
			require.Eventually(t, func() bool { return target.ConsumerCount() == 1 }, time.Second, 10*time.Millisecond)

			target.Broadcast(90000, [][]byte{{0x67, 0x42}, {0x68, 0xce}, {0x65, 0x88}}, true)
			require.Eventually(t, func() bool {
				return target.ConsumerCount() == 0 && svc.ForwardCount() == 0
			}, time.Second, 10*time.Millisecond)
		})
	}
}

func TestCodecAdmissionFailsClosedForUnknownOrMalformedAU(t *testing.T) {
	for _, au := range []struct {
		name string
		au   [][]byte
	}{
		{name: "missing AU"},
		{name: "empty NAL", au: [][]byte{{}}},
		{name: "truncated H265 NAL", au: [][]byte{{0x40}}},
		{name: "H264 non-IDR slice", au: [][]byte{{0x41, 0x01, 0x02}}},
		{name: "parameter sets without keyframe", au: [][]byte{{0x40, 0x01, 0x0c}, {0x42, 0x01, 0x01}, {0x44, 0x01, 0xc0}}},
		{name: "unknown first VCL", au: [][]byte{{0x02, 0x01, 0x02}}},
	} {
		t.Run(au.name, func(t *testing.T) {
			hub := platform.NewFrameHub()
			svc, up := startLoopbackService(t, hubSource{fakeSource{cams: []CameraInfo{{ID: "cam-1", Encoding: "h265"}}}, hub}, nil)
			_, err := svc.catalogItems()
			require.NoError(t, err)
			res := up.roundTrip(up.request(sip.INVITE, lbChannelOne, playSDP(t, "Play", false), "application/sdp"))
			require.Equal(t, 200, int(res.StatusCode()))
			require.Eventually(t, func() bool { return hub.ConsumerCount() == 1 }, time.Second, 10*time.Millisecond)

			hub.Broadcast(90000, au.au, false)
			require.Eventually(t, func() bool {
				return hub.ConsumerCount() == 0 && svc.ForwardCount() == 0
			}, time.Second, 10*time.Millisecond)
		})
	}
}

func TestH264ProfileRejectsH265ParameterSets(t *testing.T) {
	hub := platform.NewFrameHub()
	svc, up := startLoopbackService(t, hubSource{fakeSource{cams: []CameraInfo{{ID: "cam-1", Encoding: "h264"}}}, hub}, nil)
	_, err := svc.catalogItems()
	require.NoError(t, err)
	res := up.roundTrip(up.request(sip.INVITE, lbChannelOne, playSDP(t, "Play", false), "application/sdp"))
	require.Equal(t, 200, int(res.StatusCode()))
	require.Eventually(t, func() bool { return hub.ConsumerCount() == 1 }, time.Second, 10*time.Millisecond)

	hub.Broadcast(90000, [][]byte{{0x40, 0x01, 0x0c}, {0x42, 0x01, 0x01}, {0x44, 0x01, 0xc0}, {0x26, 0x01, 0x02}}, true)
	require.Eventually(t, func() bool {
		return hub.ConsumerCount() == 0 && svc.ForwardCount() == 0
	}, time.Second, 10*time.Millisecond)
}

func TestVerifiedH265AllowsSubsequentNonIDR(t *testing.T) {
	hub := platform.NewFrameHub()
	svc, up := startLoopbackService(t, hubSource{fakeSource{cams: []CameraInfo{{ID: "cam-1", Encoding: "h265"}}}, hub}, nil)
	_, err := svc.catalogItems()
	require.NoError(t, err)
	media, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)
	t.Cleanup(func() { _ = media.Close() })
	res := up.roundTrip(up.request(sip.INVITE, lbChannelOne, playSDPAtPort(t, "Play", false, media.LocalAddr().(*net.UDPAddr).Port), "application/sdp"))
	require.Equal(t, 200, int(res.StatusCode()))
	require.Eventually(t, func() bool { return hub.ConsumerCount() == 1 }, time.Second, 10*time.Millisecond)

	key := [][]byte{{0x40, 0x01, 0x0c}, {0x42, 0x01, 0x01}, {0x44, 0x01, 0xc0}, {0x26, 0x01, 0x02}}
	hub.Broadcast(90000, key, true)
	buf := make([]byte, 2048)
	require.Eventually(t, func() bool {
		_ = media.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
		n, _, readErr := media.ReadFromUDP(buf)
		return readErr == nil && n > 12
	}, time.Second, 10*time.Millisecond)

	hub.Broadcast(93600, [][]byte{{0x02, 0x01, 0x02}}, false)
	require.Eventually(t, func() bool {
		_ = media.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
		n, _, readErr := media.ReadFromUDP(buf)
		return readErr == nil && n > 12
	}, time.Second, 10*time.Millisecond)
	require.Equal(t, 1, svc.ForwardCount())
}

func TestPlaybackAdmissionFailsClosedOnRecordingQueryError(t *testing.T) {
	hub := platform.NewFrameHub()
	svc, up := startLoopbackService(t, hubSource{fakeSource{cams: []CameraInfo{{ID: "cam-1", Encoding: "h265"}}}, hub}, recordingQueryErrorStore{err: errors.New("recording index unavailable")})
	_, err := svc.catalogItems()
	require.NoError(t, err)

	res := up.roundTrip(up.request(sip.INVITE, lbChannelOne, playSDP(t, "Playback", true), "application/sdp"))
	require.Equal(t, 500, int(res.StatusCode()))
	require.Zero(t, svc.ForwardCount())
}

func TestPlaybackAdmissionFailsClosedOnSegmentParseError(t *testing.T) {
	hub := platform.NewFrameHub()
	db := newCascadeTestDB(t)
	svc, up := startLoopbackService(t, hubSource{fakeSource{cams: []CameraInfo{{ID: "cam-1", Encoding: "h265"}}}, hub}, db)
	_, err := svc.catalogItems()
	require.NoError(t, err)
	path := "parse-error"
	delete(segByPath, path)
	now := time.Now().UTC()
	require.NoError(t, db.InsertRecording(context.Background(), &Recording{
		ID: path, CameraID: "cam-1", FilePath: path, Format: FormatH265,
		StartedAt: now.Add(-10 * time.Minute), EndedAt: now.Add(-9 * time.Minute),
	}))

	res := up.roundTrip(up.request(sip.INVITE, lbChannelOne, playSDP(t, "Playback", true), "application/sdp"))
	require.Equal(t, 500, int(res.StatusCode()))
	require.Zero(t, svc.ForwardCount())
}

func TestH265PlaybackWithMatchingParsedCodecIsAccepted(t *testing.T) {
	hub := platform.NewFrameHub()
	db := newCascadeTestDB(t)
	svc, up := startLoopbackService(t, hubSource{fakeSource{cams: []CameraInfo{{ID: "cam-1", Encoding: "h265"}}}, hub}, db)
	_, err := svc.catalogItems()
	require.NoError(t, err)
	path := "matching-h265"
	segByPath[path] = &SegmentInfo{Codec: "h265"}
	t.Cleanup(func() { delete(segByPath, path) })
	now := time.Now().UTC()
	require.NoError(t, db.InsertRecording(context.Background(), &Recording{
		ID: path, CameraID: "cam-1", FilePath: path, Format: FormatH265,
		StartedAt: now.Add(-10 * time.Minute), EndedAt: now.Add(-9 * time.Minute),
	}))

	res := up.roundTrip(up.request(sip.INVITE, lbChannelOne, playSDP(t, "Playback", true), "application/sdp"))
	require.Equal(t, 200, int(res.StatusCode()))
	require.Eventually(t, func() bool { return svc.ForwardCount() == 0 }, time.Second, 10*time.Millisecond)
}

func TestPlaybackPumpSkipsParseError(t *testing.T) {
	hub := platform.NewFrameHub()
	db := newCascadeTestDB(t)
	svc, up := startLoopbackService(t, hubSource{fakeSource{cams: []CameraInfo{{ID: "cam-1", Encoding: "h265"}}}, hub}, db)
	_, err := svc.catalogItems()
	require.NoError(t, err)
	parseCalls := 0
	svc.SetSegmentParser(func(string) (*SegmentInfo, error) {
		parseCalls++
		if parseCalls == 1 {
			return &SegmentInfo{Codec: "h265"}, nil
		}
		return nil, errors.New("segment became unreadable")
	})
	now := time.Now().UTC()
	require.NoError(t, db.InsertRecording(context.Background(), &Recording{
		ID: "flaky", CameraID: "cam-1", FilePath: "flaky", Format: FormatH265,
		StartedAt: now.Add(-10 * time.Minute), EndedAt: now.Add(-9 * time.Minute),
	}))

	res := up.roundTrip(up.request(sip.INVITE, lbChannelOne, playSDP(t, "Playback", true), "application/sdp"))
	require.Equal(t, 200, int(res.StatusCode()))
	up.awaitServerRequest(sip.BYE, "")
}

type recordingQueryErrorStore struct{ err error }

func (recordingQueryErrorStore) UpsertCascadeChannel(context.Context, CascadeChannel) error {
	return nil
}

func (recordingQueryErrorStore) ListCascadeChannels(context.Context) ([]CascadeChannel, error) {
	return []CascadeChannel{{CameraID: "cam-1", GBChannelID: lbChannelOne}}, nil
}

func (s recordingQueryErrorStore) ListRecordings(context.Context, RecordingFilter) ([]Recording, error) {
	return nil, s.err
}

func registerWireGolden(raw, requestLine, marker string, auth bool) string {
	lines := strings.Split(strings.TrimRight(raw, "\r\n"), "\r\n")
	for i, line := range lines {
		switch {
		case strings.HasPrefix(line, "REGISTER "):
			lines[i] = requestLine
		case strings.HasPrefix(line, "Via: "):
			lines[i] = "Via: <volatile>"
		case strings.HasPrefix(line, "From: "):
			lines[i] = "From: <volatile>"
		case strings.HasPrefix(line, "To: "):
			lines[i] = "To: <volatile>"
		case strings.HasPrefix(line, "Call-ID: "):
			lines[i] = "Call-ID: <volatile>"
		case strings.HasPrefix(line, "Contact: "):
			lines[i] = "Contact: <volatile>"
		case strings.HasPrefix(line, "Authorization: "):
			if marker := strings.Index(line, `response="`); marker >= 0 {
				valueStart := marker + len(`response="`)
				if valueEnd := strings.Index(line[valueStart:], `"`); valueEnd >= 0 {
					lines[i] = line[:valueStart] + "<volatile>" + line[valueStart+valueEnd:]
				}
			}
		case strings.HasPrefix(line, "Allow: "):
			methods := strings.Split(strings.TrimPrefix(line, "Allow: "), ", ")
			sort.Strings(methods)
			lines[i] = "Allow: " + strings.Join(methods, ", ")
		}
	}
	return strings.Join(lines, "\r\n") + "\r\n\r\n"
}

func registerWireGoldenExpected(requestLine, marker string, auth bool) string {
	lines := []string{
		requestLine,
		"Via: <volatile>",
		"CSeq: 1 REGISTER",
		"From: <volatile>",
		"To: <volatile>",
		"Call-ID: <volatile>",
		"Contact: <volatile>",
		"Max-Forwards: 70",
		"User-Agent: GoSIP",
		"Content-Length: 0",
		"Expires: 3600",
		"X-GB-Ver: " + marker,
	}
	if auth {
		lines = append(lines, "Authorization: Digest realm=\"34020000002000000001\",algorithm=MD5,nonce=\"profile-nonce\",username=\"34020000001320000099\",uri=\"sip:34020000002000000001@127.0.0.1\",response=\"<volatile>\"")
	}
	lines = append(lines, "Allow: ACK, BYE, CANCEL, INFO, INVITE, MESSAGE, OPTIONS, SUBSCRIBE")
	return strings.Join(lines, "\r\n") + "\r\n\r\n"
}

func TestH265VersionMismatchBlocksInvite(t *testing.T) {
	for _, responseVersion := range []string{"", "2.0"} {
		t.Run("response-"+responseVersion, func(t *testing.T) {
			wantStatus := StatusVersionMismatch
			if responseVersion == "" {
				wantStatus = StatusVersionMissing
			}
			cfg := testCfg()
			cfg.ServerDomain = lbUpperDevice
			cfg.SIPListen = freeSIPListenAddress(t)
			up := newUpperSocket(t, cfg.SIPListen)
			cfg.ServerAddr = up.conn.LocalAddr().String()
			hub := platform.NewFrameHub()
			svc := New(cfg, hubSource{fakeSource{cams: []CameraInfo{{ID: "cam-1", Name: "Front", Encoding: "h265"}}}, hub}, nil)
			stop := make(chan struct{})
			go serveProfileRegistration(t, up, responseVersion, nil, stop)
			t.Cleanup(func() {
				_ = svc.Stop()
				close(stop)
			})
			require.NoError(t, svc.Start(context.Background()))
			require.Eventually(t, func() bool { return svc.Status() == wantStatus }, 5*time.Second, 20*time.Millisecond)
			svc.setOnline(svc.uppers[0], false)
			require.Equal(t, wantStatus, svc.Status(), "version status remains latched while the upper is offline")

			_, err := svc.catalogItems()
			require.NoError(t, err)
			res := up.roundTrip(up.request(sip.INVITE, lbChannelOne, playSDP(t, "Play", false), "application/sdp"))
			require.Equal(t, 488, int(res.StatusCode()))
			require.Contains(t, res.Reason(), StatusVersionMismatch)
			require.Zero(t, svc.ForwardCount())
			res = up.roundTrip(up.request(sip.INVITE, lbChannelOne, playSDP(t, "Playback", true), "application/sdp"))
			require.Equal(t, 488, int(res.StatusCode()))
		})
	}
}

func TestH265InviteSDPPSMAndParameterSets(t *testing.T) {
	cfg := testCfg()
	cfg.ServerDomain = lbUpperDevice
	cfg.SIPListen = freeSIPListenAddress(t)
	up := newUpperSocket(t, cfg.SIPListen)
	cfg.ServerAddr = up.conn.LocalAddr().String()

	hub := platform.NewFrameHub()
	svc := New(cfg, hubSource{fakeSource{cams: []CameraInfo{{ID: "cam-1", Name: "Front", Encoding: "h265"}}}, hub}, nil)
	stop := make(chan struct{})
	go serveProfileRegistration(t, up, "3.0", nil, stop)
	t.Cleanup(func() {
		_ = svc.Stop()
		close(stop)
	})
	require.NoError(t, svc.Start(context.Background()))
	require.Eventually(t, func() bool { return svc.Online() }, 5*time.Second, 20*time.Millisecond)
	_, err := svc.catalogItems()
	require.NoError(t, err)

	media, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)
	t.Cleanup(func() { _ = media.Close() })
	port := media.LocalAddr().(*net.UDPAddr).Port
	res := up.roundTrip(up.request(sip.INVITE, lbChannelOne, playSDPAtPort(t, "Play", false, port), "application/sdp"))
	require.Equal(t, 200, int(res.StatusCode()))
	require.Contains(t, string(res.Body()), "a=rtpmap:96 PS/90000")

	vps := []byte{0x40, 0x01, 0x0c}
	sps := []byte{0x42, 0x01, 0x01}
	pps := []byte{0x44, 0x01, 0xc0}
	idr := []byte{0x26, 0x01, 0x02}
	require.Eventually(t, func() bool { return hub.ConsumerCount() == 1 }, time.Second, 10*time.Millisecond)
	var ps []byte
	buf := make([]byte, 2048)
	require.Eventually(t, func() bool {
		hub.Broadcast(90000, [][]byte{vps, sps, pps, idr}, true)
		_ = media.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
		n, _, readErr := media.ReadFromUDP(buf)
		if readErr != nil || n < 12 {
			return false
		}
		ps = append(ps, buf[12:n]...)
		return buf[1]&0x80 != 0
	}, 5*time.Second, 20*time.Millisecond)
	require.True(t, bytes.Contains(ps, []byte{0, 0, 1, 0xbc}))
	require.True(t, bytes.Contains(ps, []byte{0x24, 0xe0}))
	for _, nalu := range [][]byte{vps, sps, pps, idr} {
		require.True(t, bytes.Contains(ps, nalu), "PS must carry H.265 parameter set/NALU %x", nalu)
	}
}

func TestH265PlaybackRejectsMismatchedParsedCodec(t *testing.T) {
	cfg := testCfg()
	cfg.ServerDomain = lbUpperDevice
	cfg.SIPListen = freeSIPListenAddress(t)
	up := newUpperSocket(t, cfg.SIPListen)
	cfg.ServerAddr = up.conn.LocalAddr().String()
	db := newCascadeTestDB(t)
	svc := New(cfg, fakeSource{cams: []CameraInfo{{ID: "cam-1", Name: "Front", Encoding: "h265"}}}, db)
	svc.SetSegmentParser(fakeSegmentParser)
	stop := make(chan struct{})
	t.Cleanup(func() {
		_ = svc.Stop()
		close(stop)
	})
	require.NoError(t, svc.Start(context.Background()))
	svc.setOnline(svc.uppers[0], true)
	_, err := svc.catalogItems()
	require.NoError(t, err)
	now := time.Now().UTC()
	segByPath["mismatched"] = &SegmentInfo{Codec: "h264"}
	require.NoError(t, db.InsertRecording(context.Background(), &Recording{
		ID: "mismatched", CameraID: "cam-1", FilePath: "mismatched", Format: FormatH265,
		StartedAt: now.Add(-10 * time.Minute), EndedAt: now.Add(-9 * time.Minute),
	}))

	res := up.roundTrip(up.request(sip.INVITE, lbChannelOne, playSDP(t, "Playback", true), "application/sdp"))
	require.Equal(t, 488, int(res.StatusCode()))
	require.Contains(t, res.Reason(), StatusVersionMismatch)
}

func serveProfileRegistration(t *testing.T, up *upperSocket, version string, received chan<- string, stop <-chan struct{}, wire ...chan<- string) {
	t.Helper()
	go func() {
		buf := make([]byte, 65535)
		for {
			select {
			case <-stop:
				return
			default:
			}
			_ = up.conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
			n, src, err := up.conn.ReadFromUDP(buf)
			if err != nil {
				continue
			}
			if len(wire) > 0 {
				select {
				case wire[0] <- string(buf[:n]):
				default:
				}
			}
			msg, err := parseSIPBytes(buf[:n])
			if err != nil {
				continue
			}
			req, ok := msg.(sip.Request)
			if !ok || req.Method() != sip.REGISTER {
				continue
			}
			if received != nil {
				values := req.GetHeaders("X-GB-Ver")
				if len(values) > 0 {
					select {
					case received <- values[0].Value():
					default:
					}
				}
			}
			if len(req.GetHeaders("Authorization")) == 0 {
				_, _ = up.conn.WriteToUDP(challengeResponse(t, req, 401, "Unauthorized",
					`WWW-Authenticate: Digest realm="34020000002000000001", nonce="profile-nonce", algorithm=MD5`), src)
				continue
			}
			extra := ""
			if version != "" {
				extra = "X-GB-Ver: " + version
			}
			_, _ = up.conn.WriteToUDP(challengeResponse(t, req, 200, "OK", extra), src)
			return
		}
	}()
}

func playSDPAtPort(t *testing.T, name string, playback bool, port int) string {
	t.Helper()
	sdp := playSDP(t, name, playback)
	for _, line := range strings.Split(sdp, "\r\n") {
		if strings.HasPrefix(line, "m=video ") {
			return strings.Replace(sdp, line, "m=video "+strconv.Itoa(port)+" RTP/AVP 96", 1)
		}
	}
	return sdp
}

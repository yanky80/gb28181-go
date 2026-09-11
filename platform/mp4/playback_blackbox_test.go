package mp4

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/ghettovoice/gosip/log"
	"github.com/ghettovoice/gosip/sip"
	sipparser "github.com/ghettovoice/gosip/sip/parser"
	"github.com/mickeyzzc/gb28181-go/platform"
	"github.com/mickeyzzc/gb28181-go/platform/cascade"
	"github.com/stretchr/testify/require"
)

// TestH265PlaybackPumpUsesPublicSeams sends a real playback INVITE through a
// cascade.Service and checks the RTP/PS output. The parser is injected through
// SetSegmentParser; conversion and parameter-set injection remain the pump's
// production path rather than test helpers.
func TestH265PlaybackPumpUsesPublicSeams(t *testing.T) {
	start := time.Now().Add(-time.Minute).Truncate(time.Second)
	end := start.Add(time.Second)
	sampleData := []byte{0, 0, 0, 2, 0x26, 9}
	path := writeSegment(t, h265Segment([]byte{0x40, 1}, []byte{0x42, 1}, []byte{0x44, 1}, [][]sample{{
		{duration: 40, data: sampleData, key: true},
	}}))

	const channelID = "34020000001320000001"
	store := &blackBoxPlaybackStore{
		channels: []cascade.CascadeChannel{{CameraID: "front", GBChannelID: channelID}},
		recordings: []cascade.Recording{{
			CameraID: "front", FilePath: path, Format: cascade.FormatH265,
			StartedAt: start, EndedAt: end,
		}},
	}
	upper, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	require.NoError(t, err)
	t.Cleanup(func() { _ = upper.Close() })
	rtp, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	require.NoError(t, err)
	t.Cleanup(func() { _ = rtp.Close() })

	cfg := cascade.Config{
		Enabled:       true,
		ServerDomain:  "34020000002000000001",
		ServerAddr:    upper.LocalAddr().String(),
		LocalDeviceID: "34020000001320000099",
		SIPListen:     net.JoinHostPort("127.0.0.1", strconv.Itoa(freeBlackBoxUDPPort(t))),
	}
	svc := cascade.New(cfg, blackBoxCameraSource{}, store)
	svc.SetSegmentParser(ParseSegment)
	require.NoError(t, svc.Start(context.Background()))
	t.Cleanup(func() { _ = svc.Stop() })

	invite, callID := blackBoxPlaybackInvite(t, channelID, upper.LocalAddr().(*net.UDPAddr).Port, rtp.LocalAddr().(*net.UDPAddr).Port, start, end)
	serviceAddr, err := net.ResolveUDPAddr("udp", cfg.SIPListen)
	require.NoError(t, err)
	_, err = upper.WriteToUDP([]byte(invite.String()), serviceAddr)
	require.NoError(t, err)
	response := blackBoxFinalResponse(t, upper, callID)
	require.Equal(t, 200, int(response.StatusCode()))

	au := blackBoxRTPAccessUnit(t, rtp)
	nalus, err := platform.NewPSDemuxer().FeedAU(au, 9000, true)
	require.NoError(t, err)
	require.Equal(t, [][]byte{{0x40, 1}, {0x42, 1}, {0x44, 1}, {0x26, 9}}, nalus)
}

type blackBoxCameraSource struct{}

func (blackBoxCameraSource) Cameras() []cascade.CameraInfo {
	return []cascade.CameraInfo{{ID: "front", Name: "Front"}}
}

func (blackBoxCameraSource) Hub(string) *platform.FrameHub { return nil }

type blackBoxPlaybackStore struct {
	channels   []cascade.CascadeChannel
	recordings []cascade.Recording
}

func (s *blackBoxPlaybackStore) UpsertCascadeChannel(_ context.Context, ch cascade.CascadeChannel) error {
	s.channels = append(s.channels, ch)
	return nil
}

func (s *blackBoxPlaybackStore) ListCascadeChannels(context.Context) ([]cascade.CascadeChannel, error) {
	return append([]cascade.CascadeChannel(nil), s.channels...), nil
}

func (s *blackBoxPlaybackStore) ListRecordings(context.Context, cascade.RecordingFilter) ([]cascade.Recording, error) {
	return append([]cascade.Recording(nil), s.recordings...), nil
}

func freeBlackBoxUDPPort(t *testing.T) int {
	t.Helper()
	c, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	require.NoError(t, err)
	defer c.Close()
	return c.LocalAddr().(*net.UDPAddr).Port
}

func blackBoxPlaybackInvite(t *testing.T, channelID string, viaPort, rtpPort int, start, end time.Time) (sip.Request, string) {
	t.Helper()
	const host = "127.0.0.1"
	const upperID = "34020000002000000002"
	callID := fmt.Sprintf("mp4-playback-%d", time.Now().UnixNano())
	rb := sip.NewRequestBuilder()
	rb.SetMethod(sip.INVITE)
	rb.SetFrom(&sip.Address{Uri: &sip.SipUri{FUser: sip.String{Str: upperID}, FHost: host}})
	rb.SetTo(&sip.Address{Uri: &sip.SipUri{FUser: sip.String{Str: channelID}, FHost: host}})
	rb.SetRecipient(&sip.SipUri{FUser: sip.String{Str: channelID}, FHost: host})
	rb.SetHost(host)
	port := sip.Port(viaPort)
	rb.AddVia(&sip.ViaHop{Host: host, Port: &port, Params: sip.NewParams().Add("branch", sip.String{Str: sip.GenerateBranch()})})
	cid := sip.CallID(callID)
	rb.SetCallID(&cid)
	rb.SetSeqNo(1)
	mf := sip.MaxForwards(70)
	rb.SetMaxForwards(&mf)
	body := fmt.Sprintf("v=0\r\no=%s 0 0 IN IP4 %s\r\ns=Playback\r\nc=IN IP4 %s\r\nt=%d %d\r\nm=video %d RTP/AVP 96\r\ny=12345678\r\n", upperID, host, host, start.Unix(), end.Unix(), rtpPort)
	rb.SetBody(body)
	contentType := sip.ContentType("application/sdp")
	rb.SetContentType(&contentType)
	invite, err := rb.Build()
	require.NoError(t, err)
	return invite, callID
}

func blackBoxFinalResponse(t *testing.T, conn *net.UDPConn, callID string) sip.Response {
	t.Helper()
	buf := make([]byte, 65535)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		require.NoError(t, conn.SetReadDeadline(time.Now().Add(500*time.Millisecond)))
		n, _, err := conn.ReadFromUDP(buf)
		if err != nil {
			continue
		}
		msg, err := sipparser.ParseMessage(buf[:n], log.NewDefaultLogrusLogger())
		if err != nil {
			continue
		}
		response, ok := msg.(sip.Response)
		if !ok {
			continue
		}
		responseCallID, hasCallID := response.CallID()
		if !hasCallID || responseCallID.Value() != callID {
			continue
		}
		if response.StatusCode() >= 200 {
			return response
		}
	}
	t.Fatal("no final SIP response")
	return nil
}

func blackBoxRTPAccessUnit(t *testing.T, conn *net.UDPConn) []byte {
	t.Helper()
	var au []byte
	buf := make([]byte, 65535)
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(5*time.Second)))
	for {
		n, _, err := conn.ReadFromUDP(buf)
		require.NoError(t, err)
		require.GreaterOrEqual(t, n, 12)
		header := 12 + int(buf[0]&0x0f)*4
		require.LessOrEqual(t, header, n)
		au = append(au, buf[header:n]...)
		if buf[1]&0x80 != 0 {
			return au
		}
	}
}

package cascade

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"

	"github.com/ghettovoice/gosip/sip"
	"github.com/stretchr/testify/require"
)

func TestUpperOfRequiresConfiguredIDAndSource(t *testing.T) {
	cfg := testCfg()
	cfg.ServerAddr = "10.0.0.1:5060"
	svc := New(cfg, fakeSource{}, newCascadeTestDB(t))

	req := newFromRequest(t, cfg.ServerDomain)
	req.SetSource("10.0.0.1:49152") // source ports may change across NAT.
	require.Equal(t, svc.uppers[0], svc.upperOf(req))

	req = newFromRequest(t, "99999999999999999999")
	req.SetSource("10.0.0.1:49152")
	require.Nil(t, svc.upperOf(req), "an unknown From user must not inherit the first upper")

	req = newFromRequest(t, cfg.ServerDomain)
	req.SetSource("10.0.0.2:5060")
	require.Nil(t, svc.upperOf(req), "a configured ID from an unconfigured source must be rejected")
}

func TestRejectUnauthorizedRequestDoesNotLogAuthorization(t *testing.T) {
	var logs bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(old) })

	svc := New(testCfg(), fakeSource{}, newCascadeTestDB(t))
	req := newFromRequest(t, "99999999999999999999")
	req.SetSource("127.0.0.1:5060")
	req.AppendHeader(&sip.GenericHeader{HeaderName: "Authorization", Contents: `Digest username="secret-user", response="top-secret"`})

	require.Nil(t, svc.requireUpper(req))
	require.NotContains(t, logs.String(), "top-secret")
	require.NotContains(t, logs.String(), "Authorization")
}

func TestSDPFromInviteStrictNegotiation(t *testing.T) {
	validUDP := strictSDP("10.0.0.1", "RTP/AVP", "a=recvonly\r\n", "")
	validTCP := strictSDP("10.0.0.1", "TCP/RTP/AVP", "a=recvonly\r\na=setup:passive\r\n", "")

	tests := []struct {
		name string
		sdp  string
		want string
	}{
		{"valid UDP", validUDP, ""},
		{"valid TCP passive", validTCP, ""},
		{"missing c", strings.Replace(validUDP, "c=IN IP4 10.0.0.1\r\n", "", 1), "c="},
		{"invalid c", strings.Replace(validUDP, "c=IN IP4 10.0.0.1", "c=IN IP4 not-an-ip", 1), "c="},
		{"missing video", strings.Replace(validUDP, "m=video 30000 RTP/AVP 96\r\n", "m=audio 30000 RTP/AVP 8\r\n", 1), "m=video"},
		{"invalid port", strings.Replace(validUDP, "30000", "0", 1), "port"},
		{"port overflow", strings.Replace(validUDP, "30000", "65536", 1), "port"},
		{"missing SSRC", strings.Replace(validUDP, "y=12345678\r\n", "", 1), "SSRC"},
		{"invalid SSRC", strings.Replace(validUDP, "y=12345678", "y=not-a-number", 1), "SSRC"},
		{"missing direction", strings.Replace(validUDP, "a=recvonly\r\n", "", 1), "direction"},
		{"wrong direction", strings.Replace(validUDP, "a=recvonly", "a=sendrecv", 1), "direction"},
		{"audio is not video-only", validUDP + "m=audio 30002 RTP/AVP 8\r\n", "audio"},
		{"unsupported transport", strings.Replace(validUDP, "RTP/AVP", "UDP/RTP/AVP", 1), "transport"},
		{"TCP without setup", strings.Replace(validTCP, "a=setup:passive\r\n", "", 1), "setup"},
		{"TCP active", strings.Replace(validTCP, "a=setup:passive", "a=setup:active", 1), "setup"},
		{"UDP setup", validUDP + "a=setup:passive\r\n", "setup"},
		{"wrong clock", strings.Replace(validUDP, "PS/90000", "PS/8000", 1), "PS/90000"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := sdpFromInvite([]byte(tt.sdp))
			if tt.want == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.want)
		})
	}
}

func strictSDP(host, transport, direction, setup string) string {
	return "v=0\r\no=- 0 0 IN IP4 " + host + "\r\n" +
		"s=Play\r\nc=IN IP4 " + host + "\r\nt=0 0\r\n" +
		"m=video 30000 " + transport + " 96\r\n" + direction + setup +
		"a=rtpmap:96 PS/90000\r\ny=12345678\r\n"
}

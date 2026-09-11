package cascade

import (
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/mickeyzzc/gb28181-go/platform"
	"github.com/mickeyzzc/gb28181-go/psmux"
	"github.com/stretchr/testify/require"
)

// The FIRST verified burst of a forwarding session must carry the PSM:
// receivers latch demuxer codec and IDR tracking from the keyframe before
// later VCL-only access units arrive.
func TestMediaSessionFirstBurstCarriesPSM(t *testing.T) {
	mainHub := platform.NewFrameHub()
	svc := New(testCfg(), hubSource{fakeSource{cams: []CameraInfo{{ID: "cam-1", Encoding: "h265"}}}, mainHub}, nil)

	client, srvConn := net.Pipe()
	ms := &mediaSession{
		svc: svc, callID: "c1", channel: "ch", camera: "cam-1",
		mux: psmux.New(), codecHint: "h265",
		rtp: psmux.NewRTPPacketizerTCP(srvConn, 1, 0),
	}
	ms.mux.SetVideoCodec("h265")
	ms.hub = mainHub
	ms.run(mainHub)
	defer ms.stop()

	// Establish codec identity from a keyframe's parameter sets before
	// forwarding any ambiguous VCL-only AU.
	key := [][]byte{{0x40, 0x01, 0x0c}, {0x42, 0x01, 0x01}, {0x44, 0x01, 0xc0}, {0x26, 0x01, 0x02}}
	mainHub.Broadcast(90000, key, true)

	ps := readFirstBurstPS(t, client)
	require.Contains(t, string(ps), "\x00\x00\x01\xbc",
		"first burst must carry the PSM (00 00 01 BC) even without an IDR")
	require.Contains(t, string(ps), "\x00\x00\x01\xbb",
		"first burst must carry the system header (00 00 01 BB)")

	// A verified session accepts a valid H.265 non-IDR AU without re-sending
	// the PSM.
	au := [][]byte{{0x02, 0x01, 0x02, 0x03, 0x04}}
	mainHub.Broadcast(93600, au, false)
	ps2 := readFirstBurstPS(t, client)
	require.NotContains(t, string(ps2), "\x00\x00\x01\xbc",
		"only the first burst is PSM-forced; later bursts follow auIsIDR")
}

// readFirstBurstPS drains RTP packets from the pipe until one carries the
// marker bit (end of burst) and returns the concatenated payloads.
func readFirstBurstPS(t *testing.T, c net.Conn) []byte {
	t.Helper()
	c.SetReadDeadline(time.Now().Add(3 * time.Second))
	var ps []byte
	for range 8 {
		hdr := make([]byte, 2)
		if _, err := io.ReadFull(c, hdr); err != nil {
			t.Fatalf("read framing header: %v", err)
		}
		n := binary.BigEndian.Uint16(hdr)
		pkt := make([]byte, n)
		if _, err := io.ReadFull(c, pkt); err != nil {
			t.Fatalf("read rtp packet: %v", err)
		}
		require.GreaterOrEqual(t, len(pkt), 12)
		ps = append(ps, pkt[12:]...)
		if pkt[1]&0x80 != 0 {
			return ps
		}
	}
	t.Fatal("no marker bit within 8 packets")
	return nil
}

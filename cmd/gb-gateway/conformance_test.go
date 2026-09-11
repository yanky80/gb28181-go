package main

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mickeyzzc/gb28181-go/edgeipc"
	"github.com/mickeyzzc/gb28181-go/manscdp"
	"github.com/mickeyzzc/gb28181-go/platform"
	"github.com/mickeyzzc/gb28181-go/platform/cascade"
	platformsip "github.com/mickeyzzc/gb28181-go/platform/sip"
	"github.com/stretchr/testify/require"
)

const (
	conformanceUpperID   = "34020000002000000001"
	conformanceGatewayID = "34020000001320000001"
)

func TestGatewayConformanceH265UDP(t *testing.T) {
	s := newGatewayScenario(t, "2022", "h265", "udp", "3.0", false)
	front := s.connectPeer("front", 1)
	s.connectPeer("back", 1)

	require.Eventually(t, func() bool {
		if !s.gateway.cascade.Online() || s.gateway.cascade.UpperProtocolVersion() != "3.0" {
			return false
		}
		for _, channel := range s.upperDM.Channels(conformanceGatewayID) {
			if channel.Name == "Front" {
				return true
			}
		}
		return false
	}, 10*time.Second, 20*time.Millisecond, "gateway registration and catalog must converge")

	channels := s.upperDM.Channels(conformanceGatewayID)
	byName := make(map[string]*platform.Channel, len(channels))
	for _, channel := range channels {
		byName[channel.Name] = channel
	}
	frontChannel := byName["Front"]
	require.NotNil(t, frontChannel)
	var mu sync.Mutex
	var got [][][]byte
	require.NoError(t, s.upper.InviteChannel(conformanceGatewayID, frontChannel.ID))
	require.NoError(t, s.upper.SendMessage(conformanceGatewayID, []byte(fmt.Sprintf(
		"<Query><CmdType>DeviceInfo</CmdType><SN>71</SN><DeviceID>%s</DeviceID></Query>", conformanceGatewayID))))
	require.NoError(t, s.upper.SendMessage(conformanceGatewayID, []byte(fmt.Sprintf(
		"<Query><CmdType>DeviceStatus</CmdType><SN>72</SN><DeviceID>%s</DeviceID></Query>", conformanceGatewayID))))
	require.Eventually(t, func() bool {
		device, ok := s.upperDM.Device(conformanceGatewayID)
		if !ok {
			return false
		}
		device.Mu.RLock()
		defer device.Mu.RUnlock()
		return device.Status.Load() == platform.DeviceOnline &&
			device.Name == "GB28181 Platform" && device.Manufacturer == "Unknown" && device.Model == "Unknown"
	}, 5*time.Second, 20*time.Millisecond, "DeviceInfo and DeviceStatus responses must reach the upper")
	hub := awaitUpperHub(t, s.upperSM, frontChannel.ID)
	require.NoError(t, hub.Subscribe("conformance", func(_ int64, au [][]byte, _ bool) {
		mu.Lock()
		got = append(got, au)
		mu.Unlock()
	}))

	commands := readPeerMessages(t, front.control, 2)
	require.Equal(t, edgeipc.MessageStart, commands[0].Type)
	require.Equal(t, edgeipc.MessageRequestIDR, commands[1].Type)

	writeMedia(t, front.media, edgeipc.MediaFrame{
		Codec: edgeipc.CodecH265, Flags: edgeipc.FlagIDR, CameraID: "front",
		PTS90kHz: 9000, Sequence: 1, Payload: h265IDR(),
	})
	require.Equal(t, edgeipc.MessageRequestIDR, readPeerMessages(t, front.control, 1)[0].Type)
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(got) == 1
	}, 5*time.Second, 20*time.Millisecond, "H.265 PS/RTP must reach the upper platform")
	mu.Lock()
	require.Equal(t, h265IDR(), annexBFromNALUs(got[0]))
	mu.Unlock()

	require.NoError(t, s.upper.ByeChannel(conformanceGatewayID, frontChannel.ID))
	stop := readPeerMessages(t, front.control, 1)[0]
	require.Equal(t, edgeipc.MessageStop, stop.Type)
	require.Eventually(t, func() bool {
		return s.upperSM.GetHub(frontChannel.ID) == nil && s.gateway.registry.CameraStatus("front") == "ON"
	}, 5*time.Second, 20*time.Millisecond, "BYE must release the upper session and gateway lease")
}

func TestGatewayConformanceH264Profiles(t *testing.T) {
	for _, tt := range []struct {
		name, version, marker string
	}{
		{name: "2022+h264", version: "2022", marker: "3.0"},
		{name: "2016+h264", version: "2016", marker: "2.0"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := newGatewayScenario(t, tt.version, "h264", "udp", tt.marker, false)
			peer := s.connectPeer("front", 1)
			frontChannel := s.waitForFrontChannel()
			require.NoError(t, s.upper.InviteChannel(conformanceGatewayID, frontChannel.ID))
			hub := awaitUpperHub(t, s.upperSM, frontChannel.ID)
			got := make(chan [][]byte, 1)
			require.NoError(t, hub.Subscribe("conformance-h264", func(_ int64, au [][]byte, _ bool) { got <- au }))
			commands := readPeerMessages(t, peer.control, 2)
			require.Equal(t, edgeipc.MessageStart, commands[0].Type)
			require.Equal(t, edgeipc.MessageRequestIDR, commands[1].Type)
			writeMedia(t, peer.media, edgeipc.MediaFrame{
				Codec: edgeipc.CodecH264, Flags: edgeipc.FlagIDR, CameraID: "front",
				PTS90kHz: 9000, Sequence: 1, Payload: h264IDR(),
			})
			require.Equal(t, edgeipc.MessageRequestIDR, readPeerMessages(t, peer.control, 1)[0].Type)
			select {
			case au := <-got:
				require.Equal(t, h264IDR(), annexBFromNALUs(au))
			case <-time.After(5 * time.Second):
				t.Fatal("H.264 access unit did not reach the upper")
			}
			require.NoError(t, s.upper.ByeChannel(conformanceGatewayID, frontChannel.ID))
			require.Eventually(t, func() bool {
				return s.upperSM.GetHub(frontChannel.ID) == nil && s.gateway.cascade.ForwardCount() == 0
			}, 5*time.Second, 20*time.Millisecond, "H.264 BYE must release the dialog")
		})
	}
}

func TestGatewayConformanceInvalidProfilesRejectStartup(t *testing.T) {
	for _, tt := range []struct {
		name, version, codec string
	}{
		{name: "2011+h264", version: "2011", codec: "h264"},
		{name: "2016+h265", version: "2016", codec: "h265"},
		{name: "unknown+h264", version: "2030", codec: "h264"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			cfg := Config{
				GB: GBConfig{
					ProtocolVersion: tt.version, SIPListen: freeUDPListenAddress(t),
					Heartbeat: time.Second, RegisterExpires: 1, IDRTimeout: time.Second,
				},
				IPC: IPCConfig{
					ControlSocket: filepath.Join(dir, "control.sock"),
					MediaSocket:   filepath.Join(dir, "media.sock"), MaxAUBytes: maxAUBytes,
					StatusDir: filepath.Join(dir, "status"),
				},
				Cameras: []CameraConfig{{Index: 1, LocalCameraID: "front", Expose: true, Name: "Front", Codec: tt.codec}},
			}
			_, err := NewGateway(cfg, Credentials{})
			require.Error(t, err)
		})
	}
}

func TestGatewayConformanceH265TCPActive(t *testing.T) {
	s := newGatewayScenario(t, "2022", "h265", "tcp-passive", "3.0", false)
	peer := s.connectPeer("front", 1)
	s.connectPeer("back", 1)
	frontChannel := s.waitForFrontChannel()
	require.NoError(t, s.upper.InviteChannel(conformanceGatewayID, frontChannel.ID))
	got := make(chan struct{}, 1)
	hub := awaitUpperHub(t, s.upperSM, frontChannel.ID)
	require.NoError(t, hub.Subscribe("conformance-tcp", func(_ int64, _ [][]byte, _ bool) { got <- struct{}{} }))
	commands := readPeerMessages(t, peer.control, 2)
	require.Equal(t, edgeipc.MessageStart, commands[0].Type)
	require.Equal(t, edgeipc.MessageRequestIDR, commands[1].Type)
	writeMedia(t, peer.media, edgeipc.MediaFrame{
		Codec: edgeipc.CodecH265, Flags: edgeipc.FlagIDR, CameraID: "front",
		PTS90kHz: 9000, Sequence: 1, Payload: h265IDR(),
	})
	require.Equal(t, edgeipc.MessageRequestIDR, readPeerMessages(t, peer.control, 1)[0].Type)
	select {
	case <-got:
	case <-time.After(5 * time.Second):
		t.Fatal("TCP-active media did not reach the upper platform")
	}
}

func TestGatewayConformanceDeterministicOnePercentLoss(t *testing.T) {
	s := newGatewayScenario(t, "2022", "h265", "udp", "3.0", false)
	peer := s.connectPeer("front", 1)
	frontChannel := s.waitForFrontChannel()
	require.NoError(t, s.upper.InviteChannel(conformanceGatewayID, frontChannel.ID))
	hub := awaitUpperHub(t, s.upperSM, frontChannel.ID)
	var idrs atomic.Int32
	require.NoError(t, hub.Subscribe("conformance-loss", func(_ int64, au [][]byte, _ bool) {
		if string(annexBFromNALUs(au)) == string(h265IDR()) {
			idrs.Add(1)
		}
	}))
	commands := readPeerMessages(t, peer.control, 2)
	require.Equal(t, edgeipc.MessageStart, commands[0].Type)
	require.Equal(t, edgeipc.MessageRequestIDR, commands[1].Type)

	writeMedia(t, peer.media, edgeipc.MediaFrame{
		Codec: edgeipc.CodecH265, Flags: edgeipc.FlagIDR, CameraID: "front",
		PTS90kHz: 9000, Sequence: 1, Payload: h265IDR(),
	})
	require.Equal(t, edgeipc.MessageRequestIDR, readPeerMessages(t, peer.control, 1)[0].Type)
	for sequence := uint64(2); sequence <= 100; sequence++ {
		if sequence == 50 { // one deterministic loss in a 100-frame window
			continue
		}
		writeMedia(t, peer.media, edgeipc.MediaFrame{
			Codec: edgeipc.CodecH265, CameraID: "front", PTS90kHz: sequence * 9000,
			Sequence: sequence, Payload: h265PFrame(),
		})
	}
	require.Equal(t, edgeipc.MessageRequestIDR, readPeerMessages(t, peer.control, 1)[0].Type)
	writeMedia(t, peer.media, edgeipc.MediaFrame{
		Codec: edgeipc.CodecH265, Flags: edgeipc.FlagIDR, CameraID: "front",
		PTS90kHz: 101 * 9000, Sequence: 101, Payload: h265IDR(),
	})
	require.Eventually(t, func() bool { return idrs.Load() == 2 }, 5*time.Second, 20*time.Millisecond,
		"one-percent deterministic loss must recover on the next IDR")

	require.NoError(t, s.upper.ByeChannel(conformanceGatewayID, frontChannel.ID))
	require.Eventually(t, func() bool {
		return s.upperSM.GetHub(frontChannel.ID) == nil && s.gateway.cascade.ForwardCount() == 0
	}, 5*time.Second, 20*time.Millisecond, "loss recovery must not leak the dialog")
}

func TestGatewayConformanceVersionMismatchRejectsMedia(t *testing.T) {
	s := newGatewayScenario(t, "2022", "h265", "udp", "2.0", false)
	peer := s.connectPeer("front", 1)
	s.connectPeer("back", 1)
	require.Eventually(t, func() bool {
		return s.gateway.cascade.Status() == cascade.StatusVersionMismatch
	}, 5*time.Second, 20*time.Millisecond)

	assertNoPeerMessage(t, peer.control, 50*time.Millisecond)
	require.Zero(t, s.gateway.cascade.ForwardCount())
	require.Equal(t, cascade.StatusVersionMismatch, s.gateway.cascade.Status())
}

func TestGatewayConformanceRecoveryEpochGapAndDuplicateDialog(t *testing.T) {
	s := newGatewayScenario(t, "2022", "h265", "udp", "3.0", false)
	oldPeer := s.connectPeer("front", 1)
	back := s.connectPeer("back", 1)
	frontChannel := s.waitForFrontChannel()
	require.Eventually(t, func() bool { return s.gateway.registry.CameraStatus("front") == "ON" }, 5*time.Second, 20*time.Millisecond)

	_ = back.control.Close()
	_ = back.media.Close()
	require.Eventually(t, func() bool {
		return s.gateway.registry.CameraStatus("back") == "OFF" && s.gateway.registry.CameraStatus("front") == "ON"
	}, 5*time.Second, 20*time.Millisecond, "one camera disconnect must not change the other epoch")

	newPeer := s.connectPeer("front", 2)
	_ = oldPeer.media.Close()
	require.Eventually(t, func() bool {
		view, ok := s.gateway.registry.Camera("front")
		return ok && view.Online && view.StreamEpoch == 2
	}, 5*time.Second, 20*time.Millisecond, "new epoch must replace stale peer")

	require.NoError(t, s.upper.InviteChannel(conformanceGatewayID, frontChannel.ID))
	hub := awaitUpperHub(t, s.upperSM, frontChannel.ID)
	got := make(chan struct{}, 1)
	require.NoError(t, hub.Subscribe("conformance-recovery", func(_ int64, _ [][]byte, _ bool) { got <- struct{}{} }))
	commands := readPeerMessages(t, newPeer.control, 2)
	require.Equal(t, edgeipc.MessageStart, commands[0].Type)
	require.Equal(t, edgeipc.MessageRequestIDR, commands[1].Type)

	writeMediaSlow(t, newPeer.media, edgeipc.MediaFrame{
		Codec: edgeipc.CodecH265, CameraID: "front", Sequence: 1, PTS90kHz: 9000, Payload: h265PFrame(),
	})
	require.Equal(t, edgeipc.MessageRequestIDR, readPeerMessages(t, newPeer.control, 1)[0].Type)
	writeMediaSlow(t, newPeer.media, edgeipc.MediaFrame{
		Codec: edgeipc.CodecH265, Flags: edgeipc.FlagIDR, CameraID: "front",
		Sequence: 3, PTS90kHz: 27000, Payload: h265IDR(),
	})
	select {
	case <-got:
	case <-time.After(5 * time.Second):
		t.Fatal("IDR after a deterministic sequence gap was not recovered")
	}

	require.NoError(t, s.upper.ByeChannel(conformanceGatewayID, frontChannel.ID))
	require.Equal(t, edgeipc.MessageStop, readPeerMessages(t, newPeer.control, 1)[0].Type)
	require.Eventually(t, func() bool {
		return s.upperSM.GetHub(frontChannel.ID) == nil && s.gateway.cascade.ForwardCount() == 0
	}, 5*time.Second, 20*time.Millisecond, "first dialog must release before the replacement dialog")

	require.NoError(t, s.upper.InviteChannel(conformanceGatewayID, frontChannel.ID))
	secondHub := awaitUpperHub(t, s.upperSM, frontChannel.ID)
	secondGot := make(chan struct{}, 1)
	require.NoError(t, secondHub.Subscribe("conformance-recovery-second-dialog", func(_ int64, _ [][]byte, _ bool) { secondGot <- struct{}{} }))
	secondCommands := readPeerMessages(t, newPeer.control, 2)
	require.Equal(t, edgeipc.MessageStart, secondCommands[0].Type)
	require.Equal(t, edgeipc.MessageRequestIDR, secondCommands[1].Type)
	writeMedia(t, newPeer.media, edgeipc.MediaFrame{
		Codec: edgeipc.CodecH265, Flags: edgeipc.FlagIDR, CameraID: "front",
		Sequence: 4, PTS90kHz: 36000, Payload: h265IDR(),
	})
	select {
	case <-secondGot:
	case <-time.After(5 * time.Second):
		t.Fatal("replacement dialog did not publish media")
	}
	// A duplicate INVITE against the live replacement dialog is idempotent:
	// it does not send a second START or replace the owning epoch.
	require.NoError(t, s.upper.InviteChannel(conformanceGatewayID, frontChannel.ID))
	assertNoPeerMessage(t, newPeer.control, 50*time.Millisecond)
	_ = newPeer.control.Close()
	_ = newPeer.media.Close()
	require.Eventually(t, func() bool {
		return s.gateway.registry.CameraStatus("front") == "OFF" &&
			s.gateway.cascade.ForwardCount() == 0
	}, 5*time.Second, 20*time.Millisecond, "disconnect must release dialogs and hubs")
}

func TestGatewayConformancePTZAndRecordInfo(t *testing.T) {
	s := newGatewayScenario(t, "2022", "h265", "udp", "3.0", true)
	ptzDone := make(chan struct{}, 1)
	s.gateway.cascade.SetPTZForwarder(func(cameraID, direction string, speed byte) error {
		if direction == "stop" {
			return nil
		}
		if cameraID != "front" || direction != "up" || speed != 0x20 {
			t.Errorf("PTZ = %q %q %#x", cameraID, direction, speed)
		} else {
			ptzDone <- struct{}{}
		}
		return nil
	})
	s.connectPeer("front", 1)
	s.connectPeer("back", 1)
	frontChannel := s.waitForFrontChannel()

	body, err := manscdp.Encode(manscdp.DeviceControl{
		CmdType: manscdp.CmdDeviceControl, SN: 7, DeviceID: frontChannel.ID,
		PTZCmd: "A50F0108002000DD",
	})
	require.NoError(t, err)
	require.NoError(t, s.upper.SendMessage(conformanceGatewayID, body))
	select {
	case <-ptzDone:
	case <-time.After(2 * time.Second):
		t.Fatal("PTZ command did not reach the camera seam")
	}

	now := time.Now()
	s.gateway.recordings.mu.Lock()
	s.gateway.recordings.entries["front/segment.mp4"] = recordingIndexEntry{
		ID: "front/segment.mp4", CameraID: "front", File: "front/segment.mp4",
		Format: cascade.FormatH265, StartedAt: now.Add(-time.Minute), EndedAt: now,
	}
	s.gateway.recordings.mu.Unlock()
	items, err := s.upper.QueryChannelRecords(conformanceGatewayID, frontChannel.ID, now.Add(-2*time.Minute), now.Add(time.Minute))
	require.NoError(t, err)
	require.Len(t, items, 1)
	require.Equal(t, frontChannel.ID, items[0].DeviceID)
	require.Equal(t, "Front", items[0].Name)
}

func TestGatewayConformanceRestartKeepsChannelEpochIsolation(t *testing.T) {
	s := newGatewayScenario(t, "2022", "h265", "udp", "3.0", false)
	s.connectPeer("front", 1)
	s.connectPeer("back", 1)
	frontChannel := s.waitForFrontChannel()
	oldID := frontChannel.ID
	require.NoError(t, s.gateway.Stop())

	restarted, err := NewGateway(s.cfg, Credentials{values: map[string]string{"sip.password": "conformance-pw"}})
	require.NoError(t, err)
	require.NoError(t, restarted.Start(context.Background()))
	t.Cleanup(func() { require.NoError(t, restarted.Stop()) })
	s.gateway = restarted
	s.connectPeer("front", 2)
	s.connectPeer("back", 2)
	require.Eventually(t, func() bool {
		for _, channel := range s.upperDM.Channels(conformanceGatewayID) {
			if channel.Name == "Front" && channel.ID == oldID {
				return restarted.cascade.Online() && restarted.registry.CameraStatus("front") == "ON"
			}
		}
		return false
	}, 10*time.Second, 20*time.Millisecond, "restart must reuse channel mapping and restore live epochs")
}

func (s *gatewayScenario) waitForFrontChannel() *platform.Channel {
	s.t.Helper()
	var front *platform.Channel
	require.Eventually(s.t, func() bool {
		for _, channel := range s.upperDM.Channels(conformanceGatewayID) {
			if channel.Name == "Front" {
				front = channel
				return true
			}
		}
		return false
	}, 10*time.Second, 20*time.Millisecond, "gateway catalog must contain Front")
	return front
}

func writeMediaSlow(t *testing.T, conn net.Conn, frame edgeipc.MediaFrame) {
	t.Helper()
	wire, err := edgeipc.MarshalMediaFrame(frame)
	require.NoError(t, err)
	for len(wire) > 0 {
		n := 3
		if n > len(wire) {
			n = len(wire)
		}
		_, err = conn.Write(wire[:n])
		require.NoError(t, err)
		wire = wire[n:]
		time.Sleep(time.Millisecond)
	}
}

type gatewayScenario struct {
	t       *testing.T
	cfg     Config
	upper   *platformsip.Server
	upperDM *platform.DeviceManager
	upperSM *platform.SessionManager
	gateway *Gateway
}

type gatewayPeer struct {
	control net.Conn
	media   net.Conn
}

func newGatewayScenario(t *testing.T, version, codec, mediaTransport, upperVersion string, recordPlayback bool) *gatewayScenario {
	t.Helper()
	upperAddr := freeUDPListenAddress(t)
	mediaBase := freeGatewayPort(t, mediaTransport)
	upperDM := platform.NewDeviceManager(2 * time.Second)
	upperSM := platform.NewSessionManager(platform.NewPortManager(uint16(mediaBase), uint16(mediaBase+20)), conformanceUpperID)
	noCatalogSubscription := false
	// Keep signaling UDP-only: a two-camera Catalog exceeds gosip's UDP
	// threshold and auto-promotes to its TCP connection pool, whose upstream
	// shutdown path is not deterministic. The second UDS peer still exercises
	// registry/epoch isolation; cascade loopbacks cover full Catalog/TCP SIP.
	upper := platformsip.NewServer(platformsip.Config{
		Enabled:          true,
		SIPListen:        upperAddr,
		ServerID:         conformanceUpperID,
		Realm:            "conformance",
		Password:         "conformance-pw",
		PortRange:        fmt.Sprintf("%d-%d", mediaBase, mediaBase+20),
		MediaTransport:   mediaTransport,
		SIPTransport:     "udp",
		ProtocolVersion:  upperVersion,
		SubscribeCatalog: &noCatalogSubscription,
	}, upperDM, upperSM, nil)
	require.NoError(t, upper.Start(context.Background()))
	t.Cleanup(func() { require.NoError(t, upper.Stop()) })

	dir := t.TempDir()
	cfg := Config{
		GB: GBConfig{
			Enabled:         true,
			ProtocolVersion: version,
			ServerAddr:      upperAddr,
			ServerDomain:    conformanceUpperID,
			LocalDeviceID:   conformanceGatewayID,
			Realm:           "conformance",
			Heartbeat:       time.Second,
			RegisterExpires: 3600,
			SIPListen:       freeUDPListenAddress(t),
			StopGrace:       0,
			IDRTimeout:      time.Second,
			RecordPlayback:  recordPlayback,
		},
		IPC: IPCConfig{
			ControlSocket: filepath.Join(dir, "control.sock"),
			MediaSocket:   filepath.Join(dir, "media.sock"),
			MaxAUBytes:    maxAUBytes,
			StatusDir:     filepath.Join(dir, "status"),
		},
		Cameras: []CameraConfig{
			{Index: 1, LocalCameraID: "front", Expose: true, Name: "Front", Codec: codec, PTZMode: "vendor"},
			{Index: 2, LocalCameraID: "back", Expose: false, Name: "Back", Codec: codec, PTZMode: "none"},
		},
	}
	gateway, err := NewGateway(cfg, Credentials{values: map[string]string{"sip.password": "conformance-pw"}})
	require.NoError(t, err)
	require.NoError(t, gateway.Start(context.Background()))
	t.Cleanup(func() { require.NoError(t, gateway.Stop()) })
	return &gatewayScenario{t: t, cfg: cfg, upper: upper, upperDM: upperDM, upperSM: upperSM, gateway: gateway}
}

func (s *gatewayScenario) connectPeer(cameraID string, epoch uint64) *gatewayPeer {
	peer := &gatewayPeer{
		control: dialPeer(s.t, s.cfg.IPC.ControlSocket),
		media:   dialPeer(s.t, s.cfg.IPC.MediaSocket),
	}
	writePeerMessage(s.t, peer.control, hello(cameraID, epoch, s.codec(cameraID)))
	writePeerMessage(s.t, peer.control, health())
	return peer
}

func (s *gatewayScenario) codec(cameraID string) edgeipc.Codec {
	for _, camera := range s.cfg.Cameras {
		if camera.LocalCameraID == cameraID {
			return configuredCodec(camera.Codec)
		}
	}
	s.t.Fatalf("unknown camera %q", cameraID)
	return edgeipc.DefaultCodec
}

func freeGatewayPort(t *testing.T, mediaTransport string) int {
	t.Helper()
	if strings.HasPrefix(mediaTransport, "tcp-") {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		defer listener.Close()
		return listener.Addr().(*net.TCPAddr).Port
	}
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)
	defer conn.Close()
	return conn.LocalAddr().(*net.UDPAddr).Port
}

func awaitUpperHub(t *testing.T, sessions *platform.SessionManager, channelID string) *platform.FrameHub {
	t.Helper()
	var hub *platform.FrameHub
	require.Eventually(t, func() bool {
		hub = sessions.GetHub(channelID)
		return hub != nil
	}, 5*time.Second, 20*time.Millisecond, "upper session hub must appear")
	return hub
}

func annexBFromNALUs(au [][]byte) []byte {
	var out []byte
	for _, nalu := range au {
		out = append(out, 0, 0, 0, 1)
		out = append(out, nalu...)
	}
	return out
}

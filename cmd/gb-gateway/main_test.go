package main

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mickeyzzc/gb28181-go/edgeipc"
	"github.com/mickeyzzc/gb28181-go/platform/cascade"
)

func TestNewGatewayRestoresChannelsAndUsesConfiguredCodec(t *testing.T) {
	dir := t.TempDir()
	statusDir := filepath.Join(dir, "status")
	channel := testCascadeChannel()
	cfg := defaultConfig()
	cfg.IPC.StatusDir = statusDir
	cfg.IPC.ControlSocket = filepath.Join(dir, "control.sock")
	cfg.IPC.MediaSocket = filepath.Join(dir, "media.sock")
	cfg.Cameras = []CameraConfig{{Index: 1, LocalCameraID: "front", Expose: true, Name: "Front", Codec: "h264", PTZMode: "none"}}
	first, err := NewGateway(cfg, Credentials{})
	if err != nil {
		t.Fatal(err)
	}
	if err := first.store.UpsertCascadeChannel(context.Background(), channel); err != nil {
		t.Fatal(err)
	}
	if err := first.Stop(); err != nil {
		t.Fatal(err)
	}

	gateway, err := NewGateway(cfg, Credentials{})
	if err != nil {
		t.Fatal(err)
	}
	defer gateway.Stop()

	view, ok := gateway.registry.Camera("front")
	if !ok || view.Codec != edgeipc.CodecH264 || view.Online {
		t.Fatalf("camera view = %+v, %v; want configured H.264 and OFF", view, ok)
	}
	channels, err := gateway.store.ListCascadeChannels(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(channels) != 1 || channels[0] != channel {
		t.Fatalf("restored channels = %#v, want %#v", channels, []cascade.CascadeChannel{channel})
	}
	if _, err := gateway.store.ListRecordings(context.Background(), cascade.RecordingFilter{}); err != nil {
		t.Fatal(err)
	}
	if _, err := gateway.store.ListRecordings(cancelledContext(), cascade.RecordingFilter{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled recording query error = %v, want context canceled", err)
	}
}

func TestDefaultConfigUsesDurableGatewayState(t *testing.T) {
	if got := defaultConfig().IPC.StatusDir; got != "/var/lib/edge-gateway" {
		t.Fatalf("default status directory = %q, want durable gateway state directory", got)
	}
}

func TestGatewayIDRTimeoutDisconnectsTimedOutEpoch(t *testing.T) {
	dir := t.TempDir()
	cfg := defaultConfig()
	cfg.GB.IDRTimeout = 20 * time.Millisecond
	cfg.IPC.StatusDir = filepath.Join(dir, "status")
	cfg.IPC.ControlSocket = filepath.Join(dir, "control.sock")
	cfg.IPC.MediaSocket = filepath.Join(dir, "media.sock")
	cfg.Cameras = []CameraConfig{{Index: 1, LocalCameraID: "front", Expose: true, Name: "Front", Codec: "h265", PTZMode: "none"}}
	gateway, err := NewGateway(cfg, Credentials{})
	if err != nil {
		t.Fatal(err)
	}
	defer gateway.Stop()
	if err := gateway.registry.HandleHello(hello("front", 1, edgeipc.CodecH265)); err != nil {
		t.Fatal(err)
	}
	if err := gateway.registry.HandleHealth("front", 1, health()); err != nil {
		t.Fatal(err)
	}

	serverConn, peer := net.Pipe()
	done := make(chan error, 1)
	go func() { done <- gateway.media.ServeConn(serverConn) }()
	writeMedia(t, peer, edgeipc.MediaFrame{Codec: edgeipc.CodecH265, CameraID: "front", Sequence: 1, PTS90kHz: 9000, Payload: h265PFrame()})
	eventually(t, time.Second, func() bool {
		return gateway.registry.CameraStatus("front") == "OFF" && gateway.registry.Hub("front").ConsumerCount() == 0
	})

	if err := gateway.registry.HandleHello(hello("front", 2, edgeipc.CodecH265)); err != nil {
		t.Fatal(err)
	}
	if err := gateway.registry.HandleHealth("front", 2, health()); err != nil {
		t.Fatal(err)
	}
	if got := gateway.registry.CameraStatus("front"); got != "ON" {
		t.Fatalf("reconnected camera status = %q, want ON", got)
	}
	if err := peer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("timed-out media connection error = %v", err)
	}
}

func TestGatewayStaleIDRTimeoutPreservesReplacementStream(t *testing.T) {
	dir := t.TempDir()
	cfg := defaultConfig()
	cfg.GB.IDRTimeout = time.Minute
	cfg.IPC.StatusDir = filepath.Join(dir, "status")
	cfg.IPC.ControlSocket = filepath.Join(dir, "control.sock")
	cfg.IPC.MediaSocket = filepath.Join(dir, "media.sock")
	cfg.Cameras = []CameraConfig{{Index: 1, LocalCameraID: "front", Expose: true, Name: "Front", Codec: "h265", PTZMode: "none"}}
	gateway, err := NewGateway(cfg, Credentials{})
	if err != nil {
		t.Fatal(err)
	}
	defer gateway.Stop()
	if err := gateway.registry.HandleHello(hello("front", 1, edgeipc.CodecH265)); err != nil {
		t.Fatal(err)
	}
	if err := gateway.registry.HandleHealth("front", 1, health()); err != nil {
		t.Fatal(err)
	}
	oldServer, oldPeer := net.Pipe()
	oldDone := make(chan error, 1)
	go func() { oldDone <- gateway.media.ServeConn(oldServer) }()
	writeMedia(t, oldPeer, edgeipc.MediaFrame{Codec: edgeipc.CodecH265, CameraID: "front", Sequence: 1, PTS90kHz: 9000, Payload: h265PFrame()})
	var old *mediaConnection
	eventually(t, time.Second, func() bool {
		gateway.media.mu.Lock()
		defer gateway.media.mu.Unlock()
		old = gateway.media.connections["front"]
		return old != nil && old.isWaiting()
	})
	old.mu.Lock()
	oldDeadline := old.deadline
	old.mu.Unlock()

	if err := gateway.registry.HandleHello(hello("front", 2, edgeipc.CodecH265)); err != nil {
		t.Fatal(err)
	}
	if err := gateway.registry.HandleHealth("front", 2, health()); err != nil {
		t.Fatal(err)
	}
	frames := make(chan struct{}, 1)
	if err := gateway.registry.Hub("front").Subscribe("test", func(int64, [][]byte, bool) { frames <- struct{}{} }); err != nil {
		t.Fatal(err)
	}
	newServer, newPeer := net.Pipe()
	newDone := make(chan error, 1)
	go func() { newDone <- gateway.media.ServeConn(newServer) }()
	writeMedia(t, newPeer, edgeipc.MediaFrame{Codec: edgeipc.CodecH265, CameraID: "front", Flags: edgeipc.FlagIDR, Sequence: 1, PTS90kHz: 9000, Payload: h265IDR()})
	eventually(t, time.Second, func() bool {
		gateway.media.mu.Lock()
		defer gateway.media.mu.Unlock()
		return gateway.media.connections["front"] != old
	})

	old.idrTimeout(oldDeadline)
	gateway.media.config.OnIDRTimeoutEpoch("front", 1, context.DeadlineExceeded)
	if got := gateway.registry.CameraStatus("front"); got != "ON" {
		t.Fatalf("stale Gateway timeout status = %q, want ON", got)
	}
	writeMedia(t, newPeer, edgeipc.MediaFrame{Codec: edgeipc.CodecH265, CameraID: "front", Sequence: 2, PTS90kHz: 12000, Payload: h265PFrame()})
	select {
	case <-frames:
	case <-time.After(time.Second):
		t.Fatal("replacement stream did not survive stale timeout")
	}
	oldPeer.Close()
	newPeer.Close()
	if err := <-oldDone; err == nil {
		t.Fatal("replaced Gateway connection did not stop")
	}
	if err := <-newDone; err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("replacement Gateway connection error = %v", err)
	}
}

func testCascadeChannel() cascade.CascadeChannel {
	return cascade.CascadeChannel{CameraID: "front", GBChannelID: "34020000001320000001", Name: "Front"}
}

func cancelledContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return ctx
}

func TestGatewayStartsSocketsBeforeCascadeAndStopsCleanly(t *testing.T) {
	dir := t.TempDir()
	sipListen := freeUDPListenAddress(t)
	cfg := defaultConfig()
	cfg.GB = GBConfig{
		Enabled:         true,
		ProtocolVersion: "2022",
		ServerAddr:      "127.0.0.1:9",
		ServerDomain:    "34020000002000000001",
		LocalDeviceID:   "34020000002000000002",
		Realm:           "3402000000",
		SIPListen:       sipListen,
		Heartbeat:       time.Second,
		RegisterExpires: 60,
		Transport:       "udp",
		MediaTransport:  "auto",
		StopGrace:       10 * time.Millisecond,
		IDRTimeout:      time.Second,
		RecordPlayback:  true,
	}
	cfg.IPC.StatusDir = filepath.Join(dir, "status")
	cfg.IPC.ControlSocket = filepath.Join(dir, "run", "control.sock")
	cfg.IPC.MediaSocket = filepath.Join(dir, "run", "media.sock")
	cfg.Cameras = []CameraConfig{{Index: 1, LocalCameraID: "front", Expose: true, Name: "Front", Codec: "h265", PTZMode: "none"}}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}

	gateway, err := NewGateway(cfg, Credentials{values: map[string]string{"sip.password": "test"}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := gateway.Start(ctx); err != nil {
		t.Fatal(err)
	}

	if err := waitForPath(cfg.IPC.ControlSocket); err != nil {
		t.Fatal(err)
	}
	if err := waitForPath(cfg.IPC.MediaSocket); err != nil {
		t.Fatal(err)
	}
	if got := gateway.registry.CameraStatus("front"); got != "OFF" {
		t.Fatalf("new camera status = %q, want OFF", got)
	}
	if gateway.cascade == nil {
		t.Fatal("gateway did not assemble cascade service")
	}

	cancel()
	if err := gateway.Wait(); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{cfg.IPC.ControlSocket, cfg.IPC.MediaSocket} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("socket %q remains after shutdown: %v", path, err)
		}
	}
}

func freeUDPListenAddress(t *testing.T) string {
	t.Helper()
	conn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	return conn.LocalAddr().String()
}

func waitForPath(path string) error {
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return nil
		}
		time.Sleep(time.Millisecond)
	}
	return os.ErrNotExist
}

func eventually(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !condition() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !condition() {
		t.Fatal("condition did not converge")
	}
}

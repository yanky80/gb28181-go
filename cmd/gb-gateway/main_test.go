package main

import (
	"context"
	"errors"
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
	if err := os.MkdirAll(statusDir, 0750); err != nil {
		t.Fatal(err)
	}
	storePath := filepath.Join(statusDir, "channels.json")
	store, err := NewChannelStore(storePath)
	if err != nil {
		t.Fatal(err)
	}
	channel := testCascadeChannel()
	if err := store.UpsertCascadeChannel(context.Background(), channel); err != nil {
		t.Fatal(err)
	}

	cfg := defaultConfig()
	cfg.IPC.StatusDir = filepath.Join(dir, "status")
	cfg.IPC.ControlSocket = filepath.Join(dir, "control.sock")
	cfg.IPC.MediaSocket = filepath.Join(dir, "media.sock")
	cfg.Cameras = []CameraConfig{{Index: 1, LocalCameraID: "front", Expose: true, Name: "Front", Codec: "h264", PTZMode: "none"}}
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

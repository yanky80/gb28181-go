package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mickeyzzc/gb28181-go/metrics"
)

type blockingGatewayHooks struct {
	started chan struct{}
	release chan struct{}
	seen    atomic.Int64
}

func (h *blockingGatewayHooks) RegisterAttempt()      { h.observe() }
func (h *blockingGatewayHooks) RegisterOK()           { h.observe() }
func (h *blockingGatewayHooks) RegisterFail()         { h.observe() }
func (h *blockingGatewayHooks) KeepaliveFail()        { h.observe() }
func (h *blockingGatewayHooks) InviteSessionStarted() { h.observe() }
func (h *blockingGatewayHooks) InviteSessionStopped() { h.observe() }
func (h *blockingGatewayHooks) InviteFail()           { h.observe() }
func (h *blockingGatewayHooks) PSBytesOut(int64)      { h.observe() }
func (h *blockingGatewayHooks) ObserveGateway(metrics.GatewayEvent) {
	h.observe()
}

func (h *blockingGatewayHooks) observe() {
	if h.seen.Add(1) == 1 {
		close(h.started)
	}
	<-h.release
}

func TestGatewayObservabilityDropsForSlowConsumer(t *testing.T) {
	hooks := &blockingGatewayHooks{started: make(chan struct{}), release: make(chan struct{})}
	obs := NewGatewayObservability(1, hooks)
	defer func() {
		close(hooks.release)
		obs.Close()
	}()

	obs.Record(metrics.GatewayEvent{Name: "access_unit", Value: 1})
	select {
	case <-hooks.started:
	case <-time.After(time.Second):
		t.Fatal("slow hook was not called")
	}

	start := time.Now()
	for i := 0; i < 100; i++ {
		obs.Record(metrics.GatewayEvent{Name: "access_unit", Value: int64(i)})
	}
	if elapsed := time.Since(start); elapsed > 100*time.Millisecond {
		t.Fatalf("Record blocked behind slow consumer for %v", elapsed)
	}
	if got := obs.Snapshot().ObservabilityDrops; got == 0 {
		t.Fatal("slow consumer did not produce observable drops")
	}
}

func TestGatewayStateSnapshotNeverTears(t *testing.T) {
	state := newGatewayState(GatewaySnapshot{ProtocolVersion: "2022", Codec: "h265"})
	const updates = 1000
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for n := 0; n < updates; n++ {
				state.Store(GatewaySnapshot{
					ProtocolVersion: "2022",
					Codec:           "h265",
					Registration:    "ONLINE",
					Channels: []GatewayChannelSnapshot{{
						CameraID:    "cam-" + string(rune('a'+i)),
						Status:      "ON",
						StreamEpoch: uint64(n + 1),
					}},
				})
				got := state.Load()
				if got.ProtocolVersion != "2022" || got.Codec != "h265" || got.Registration != "ONLINE" {
					t.Errorf("torn snapshot = %+v", got)
					return
				}
				if len(got.Channels) != 1 || got.Channels[0].Status != "ON" || got.Channels[0].StreamEpoch == 0 {
					t.Errorf("torn channel snapshot = %+v", got.Channels)
					return
				}
			}
		}(i)
	}
	wg.Wait()
}

func TestGatewaySnapshotJSONContainsNoSecrets(t *testing.T) {
	snapshot := GatewaySnapshot{
		ProtocolVersion: "2022",
		Codec:           "h265",
		PeerGBVersion:   "3.0",
		Channels:        []GatewayChannelSnapshot{{CameraID: "front", GBChannelID: "34020000001320000001", Status: "OFF"}},
	}
	data, err := json.Marshal(snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "password") || strings.Contains(string(data), "Authorization") || strings.Contains(string(data), "rtsp://") {
		t.Fatalf("snapshot leaked secret material: %s", data)
	}
}

func TestWriteGatewaySnapshotReplacesFileAtomically(t *testing.T) {
	path := filepath.Join(t.TempDir(), gatewayStatusFile)
	snapshot := GatewaySnapshot{ProtocolVersion: "2022", Codec: "h265"}
	if err := writeGatewaySnapshot(path, snapshot); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var got GatewaySnapshot
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if got.ProtocolVersion != snapshot.ProtocolVersion || got.Codec != snapshot.Codec {
		t.Fatalf("snapshot = %+v, want %+v", got, snapshot)
	}
	if mode := func() os.FileMode { info, _ := os.Stat(path); return info.Mode().Perm() }(); mode != 0640 {
		t.Fatalf("snapshot mode = %o, want 640", mode)
	}
}

func TestGatewayWatchdogNotifierRunsOutsideBusinessPath(t *testing.T) {
	notifier := &countingNotifier{entered: make(chan struct{})}
	watchdog := newGatewayWatchdog(notifier, 1*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go watchdog.Run(ctx)
	select {
	case <-notifier.entered:
	case <-time.After(time.Second):
		t.Fatal("watchdog notifier was not called")
	}
	if notifier.calls.Load() == 0 {
		t.Fatal("watchdog call was not recorded")
	}
}

type countingNotifier struct {
	entered chan struct{}
	calls   atomic.Int64
}

func (n *countingNotifier) Notify() error {
	n.calls.Add(1)
	select {
	case <-n.entered:
	default:
		close(n.entered)
	}
	return nil
}

func TestGatewayLogFieldsRedactSecrets(t *testing.T) {
	var logs strings.Builder
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	defer slog.SetDefault(old)

	logGateway(slog.LevelWarn, "test", gatewayLogContext{
		cameraID: "front",
		err:      errors.New("Authorization=top-secret rtsp://camera/payload"),
	})
	output := logs.String()
	for _, secret := range []string{"top-secret", "Authorization", "rtsp://", "payload"} {
		if strings.Contains(output, secret) {
			t.Fatalf("log output contains %q: %s", secret, output)
		}
	}
	for _, field := range []string{"camera_id", "gb_channel_id", "call_id", "stream_epoch", "request_id", "codec", "protocol_version", "peer_gb_version", "transport", "ssrc", "error_code"} {
		if !strings.Contains(output, field) {
			t.Fatalf("log output missing structured field %q: %s", field, output)
		}
	}
}

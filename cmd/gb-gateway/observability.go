package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mickeyzzc/gb28181-go/metrics"
	"github.com/mickeyzzc/gb28181-go/platform/cascade"
)

const (
	gatewayStatusFile  = "gateway-status.json"
	observabilityQueue = 256
)

// GatewayChannelSnapshot is the immutable local view of one GB channel.
type GatewayChannelSnapshot struct {
	CameraID    string    `json:"camera_id"`
	GBChannelID string    `json:"gb_channel_id,omitempty"`
	Status      string    `json:"status"`
	LastHealth  time.Time `json:"last_health,omitempty"`
	StreamEpoch uint64    `json:"stream_epoch"`
	Codec       string    `json:"codec"`
}

// GatewaySnapshot is the atomically published gateway health view. Slices
// are copied on Store and Load so readers never share mutable state.
type GatewaySnapshot struct {
	GeneratedAt     time.Time                `json:"generated_at"`
	Registration    string                   `json:"registration"`
	Channels        []GatewayChannelSnapshot `json:"channels"`
	ActiveDialogs   int                      `json:"active_dialogs"`
	ProtocolVersion string                   `json:"protocol_version"`
	Codec           string                   `json:"codec"`
	PeerGBVersion   string                   `json:"peer_gb_version,omitempty"`
	VersionMismatch bool                     `json:"version_mismatch"`
	Metrics         GatewayMetricsSnapshot   `json:"metrics"`
}

// GatewayMetricsSnapshot contains monotonic counters and the current IPC
// command queue depth. All fields are safe to read while the gateway runs.
type GatewayMetricsSnapshot struct {
	IPCConnections     int64 `json:"ipc_connections"`
	IPCDisconnects     int64 `json:"ipc_disconnects"`
	AccessUnits        int64 `json:"access_units"`
	AUBytes            int64 `json:"au_bytes"`
	QueueDepth         int64 `json:"queue_depth"`
	QueueDrops         int64 `json:"queue_drops"`
	FrameDrops         int64 `json:"frame_drops"`
	Discontinuities    int64 `json:"discontinuities"`
	IDRWaits           int64 `json:"idr_waits"`
	RegisterAttempts   int64 `json:"register_attempts"`
	RegisterOK         int64 `json:"register_ok"`
	RegisterFailures   int64 `json:"register_failures"`
	HeartbeatFailures  int64 `json:"heartbeat_failures"`
	CatalogResults     int64 `json:"catalog_results"`
	InviteStarted      int64 `json:"invite_started"`
	InviteStopped      int64 `json:"invite_stopped"`
	InviteFailures     int64 `json:"invite_failures"`
	RTPPackets         int64 `json:"rtp_packets"`
	RTPBytes           int64 `json:"rtp_bytes"`
	PlaybackSuccesses  int64 `json:"playback_successes"`
	PlaybackFailures   int64 `json:"playback_failures"`
	ObservabilityDrops int64 `json:"observability_drops"`
}

type gatewayMetricCounters struct {
	ipcConnections, ipcDisconnects, accessUnits, auBytes              atomic.Int64
	queueDepth, queueDrops, frameDrops, discontinuities, idrWaits     atomic.Int64
	registerAttempts, registerOK, registerFailures, heartbeatFailures atomic.Int64
	catalogResults, inviteStarted, inviteStopped, inviteFailures      atomic.Int64
	rtpPackets, rtpBytes, playbackSuccesses, playbackFailures         atomic.Int64
	observabilityDrops                                                atomic.Int64
}

func (c *gatewayMetricCounters) snapshot() GatewayMetricsSnapshot {
	return GatewayMetricsSnapshot{
		IPCConnections: c.ipcConnections.Load(), IPCDisconnects: c.ipcDisconnects.Load(),
		AccessUnits: c.accessUnits.Load(), AUBytes: c.auBytes.Load(), QueueDepth: c.queueDepth.Load(),
		QueueDrops: c.queueDrops.Load(), FrameDrops: c.frameDrops.Load(),
		Discontinuities: c.discontinuities.Load(), IDRWaits: c.idrWaits.Load(),
		RegisterAttempts: c.registerAttempts.Load(), RegisterOK: c.registerOK.Load(),
		RegisterFailures: c.registerFailures.Load(), HeartbeatFailures: c.heartbeatFailures.Load(),
		CatalogResults: c.catalogResults.Load(), InviteStarted: c.inviteStarted.Load(),
		InviteStopped: c.inviteStopped.Load(), InviteFailures: c.inviteFailures.Load(),
		RTPPackets: c.rtpPackets.Load(), RTPBytes: c.rtpBytes.Load(),
		PlaybackSuccesses: c.playbackSuccesses.Load(), PlaybackFailures: c.playbackFailures.Load(),
		ObservabilityDrops: c.observabilityDrops.Load(),
	}
}

type observabilityEvent struct {
	metric string
	event  metrics.GatewayEvent
}

// GatewayObservability is the nonblocking adapter between gateway seams and
// a host's metrics hooks. Business goroutines only update atomics and enqueue
// into a bounded channel; a slow host hook can lose observations, never block
// IPC or media.
type GatewayObservability struct {
	counters gatewayMetricCounters
	events   chan observabilityEvent
	done     chan struct{}
	close    sync.Once

	hooksMu sync.RWMutex
	hooks   metrics.Hooks
}

func NewGatewayObservability(queueSize int, hooks metrics.Hooks) *GatewayObservability {
	if queueSize <= 0 {
		queueSize = observabilityQueue
	}
	if hooks == nil {
		hooks = metrics.NoopHooks{}
	}
	o := &GatewayObservability{events: make(chan observabilityEvent, queueSize), done: make(chan struct{}), hooks: hooks}
	go o.dispatch()
	return o
}

func (o *GatewayObservability) SetHooks(hooks metrics.Hooks) {
	if hooks == nil {
		hooks = metrics.NoopHooks{}
	}
	o.hooksMu.Lock()
	o.hooks = hooks
	o.hooksMu.Unlock()
}

func (o *GatewayObservability) Close() { o.close.Do(func() { close(o.done) }) }

func (o *GatewayObservability) Snapshot() GatewayMetricsSnapshot { return o.counters.snapshot() }

func (o *GatewayObservability) Record(event metrics.GatewayEvent) {
	o.count(event)
	metric := event.Name
	switch metric {
	case "register_attempt", "register_ok", "register_failure", "heartbeat_failure",
		"invite_started", "invite_stopped", "invite_failure", "ps_bytes_out":
	default:
		metric = ""
	}
	select {
	case <-o.done:
		return
	case o.events <- observabilityEvent{metric: metric, event: event}:
	default:
		o.counters.observabilityDrops.Add(1)
	}
}

func (o *GatewayObservability) ObserveGateway(event metrics.GatewayEvent) { o.Record(event) }

func (o *GatewayObservability) RegisterAttempt() {
	o.Record(metrics.GatewayEvent{Name: "register_attempt"})
}
func (o *GatewayObservability) RegisterOK() { o.Record(metrics.GatewayEvent{Name: "register_ok"}) }
func (o *GatewayObservability) RegisterFail() {
	o.Record(metrics.GatewayEvent{Name: "register_failure"})
}
func (o *GatewayObservability) KeepaliveFail() {
	o.Record(metrics.GatewayEvent{Name: "heartbeat_failure"})
}
func (o *GatewayObservability) InviteSessionStarted() {
	o.Record(metrics.GatewayEvent{Name: "invite_started"})
}
func (o *GatewayObservability) InviteSessionStopped() {
	o.Record(metrics.GatewayEvent{Name: "invite_stopped"})
}
func (o *GatewayObservability) InviteFail() { o.Record(metrics.GatewayEvent{Name: "invite_failure"}) }
func (o *GatewayObservability) PSBytesOut(n int64) {
	o.Record(metrics.GatewayEvent{Name: "ps_bytes_out", Value: n})
}

func (o *GatewayObservability) count(event metrics.GatewayEvent) {
	switch event.Name {
	case "ipc_connection":
		o.counters.ipcConnections.Add(1)
	case "ipc_disconnect":
		o.counters.ipcDisconnects.Add(1)
	case "access_unit":
		o.counters.accessUnits.Add(1)
		o.counters.auBytes.Add(event.Value)
	case "queue_depth":
		o.counters.queueDepth.Store(event.Value)
	case "queue_drop":
		o.counters.queueDrops.Add(1)
	case "frame_drop":
		o.counters.frameDrops.Add(1)
	case "discontinuity":
		o.counters.discontinuities.Add(1)
	case "idr_wait":
		o.counters.idrWaits.Add(1)
	case "register_attempt":
		o.counters.registerAttempts.Add(1)
	case "register_ok":
		o.counters.registerOK.Add(1)
	case "register_failure":
		o.counters.registerFailures.Add(1)
	case "heartbeat_failure":
		o.counters.heartbeatFailures.Add(1)
	case "catalog_result":
		o.counters.catalogResults.Add(1)
	case "invite_started":
		o.counters.inviteStarted.Add(1)
	case "invite_stopped":
		o.counters.inviteStopped.Add(1)
	case "invite_failure":
		o.counters.inviteFailures.Add(1)
	case "rtp_packet":
		o.counters.rtpPackets.Add(1)
		o.counters.rtpBytes.Add(event.Value)
	case "playback_success":
		o.counters.playbackSuccesses.Add(1)
	case "playback_failure":
		o.counters.playbackFailures.Add(1)
	}
}

func (o *GatewayObservability) dispatch() {
	for {
		select {
		case <-o.done:
			return
		case item := <-o.events:
			o.hooksMu.RLock()
			hooks := o.hooks
			o.hooksMu.RUnlock()
			if gatewayHooks, ok := hooks.(metrics.GatewayHooks); ok {
				gatewayHooks.ObserveGateway(item.event)
			}
			switch item.metric {
			case "register_attempt":
				hooks.RegisterAttempt()
			case "register_ok":
				hooks.RegisterOK()
			case "register_failure":
				hooks.RegisterFail()
			case "heartbeat_failure":
				hooks.KeepaliveFail()
			case "invite_started":
				hooks.InviteSessionStarted()
			case "invite_stopped":
				hooks.InviteSessionStopped()
			case "invite_failure":
				hooks.InviteFail()
			case "ps_bytes_out":
				hooks.PSBytesOut(item.event.Value)
			}
		}
	}
}

type gatewayState struct {
	value atomic.Pointer[GatewaySnapshot]
}

func newGatewayState(snapshot GatewaySnapshot) *gatewayState {
	s := &gatewayState{}
	s.Store(snapshot)
	return s
}

func (s *gatewayState) Store(snapshot GatewaySnapshot) {
	copy := snapshot
	copy.Channels = append([]GatewayChannelSnapshot(nil), snapshot.Channels...)
	s.value.Store(&copy)
}

func (s *gatewayState) Load() GatewaySnapshot {
	snapshot := s.value.Load()
	if snapshot == nil {
		return GatewaySnapshot{}
	}
	copy := *snapshot
	copy.Channels = append([]GatewayChannelSnapshot(nil), snapshot.Channels...)
	return copy
}

// WatchdogNotifier is intentionally tiny so tests and service managers can
// inject a notifier without coupling the gateway to systemd.
type WatchdogNotifier interface {
	Notify() error
}

type gatewayWatchdog struct {
	notifier WatchdogNotifier
	interval time.Duration
}

func newGatewayWatchdog(notifier WatchdogNotifier, interval time.Duration) *gatewayWatchdog {
	if interval <= 0 {
		interval = time.Second
	}
	return &gatewayWatchdog{notifier: notifier, interval: interval}
}

func (w *gatewayWatchdog) Run(ctx context.Context) {
	if w == nil || w.notifier == nil {
		return
	}
	_ = w.notifier.Notify()
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			_ = w.notifier.Notify()
		case <-ctx.Done():
			return
		}
	}
}

type systemdWatchdogNotifier struct {
	socket string
}

// NewSystemdWatchdogNotifier returns nil when systemd did not enable a
// watchdog for this process.
func NewSystemdWatchdogNotifier() WatchdogNotifier {
	if os.Getenv("NOTIFY_SOCKET") == "" || os.Getenv("WATCHDOG_USEC") == "" {
		return nil
	}
	return &systemdWatchdogNotifier{socket: os.Getenv("NOTIFY_SOCKET")}
}

func (n *systemdWatchdogNotifier) Notify() error {
	address := n.socket
	if strings.HasPrefix(address, "@") {
		address = "\x00" + address[1:]
	}
	conn, err := net.DialUnix("unixgram", nil, &net.UnixAddr{Name: address, Net: "unixgram"})
	if err != nil {
		return err
	}
	defer conn.Close()
	_ = conn.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))
	_, err = conn.Write([]byte("WATCHDOG=1\n"))
	return err
}

func systemdWatchdogInterval() time.Duration {
	usec, err := strconv.ParseInt(os.Getenv("WATCHDOG_USEC"), 10, 64)
	if err != nil || usec <= 0 {
		return 0
	}
	return time.Duration(usec) * time.Microsecond / 2
}

func writeGatewaySnapshot(path string, snapshot GatewaySnapshot) error {
	if path == "" {
		return errors.New("gateway snapshot path is empty")
	}
	data, err := json.Marshal(snapshot)
	if err != nil {
		return fmt.Errorf("encode gateway snapshot: %w", err)
	}
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".gateway-status.json.tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	remove := true
	defer func() {
		if remove {
			_ = os.Remove(tmpName)
		}
	}()
	if _, err := tmp.Write(append(data, '\n')); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Chmod(tmpName, 0640); err != nil {
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	remove = false
	return nil
}

type gatewayLogContext struct {
	cameraID, gbChannelID, callID string
	streamEpoch, requestID        uint64
	codec, protocolVersion        string
	peerGBVersion, transport      string
	ssrc                          uint32
	err                           error
}

func logGateway(level slog.Level, message string, fields gatewayLogContext) {
	errorCode := ""
	if fields.err != nil {
		errorCode = "operation_failed"
		if errors.Is(fields.err, context.Canceled) {
			errorCode = "context_canceled"
		} else if errors.Is(fields.err, context.DeadlineExceeded) {
			errorCode = "deadline_exceeded"
		}
	}
	slog.Log(context.Background(), level, message,
		"camera_id", fields.cameraID,
		"gb_channel_id", fields.gbChannelID,
		"call_id", fields.callID,
		"stream_epoch", fields.streamEpoch,
		"request_id", fields.requestID,
		"codec", fields.codec,
		"protocol_version", fields.protocolVersion,
		"peer_gb_version", fields.peerGBVersion,
		"transport", fields.transport,
		"ssrc", fields.ssrc,
		"error_code", errorCode,
	)
}

func (g *Gateway) refreshSnapshot() GatewaySnapshot {
	channels, _ := g.store.ListCascadeChannels(context.Background())
	byCamera := make(map[string]string, len(channels))
	for _, channel := range channels {
		byCamera[channel.CameraID] = channel.GBChannelID
	}
	views := g.registry.Snapshot()
	channelViews := make([]GatewayChannelSnapshot, 0, len(views))
	for _, view := range views {
		status := "OFF"
		if view.Online {
			status = "ON"
		}
		channelViews = append(channelViews, GatewayChannelSnapshot{
			CameraID: view.ID, GBChannelID: byCamera[view.ID], Status: status,
			LastHealth: view.LastHealth, StreamEpoch: view.StreamEpoch, Codec: codecName(view.Codec),
		})
	}
	registration := "DISABLED"
	peerVersion := ""
	activeDialogs := 0
	if g.cfg.GB.Enabled && g.cascade != nil {
		registration = g.cascade.Status()
		peerVersion = g.cascade.UpperProtocolVersion()
		activeDialogs = g.cascade.ForwardCount()
	}
	codec := ""
	if len(g.cfg.Cameras) > 0 {
		codec = g.cfg.Cameras[0].Codec
	}
	snapshot := GatewaySnapshot{
		GeneratedAt: time.Now(), Registration: registration, Channels: channelViews,
		ActiveDialogs: activeDialogs, ProtocolVersion: g.cfg.GB.ProtocolVersion,
		Codec: codec, PeerGBVersion: peerVersion,
		VersionMismatch: registration == cascade.StatusVersionMismatch,
		Metrics:         g.observability.Snapshot(),
	}
	g.state.Store(snapshot)
	return snapshot
}

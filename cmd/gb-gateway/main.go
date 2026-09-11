package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/mickeyzzc/gb28181-go/edgeipc"
	"github.com/mickeyzzc/gb28181-go/platform/cascade"
	"github.com/mickeyzzc/gb28181-go/platform/mp4"
)

// Gateway owns the one-process lifecycle for the local camera registry, both
// Edge IPC sockets, persistence, and the optional cascade client.
type Gateway struct {
	cfg      Config
	registry *CameraRegistry
	store    cascade.Store
	control  *ControlServer
	media    *MediaHost
	cascade  *cascade.Service

	recordings *RecordingStore

	mu       sync.Mutex
	cancel   context.CancelFunc
	started  bool
	startErr error
	stop     sync.Once
	stopErr  error
	wg       sync.WaitGroup
}

// NewGateway restores persistent state and creates every runtime seam. It
// does not bind sockets or start registration until Start is called.
func NewGateway(cfg Config, credentials Credentials) (*Gateway, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(cfg.IPC.StatusDir, 0750); err != nil {
		return nil, fmt.Errorf("create gateway status directory: %w", err)
	}
	for _, socket := range []string{cfg.IPC.ControlSocket, cfg.IPC.MediaSocket} {
		if err := os.MkdirAll(filepath.Dir(socket), 0750); err != nil {
			return nil, fmt.Errorf("create socket directory: %w", err)
		}
	}

	storePath := filepath.Join(cfg.IPC.StatusDir, "channels.json")
	if err := os.MkdirAll(filepath.Dir(storePath), 0750); err != nil {
		return nil, fmt.Errorf("create channel store directory: %w", err)
	}
	channels, err := NewChannelStore(storePath)
	if err != nil {
		return nil, err
	}

	var recordings *RecordingStore
	if cfg.GB.RecordPlayback {
		indexPath := filepath.Join(cfg.IPC.StatusDir, "recordings.jsonl")
		root := filepath.Join(cfg.IPC.StatusDir, "recordings")
		if err := os.MkdirAll(filepath.Dir(indexPath), 0750); err != nil {
			return nil, fmt.Errorf("create recording index directory: %w", err)
		}
		if err := os.MkdirAll(root, 0750); err != nil {
			return nil, fmt.Errorf("create recording root: %w", err)
		}
		recordings, err = NewRecordingStore(indexPath, root)
		if err != nil {
			return nil, err
		}
	}

	specs := make([]CameraSpec, 0, len(cfg.Cameras))
	for _, camera := range cfg.Cameras {
		specs = append(specs, CameraSpec{
			ID:            camera.LocalCameraID,
			Name:          camera.Name,
			Codec:         configuredCodec(camera.Codec),
			CascadeHidden: !camera.Expose,
		})
	}
	registry, err := NewCameraRegistry(specs...)
	if err != nil {
		return nil, err
	}

	store := &gatewayStore{channels: channels, recordings: recordings}
	control := NewControlServer(cfg.IPC.ControlSocket, registry, cfg.GB.StopGrace)
	gbPassword, _ := credentials.Lookup("sip.password")
	cascadeService := cascade.New(cascade.Config{
		Enabled:           cfg.GB.Enabled,
		ProtocolVersion:   cfg.GB.ProtocolVersion,
		ServerAddr:        cfg.GB.ServerAddr,
		ServerDomain:      cfg.GB.ServerDomain,
		LocalDeviceID:     cfg.GB.LocalDeviceID,
		Realm:             cfg.GB.Realm,
		Password:          gbPassword,
		SIPListen:         cfg.GB.SIPListen,
		HeartbeatInterval: cfg.GB.Heartbeat.String(),
		RegisterExpires:   cfg.GB.RegisterExpires,
	}, registry, store)
	cascadeService.SetMainStreamAcquirer(control)
	if recordings != nil {
		cascadeService.SetSegmentParser(mp4.ParseSegment)
	}
	media := NewMediaHost(registry, MediaHostConfig{
		Path:       cfg.IPC.MediaSocket,
		MaxAUBytes: cfg.IPC.MaxAUBytes,
		IDRTimeout: cfg.GB.IDRTimeout,
		RequestIDR: control.RequestIDR,
		OnIDRTimeoutEpoch: func(cameraID string, streamEpoch uint64, _ error) {
			registry.HandleDisconnect(cameraID, streamEpoch)
			cascadeService.NotifyCameraUnavailable(cameraID)
		},
	})

	return &Gateway{
		cfg: cfg, registry: registry, store: store, control: control,
		media: media, cascade: cascadeService, recordings: recordings,
	}, nil
}

func configuredCodec(codec string) edgeipc.Codec {
	if codec == "h264" {
		return edgeipc.CodecH264
	}
	return edgeipc.CodecH265
}

// Start binds both local IPC sockets before starting the cascade registration
// loop. Cameras are already present in the registry and therefore advertise
// stable OFF catalog entries before an encoder connects.
func (g *Gateway) Start(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	g.mu.Lock()
	if g.started {
		g.mu.Unlock()
		return errors.New("gateway: already started")
	}
	if g.startErr != nil {
		err := g.startErr
		g.mu.Unlock()
		return err
	}
	runCtx, cancel := context.WithCancel(ctx)
	g.cancel = cancel
	g.mu.Unlock()

	if err := g.control.Start(runCtx); err != nil {
		cancel()
		g.recordStartError(err)
		return err
	}
	if _, err := g.media.Listen(); err != nil {
		_ = g.control.Stop()
		cancel()
		g.recordStartError(err)
		return err
	}
	if g.recordings != nil {
		if err := g.recordings.Scan(runCtx); err != nil && !errors.Is(err, context.Canceled) {
			_ = g.media.Close()
			_ = g.control.Stop()
			cancel()
			g.recordStartError(err)
			return err
		}
	}

	g.mu.Lock()
	g.started = true
	g.mu.Unlock()
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		_ = g.media.Serve(runCtx)
	}()
	if g.recordings != nil {
		g.wg.Add(1)
		go g.recordingLoop(runCtx)
	}

	if g.cfg.GB.Enabled {
		if err := g.cascade.Start(runCtx); err != nil {
			_ = g.Stop()
			g.recordStartError(err)
			return err
		}
	}
	slog.Info("gb-gateway started", "control_socket", g.cfg.IPC.ControlSocket, "media_socket", g.cfg.IPC.MediaSocket)
	return nil
}

// Stop first blocks new cascade admissions and releases active media dialogs,
// then closes the local sockets and waits for their goroutines.
func (g *Gateway) Stop() error {
	g.stop.Do(func() {
		g.mu.Lock()
		cancel := g.cancel
		g.mu.Unlock()
		if cancel != nil {
			cancel()
		}
		if g.cascade != nil && g.cfg.GB.Enabled {
			g.stopErr = g.cascade.Stop()
		}
		if err := g.media.Close(); g.stopErr == nil && err != nil {
			g.stopErr = err
		}
		if err := g.control.Stop(); g.stopErr == nil && err != nil {
			g.stopErr = err
		}
		g.wg.Wait()
	})
	return g.stopErr
}

// Wait is the signal-friendly shutdown entrypoint.
func (g *Gateway) Wait() error { return g.Stop() }

func (g *Gateway) recordingLoop(ctx context.Context) {
	defer g.wg.Done()
	interval := g.cfg.GB.Heartbeat
	if interval <= 0 {
		interval = time.Minute
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := g.recordings.Scan(ctx); err != nil && !errors.Is(err, context.Canceled) {
				slog.Warn("gb-gateway recording scan failed", "error", err)
			}
		case <-ctx.Done():
			return
		}
	}
}

func (g *Gateway) recordStartError(err error) {
	g.mu.Lock()
	g.startErr = err
	g.mu.Unlock()
}

type gatewayStore struct {
	channels   *ChannelStore
	recordings *RecordingStore
}

func (s *gatewayStore) UpsertCascadeChannel(ctx context.Context, ch cascade.CascadeChannel) error {
	return s.channels.UpsertCascadeChannel(ctx, ch)
}

func (s *gatewayStore) ListCascadeChannels(ctx context.Context) ([]cascade.CascadeChannel, error) {
	return s.channels.ListCascadeChannels(ctx)
}

func (s *gatewayStore) ListRecordings(ctx context.Context, filter cascade.RecordingFilter) ([]cascade.Recording, error) {
	if s.recordings == nil {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return nil, nil
	}
	return s.recordings.ListRecordings(ctx, filter)
}

func (s *gatewayStore) AllocateCascadeChannel(ctx context.Context, cameraID, prefix, name string) (cascade.CascadeChannel, error) {
	return s.channels.AllocateCascadeChannel(ctx, cameraID, prefix, name)
}

var _ cascade.Store = (*gatewayStore)(nil)
var _ cascade.CascadeChannelAllocator = (*gatewayStore)(nil)

func main() {
	if err := run(os.Args[1:]); err != nil {
		slog.Error("gb-gateway failed", "error", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	flags := flag.NewFlagSet("gb-gateway", flag.ContinueOnError)
	configPath := flags.String("config", "/etc/edge-gateway/gateway.conf", "gateway configuration file")
	credentialsPath := flags.String("credentials", "/etc/edge-gateway/credentials", "0600 SIP credentials file")
	if err := flags.Parse(args); err != nil {
		return err
	}
	cfg, err := LoadConfig(*configPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	credentials := Credentials{}
	if cfg.GB.Enabled {
		credentials, err = LoadCredentials(*credentialsPath)
		if err != nil {
			return fmt.Errorf("load credentials: %w", err)
		}
	}
	gateway, err := NewGateway(cfg, credentials)
	if err != nil {
		return fmt.Errorf("assemble gateway: %w", err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := gateway.Start(ctx); err != nil {
		return fmt.Errorf("start gateway: %w", err)
	}
	<-ctx.Done()
	return gateway.Wait()
}

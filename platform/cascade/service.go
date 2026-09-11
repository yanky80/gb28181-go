// Package cascade implements the GB/T 28181 LOWER-LEVEL platform role: the
// NVR registers to an upper-level platform (SIP UAC), answers its catalog
// queries with an aggregated view of all local cameras, and on the upper
// platform's INVITE forwards the camera's stream as RTP/PS (via psmux).
//
// The upper platform needs no cascade-specific support — any GB/T 28181
// platform implementation (including this NVR's own platform role) can be the
// upper side: REGISTER / Catalog Query / INVITE are all standard.
package cascade

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"net"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ghettovoice/gosip"
	"github.com/ghettovoice/gosip/sip"
	"github.com/mickeyzzc/gb28181-go/backoff"
	"github.com/mickeyzzc/gb28181-go/manscdp"
	"github.com/mickeyzzc/gb28181-go/metrics"
	"github.com/mickeyzzc/gb28181-go/platform"
	mbsip "github.com/mickeyzzc/gb28181-go/platform/sip"
)

// CameraInfo is the cascade's view of a local camera.
type CameraInfo struct {
	ID    string
	Name  string
	Brand string
	Model string
	// PTZMode names the configured local control path. An empty mode preserves
	// the legacy injected-forwarder seam; "none" explicitly disables PTZ.
	PTZMode string
	// Encoding is the camera's configured codec ("h264"|"h265"). Empty uses
	// the selected protocol profile's default (H.265 for the default profile).
	Encoding string
	// SubStream selects the camera's low-res tier for forwarding when true
	// (#512): the INVITE acquires the on-demand sub-stream instead of the
	// main hub, falling back to main when unavailable.
	SubStream bool
	// CascadeHidden excludes the camera from the aggregated catalog and makes
	// INVITEs for its channel fail with 404 (catalog convergence: expose only
	// a chosen subset to the upper platform).
	CascadeHidden bool
}

// CameraSource supplies the local camera list and their stream hubs. The
// camera manager adapts to it (pkg/app wiring).
type CameraSource interface {
	Cameras() []CameraInfo
	Hub(cameraID string) *platform.FrameHub
}

// CameraStatusSource is an optional dynamic status view for CameraSource.
// Sources that do not implement it retain the historical catalog behavior:
// every visible camera is reported as ON. An empty or non-ON status is OFF.
type CameraStatusSource interface {
	CameraStatus(cameraID string) string
}

// SubStreamAcquirer grants the cascade access to the on-demand sub-stream
// tier (#513): one INVITE holds one reference for its lifetime. Nil (or an
// error) falls back to main-stream forwarding.
type SubStreamAcquirer interface {
	AcquireSubHub(ctx context.Context, cameraID string) (hub *platform.FrameHub, release func(), err error)
}

// MainStreamAcquirer grants one live dialog a main-stream lease. Nil keeps the
// legacy CameraSource.Hub path, where the host owns the stream lifetime.
type MainStreamAcquirer interface {
	AcquireMainHub(ctx context.Context, cameraID string) (hub *platform.FrameHub, release func(), err error)
}

// upper is one upper-platform registration session (#370): its own REGISTER /
// keepalive loop and online state over the shared SIP listener. The single
// legacy config form becomes uppers[0]; gb28181_cascade.upstreams appends
// more.
type upper struct {
	cfg                 Upstream // resolved — defaults filled in
	online              bool
	regTS               time.Time
	wake                chan struct{}
	generation          atomic.Uint64
	stale               atomic.Bool
	protocolVersion     string
	protocolVersionSeen bool
}

// Service is the cascade client (pkg/app.Service "gb28181-cascade").
type Service struct {
	cfg     Config
	src     CameraSource
	db      Store
	metrics metrics.Hooks
	// segParser reads recorded segment files for playback forwarding; injected
	// via SetSegmentParser, nil makes Playback/Download INVITEs fail closed
	// (RecordInfo answers still work off the Store).
	segParser SegmentParser
	capMu     sync.RWMutex
	// subAcq serves sub-stream forwardings (#512); nil = main-only.
	subAcq SubStreamAcquirer
	// mainAcq serves live main-stream leases; nil preserves the legacy Hub path.
	mainAcq MainStreamAcquirer

	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	storeMu     sync.RWMutex
	storeCtx    context.Context
	storeCancel context.CancelFunc

	// admissionMu serializes INVITE admission with Stop. ponytail: one global
	// lock keeps lifecycle ordering simple; per-camera admission if throughput
	// ever makes serialized INVITEs measurable. stopping is set before Stop
	// waits on the mutex, so new INVITEs reject while an in-flight acquire can
	// still observe the cancelled service context.
	admissionMu sync.Mutex
	stopping    atomic.Bool
	stopOnce    sync.Once
	stopErr     error

	srv gosip.Server

	sn atomic.Int64 // MANSCDP sequence numbers

	// REGISTER retry backoff bounds (issue #44), resolved from Config in
	// New; per-upper Backoff objects live inside registerLoop.
	retryBase time.Duration
	retryMax  time.Duration
	retryMu   sync.Mutex
	retryRand func(int64) int64
	now       func() time.Time
	wait      func(context.Context, time.Duration, <-chan struct{}) bool

	uppers []*upper // #370: one entry per upper platform

	mu sync.Mutex
	// ponytail: one global admission lock; split by upper only if INVITE
	// throughput makes serialized admission measurable.
	stopDone    chan struct{}
	sessions    map[string]*mediaSession    // SIP Call-ID → active live forward
	playbacks   map[string]*playbackSession // SIP Call-ID → active playback dialog
	subs        map[string]*catalogSub      // catalog subscriptions (SUBSCRIBE → NOTIFY, #370)
	ptzForward  PTZForwarder
	ptzMu       sync.Mutex
	ptzAdapters map[string]PTZAdapter
	ptzMotion   map[string]*ptzMotion
	ptzLease    time.Duration
	ptzAudit    PTZAuditSink
	ptzState    PTZStateSink
	tzMu        sync.RWMutex
	gbLoc       *time.Location // GB naive-clock zone (nil → time.Local)
	channelMu   sync.RWMutex
	// nil-Store channel bindings live for this Service's lifetime.
	channelBindings   map[string]string // GB channel ID → camera ID
	nextChannelSerial int
}

// buildUppers resolves the configured upper platforms: the legacy single form
// (ServerAddr non-empty) first, then every upstreams[] entry with unset
// fields inherited from the single form.
func buildUppers(cfg Config) []*upper {
	var uppers []*upper
	if cfg.ServerAddr != "" {
		uppers = append(uppers, &upper{cfg: Upstream{
			ServerDomain:      cfg.ServerDomain,
			ServerAddr:        cfg.ServerAddr,
			LocalDeviceID:     cfg.LocalDeviceID,
			Realm:             cfg.Realm,
			Password:          cfg.Password,
			HeartbeatInterval: cfg.HeartbeatInterval,
			RegisterExpires:   cfg.RegisterExpires,
		}, wake: make(chan struct{}, 1)})
	}
	for _, u := range cfg.Upstreams {
		if u.ServerAddr == "" {
			continue
		}
		if u.LocalDeviceID == "" {
			u.LocalDeviceID = cfg.LocalDeviceID
		}
		if u.Realm == "" {
			u.Realm = cfg.Realm
		}
		if u.Password == "" {
			u.Password = cfg.Password
		}
		if u.HeartbeatInterval == "" {
			u.HeartbeatInterval = cfg.HeartbeatInterval
		}
		if u.RegisterExpires == 0 {
			u.RegisterExpires = cfg.RegisterExpires
		}
		uppers = append(uppers, &upper{cfg: u, wake: make(chan struct{}, 1)})
	}
	return uppers
}

// SetSubStreamAcquirer wires the on-demand sub-stream provider (#512). Call
// once at wiring time, before Start.
func (s *Service) SetSubStreamAcquirer(a SubStreamAcquirer) { s.subAcq = a }

// SetMainStreamAcquirer wires the host's main-stream lease provider. Call once
// at wiring time, before Start.
func (s *Service) SetMainStreamAcquirer(a MainStreamAcquirer) { s.mainAcq = a }

func New(cfg Config, src CameraSource, db Store) *Service {
	retryBase := parseRetryDuration(cfg.RegisterRetryBase, registerRetryBaseDefault)
	retryMax := parseRetryDuration(cfg.RegisterRetryMax, registerRetryMaxDefault)
	if retryMax < retryBase {
		retryMax = retryBase
	}
	lease := cfg.PTZLeaseTimeout
	if lease <= 0 {
		lease = 5 * time.Second
	}
	return &Service{
		cfg: cfg, src: src, db: db,
		metrics:         metrics.NoopHooks{},
		retryBase:       retryBase,
		retryMax:        retryMax,
		retryRand:       rand.Int63n,
		now:             time.Now,
		wait:            waitCtx,
		uppers:          buildUppers(cfg),
		sessions:        make(map[string]*mediaSession),
		playbacks:       make(map[string]*playbackSession),
		subs:            make(map[string]*catalogSub),
		channelBindings: make(map[string]string),
		ptzAdapters:     make(map[string]PTZAdapter),
		ptzMotion:       make(map[string]*ptzMotion),
		ptzLease:        lease,
		stopDone:        make(chan struct{}),
	}
}

// SetMetricsHooks wires the optional event sink. It is a setup-time seam;
// legacy Hooks implementations remain supported when GatewayHooks is absent.
func (s *Service) SetMetricsHooks(h metrics.Hooks) {
	if h == nil {
		h = metrics.NoopHooks{}
	}
	s.metrics = h
}

func (s *Service) observe(event metrics.GatewayEvent, legacy func(metrics.Hooks)) {
	hooks := s.metrics
	if hooks == nil {
		hooks = metrics.NoopHooks{}
	}
	if h, ok := hooks.(metrics.GatewayHooks); ok {
		h.ObserveGateway(event)
		return
	}
	if legacy != nil {
		legacy(hooks)
	}
}

func (s *Service) observeInviteFailure(callID, channelID, code string) {
	s.observe(metrics.GatewayEvent{
		Name: "invite_failure", CallID: callID, GBChannelID: channelID, ErrorCode: code,
	}, func(h metrics.Hooks) { h.InviteFail() })
}

func (s *Service) upperVersion(u *upper) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if u == nil {
		return ""
	}
	return u.protocolVersion
}

func (s *Service) storeContext(ctxs ...context.Context) context.Context {
	if len(ctxs) > 0 && ctxs[0] != nil {
		return ctxs[0]
	}
	s.storeMu.RLock()
	ctx := s.storeCtx
	if ctx == nil {
		ctx = s.ctx
	}
	s.storeMu.RUnlock()
	if ctx != nil {
		return ctx
	}
	return context.Background()
}

func (s *Service) cancelStoreContext() {
	s.storeMu.RLock()
	cancel := s.storeCancel
	s.storeMu.RUnlock()
	if cancel != nil {
		cancel()
	}
}

func (s *Service) resetStoreContext() {
	s.storeMu.Lock()
	defer s.storeMu.Unlock()
	if s.storeCancel != nil {
		s.storeCancel()
	}
	if s.ctx != nil {
		s.storeCtx, s.storeCancel = context.WithCancel(s.ctx)
	} else {
		s.storeCtx, s.storeCancel = nil, nil
	}
}

// SetRegisterRetryRandomSource replaces the bounded-jitter source. It is a
// wiring/test seam and should be called before Start; nil restores the
// standard-library source.
func (s *Service) SetRegisterRetryRandomSource(random func(int64) int64) {
	if random == nil {
		random = rand.Int63n
	}
	s.retryMu.Lock()
	s.retryRand = random
	s.retryMu.Unlock()
}

// REGISTER retry backoff bounds (issue #44): base doubles per
// consecutive failure, capped; a successful registration resets.
const (
	registerRetryBaseDefault   = time.Second
	registerRetryMaxDefault    = 5 * time.Minute
	registerRetryJitterDivisor = 10 // positive jitter is bounded to 10% of wait
)

// parseRetryDuration parses a config duration, falling back to def for
// empty or malformed values (config-style tolerance: a bad unit string
// must not brick registration).
func parseRetryDuration(v string, def time.Duration) time.Duration {
	if v == "" {
		return def
	}
	if d, err := time.ParseDuration(v); err == nil && d > 0 {
		return d
	}
	return def
}

// SetSegmentParser injects the host's recorded-segment reader (fMP4 or
// otherwise). Without it, playback INVITEs are rejected as unavailable.
func (s *Service) SetSegmentParser(p SegmentParser) {
	s.capMu.Lock()
	s.segParser = p
	s.capMu.Unlock()
}

// parseSegment reads one segment file through the injected parser.
func (s *Service) parseSegment(path string) (*SegmentInfo, error) {
	s.capMu.RLock()
	parser := s.segParser
	s.capMu.RUnlock()
	if parser == nil {
		return nil, errors.New("cascade: no segment parser configured")
	}
	return parser(path)
}

func (s *Service) segmentParserConfigured() bool {
	s.capMu.RLock()
	defer s.capMu.RUnlock()
	return s.segParser != nil
}

// SetGBTimezone pins the zone used for GB/T 28181 naive timestamps (RecordInfo
// query parsing / response formatting). Deployments whose host clock zone
// differs from the devices' (e.g. a UTC container cascading into CST cameras)
// set this to the devices' zone.
func (s *Service) SetGBTimezone(loc *time.Location) {
	if loc != nil {
		s.tzMu.Lock()
		s.gbLoc = loc
		s.tzMu.Unlock()
	}
}

// gbTZ returns the effective GB naive-clock zone.
func (s *Service) gbTZ() *time.Location {
	s.tzMu.RLock()
	defer s.tzMu.RUnlock()
	if s.gbLoc != nil {
		return s.gbLoc
	}
	return time.Local
}

func (s *Service) cameraStatus(cameraID string) string {
	src, ok := s.src.(CameraStatusSource)
	if !ok {
		return "ON"
	}
	if strings.EqualFold(strings.TrimSpace(src.CameraStatus(cameraID)), "ON") {
		return "ON"
	}
	return "OFF"
}

func (s *Service) cameraAvailable(cameraID string) bool {
	cam, ok := s.cameraInfo(cameraID)
	if !ok || cam.CascadeHidden || s.cameraStatus(cameraID) != "ON" {
		return false
	}
	return s.mainAcq != nil || s.src.Hub(cameraID) != nil
}

func (s *Service) acquireMainHub(cameraID string) (*platform.FrameHub, func(), error) {
	if s.mainAcq != nil {
		ctx := context.Background()
		if s.ctx != nil {
			ctx = s.ctx
		}
		hub, release, err := s.mainAcq.AcquireMainHub(ctx, cameraID)
		if err != nil {
			return nil, nil, err
		}
		if hub == nil {
			return nil, nil, errors.New("main stream unavailable")
		}
		if release == nil {
			release = func() {}
		}
		return hub, release, nil
	}
	hub := s.src.Hub(cameraID)
	if hub == nil {
		return nil, nil, errors.New("main stream unavailable")
	}
	return hub, func() {}, nil
}
func (s *Service) Name() string { return "gb28181-cascade" }

func (s *Service) Start(ctx context.Context) error {
	if err := s.validateProtocolProfiles(); err != nil {
		return fmt.Errorf("gb28181-cascade: %w", err)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	s.ctx, s.cancel = context.WithCancel(ctx)
	s.storeMu.Lock()
	s.storeCtx, s.storeCancel = context.WithCancel(s.ctx)
	s.storeMu.Unlock()

	listen := s.cfg.SIPListen
	if listen == "" {
		listen = ":5061"
	}
	srv, err := newSIPServer(listen, s.cfg.EffectiveUserAgent())
	if err != nil {
		return fmt.Errorf("gb28181-cascade: SIP listen %s: %w", listen, err)
	}
	s.srv = srv
	_ = srv.OnRequest(sip.MESSAGE, s.onMessage)
	_ = srv.OnRequest(sip.INVITE, s.onInvite)
	_ = srv.OnRequest(sip.BYE, s.onBye)
	// ACK completes the upper platform's INVITE dialog. gosip logs "SIP
	// request handler not found" for unregistered methods; a no-op handler
	// keeps the transaction layer quiet (dialog state lives in the media
	// sessions map, keyed by Call-ID).
	_ = srv.OnRequest(sip.ACK, func(_ sip.Request, _ sip.ServerTransaction) {})
	// The upper platform may SUBSCRIBE to catalog changes — catalog NOTIFYs
	// push channel additions/removals without waiting for the upper's
	// polling fallback (#370).
	_ = srv.OnRequest(sip.SUBSCRIBE, s.onSubscribe)
	_ = srv.OnRequest(sip.OPTIONS, s.onOptions)
	// MANSRTSP playback controls (pause/resume/seek/scale) ride in-dialog INFO
	// messages; onInfo routes them to the channel's playback session.
	_ = srv.OnRequest(sip.INFO, s.onInfo)

	if len(s.uppers) == 0 {
		s.stopping.Store(true)
		if s.cancel != nil {
			s.cancel()
		}
		srv.Shutdown()
		return fmt.Errorf("gb28181-cascade: no upper platform configured (server_addr / upstreams)")
	}
	for _, u := range s.uppers {
		s.wg.Add(1)
		go s.registerLoop(u)
	}
	s.wg.Add(1)
	go s.catalogNotifyLoop() //nolint:contextcheck // the loop reads Service.ctx directly (struct field)
	go func() {
		<-s.ctx.Done()
		_ = s.Stop()
	}()
	slog.Info("gb28181-cascade: started",
		"listen", listen, "uppers", len(s.uppers), "device", s.cfg.LocalDeviceID)
	return nil
}

func (s *Service) Stop() error {
	s.stopOnce.Do(func() { s.stopErr = s.stop() })
	return s.stopErr
}

func (s *Service) stop() error {
	defer close(s.stopDone)
	s.stopping.Store(true)
	s.cancelStoreContext()
	if s.cancel != nil {
		s.cancel()
	}
	s.stopAllPTZ()
	// Stop must not race an INVITE that is acquiring a stream or creating a
	// dialog. New INVITEs reject before entering this critical section.
	s.admissionMu.Lock()
	s.admissionMu.Unlock()
	s.wg.Wait()

	// Best-effort unregister (Expires 0) and BYE of active forwards/playbacks.
	s.mu.Lock()
	sessions := make([]*mediaSession, 0, len(s.sessions))
	for _, ms := range s.sessions {
		sessions = append(sessions, ms)
	}
	s.sessions = make(map[string]*mediaSession)
	playbacks := make([]*playbackSession, 0, len(s.playbacks))
	for _, ps := range s.playbacks {
		playbacks = append(playbacks, ps)
	}
	subs := make([]*catalogSub, 0, len(s.subs))
	for _, sub := range s.subs {
		subs = append(subs, sub)
	}
	s.playbacks = make(map[string]*playbackSession)
	s.subs = make(map[string]*catalogSub)
	for _, u := range s.uppers {
		u.online = false
		u.regTS = time.Time{}
	}
	s.mu.Unlock()
	for _, ms := range sessions {
		ms.stop()
	}
	for _, ps := range playbacks {
		ps.stop()
	}
	for _, sub := range subs {
		if sub.cancel != nil {
			sub.cancel()
		}
		sub.sendMu.Lock()
		sub.sendMu.Unlock()
	}
	if s.srv != nil {
		for _, u := range s.uppers {
			_ = s.sendRegister(u, 0)
		}
		s.srv.Shutdown()
	}
	slog.Info("gb28181-cascade: stopped")
	return nil
}

// ---- registration & keepalive ----

func (s *Service) registerLoop(u *upper) {
	defer s.wg.Done()
	defer s.setOnline(u, false)
	expires := u.cfg.RegisterExpires
	if expires <= 0 {
		expires = 3600
	}
	retry := backoff.New(s.retryBase, s.retryMax)
	for {
		if s.ctx.Err() != nil {
			return
		}
		generation := u.generation.Load()
		s.observe(metrics.GatewayEvent{
			Name: "register_attempt", ProtocolVersion: s.cfg.EffectiveProtocolVersion(),
			PeerGBVersion: s.upperVersion(u), Transport: "udp",
		}, func(h metrics.Hooks) { h.RegisterAttempt() })
		if err := s.sendRegister(u, expires); err != nil {
			wait := s.jitterRetry(retry.Next())
			s.observe(metrics.GatewayEvent{
				Name: "register_failure", ProtocolVersion: s.cfg.EffectiveProtocolVersion(),
				PeerGBVersion: s.upperVersion(u), Transport: "udp", ErrorCode: safeErrorCode(err),
			}, func(h metrics.Hooks) { h.RegisterFail() })
			slog.Warn("gb28181-cascade: register failed, retrying",
				"upper", u.cfg.ServerAddr, "retry_in", wait.String(),
				"error_code", safeErrorCode(err), "diagnostic", safeDiagnostic(err))
			s.setOnline(u, false)
			if !s.wait(s.ctx, wait, u.wake) {
				return
			}
			continue
		}
		if u.generation.Load() != generation {
			drainWake(u.wake)
			continue
		}
		if !s.setOnlineForGeneration(u, true, generation) {
			continue
		}
		s.observe(metrics.GatewayEvent{
			Name: "register_ok", ProtocolVersion: s.cfg.EffectiveProtocolVersion(),
			PeerGBVersion: s.upperVersion(u), Transport: "udp",
		}, func(h metrics.Hooks) { h.RegisterOK() })
		retry.Reset()
		// Keepalive cadence while registered.
		hb := 60 * time.Second
		if d, err := time.ParseDuration(u.cfg.HeartbeatInterval); err == nil && d > 0 {
			hb = d
		}
		reRegister := time.Duration(expires)*8/10*time.Second - hb
		for i := time.Duration(0); i < reRegister; i += hb {
			if !s.wait(s.ctx, hb, u.wake) {
				return
			}
			if u.generation.Load() != generation {
				break
			}
			s.observe(metrics.GatewayEvent{
				Name: "heartbeat_attempt", ProtocolVersion: s.cfg.EffectiveProtocolVersion(),
				PeerGBVersion: s.upperVersion(u), Transport: "udp",
			}, nil)
			if err := s.sendKeepalive(u); err != nil {
				// A keepalive failure usually means the upper platform
				// restarted (403 Device not registered) or vanished —
				// re-REGISTER immediately instead of waiting out the
				// Expires window.
				s.observe(metrics.GatewayEvent{
					Name: "heartbeat_failure", ProtocolVersion: s.cfg.EffectiveProtocolVersion(),
					PeerGBVersion: s.upperVersion(u), Transport: "udp", ErrorCode: safeErrorCode(err),
				}, func(h metrics.Hooks) { h.KeepaliveFail() })
				slog.Warn("gb28181-cascade: keepalive failed — re-registering",
					"upper", u.cfg.ServerAddr, "error_code", safeErrorCode(err),
					"diagnostic", safeDiagnostic(err))
				s.setOnline(u, false)
				break
			}
			s.observe(metrics.GatewayEvent{
				Name: "heartbeat_ok", ProtocolVersion: s.cfg.EffectiveProtocolVersion(),
				PeerGBVersion: s.upperVersion(u), Transport: "udp",
			}, nil)
		}
	}
}

func (s *Service) setOnline(u *upper, v bool) {
	if u == nil {
		return
	}
	s.setOnlineForGeneration(u, v, u.generation.Load())
}

func (s *Service) setOnlineForGeneration(u *upper, v bool, generation uint64) bool {
	if u == nil {
		return false
	}
	s.mu.Lock()
	if u.generation.Load() != generation {
		s.mu.Unlock()
		return false
	}
	changed := u.online != v
	u.online = v
	if v {
		u.regTS = s.now()
		u.stale.Store(false)
	} else if changed {
		u.regTS = time.Time{}
	}
	s.mu.Unlock()
	if !v {
		s.closeUpperDialogs(u)
	}
	if changed {
		state := "offline"
		if v {
			state = "online"
		}
		slog.Info("gb28181-cascade: registration state",
			"upper", u.cfg.ServerAddr, "state", state)
	}
	return true
}

// Online reports the registration state (diagnostics).
func (s *Service) Online() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, u := range s.uppers {
		if u.online {
			return true
		}
	}
	return false
}

// Status reports the cascade registration/media admission state.
func (s *Service) Status() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, u := range s.uppers {
		if s.upperVersionMismatchLocked(u) {
			return StatusVersionMismatch
		}
	}
	for _, u := range s.uppers {
		if u.online {
			return StatusOnline
		}
	}
	return StatusOffline
}

// UpperProtocolVersion returns the registered version marker for the first
// configured upper platform. Empty means no successful versioned REGISTER
// response has been received (or the response omitted X-GB-Ver).
func (s *Service) UpperProtocolVersion() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.uppers) == 0 {
		return ""
	}
	return s.uppers[0].protocolVersion
}

// RegistrationSince returns how long the OLDEST live registration has been
// up (ok=false when every upper is offline).
func (s *Service) RegistrationSince() (time.Duration, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var oldest time.Time
	for _, u := range s.uppers {
		if u.online && (oldest.IsZero() || u.regTS.Before(oldest)) {
			oldest = u.regTS
		}
	}
	if oldest.IsZero() {
		return 0, false
	}
	return s.now().Sub(oldest), true
}

// NotifyNetworkChange invalidates every upper-platform dialog and wakes each
// registration loop. A changed local address/NAT mapping makes old SIP and
// media dialogs unusable even when the registration state was still online.
func (s *Service) NotifyNetworkChange() {
	s.cancelStoreContext()
	s.admissionMu.Lock()
	defer s.admissionMu.Unlock()
	s.resetStoreContext()
	for _, u := range s.uppers {
		s.mu.Lock()
		u.generation.Add(1)
		u.stale.Store(true)
		u.online = false
		u.regTS = time.Time{}
		s.mu.Unlock()
		s.closeUpperDialogsLocked(u)
		if u.wake != nil {
			select {
			case u.wake <- struct{}{}:
			default:
			}
		}
	}
}

func (s *Service) closeUpperDialogs(u *upper) {
	s.admissionMu.Lock()
	defer s.admissionMu.Unlock()
	s.closeUpperDialogsLocked(u)
}

func (s *Service) closeUpperDialogsLocked(u *upper) {
	s.mu.Lock()
	var sessions []*mediaSession
	for callID, ms := range s.sessions {
		if ms.upper == u {
			delete(s.sessions, callID)
			sessions = append(sessions, ms)
		}
	}
	var playbacks []*playbackSession
	for callID, ps := range s.playbacks {
		if ps.upper == u {
			delete(s.playbacks, callID)
			playbacks = append(playbacks, ps)
		}
	}
	var subs []*catalogSub
	for callID, sub := range s.subs {
		if sub.upper == u {
			delete(s.subs, callID)
			subs = append(subs, sub)
		}
	}
	s.mu.Unlock()

	for _, ms := range sessions {
		ms.stop()
	}
	for _, ps := range playbacks {
		ps.stop()
	}
	for _, sub := range subs {
		if sub.cancel != nil {
			sub.cancel()
		}
		sub.sendMu.Lock()
		sub.sendMu.Unlock()
	}
}

func (s *Service) jitterRetry(wait time.Duration) time.Duration {
	if wait <= 0 {
		return wait
	}
	jitter := wait / registerRetryJitterDivisor
	low := wait - jitter
	high := wait + jitter
	if high > s.retryMax {
		high = s.retryMax
	}
	if low < 0 {
		low = 0
	}
	span := high - low
	if span <= 0 {
		return wait
	}
	s.retryMu.Lock()
	n := s.retryRand(int64(span) + 1)
	s.retryMu.Unlock()
	if n < 0 {
		n = 0
	} else if n > int64(span) {
		n = int64(span)
	}
	return low + time.Duration(n)
}

func drainWake(wake <-chan struct{}) {
	if wake == nil {
		return
	}
	select {
	case <-wake:
	default:
	}
}

// ForwardCount returns the number of active media dialogs (live forwards +
// playback streams) currently sending to the upper platform.
func (s *Service) ForwardCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.sessions) + len(s.playbacks)
}

func upperAddr(u *upper) (*net.UDPAddr, error) {
	return net.ResolveUDPAddr("udp", u.cfg.ServerAddr)
}

// upperOf resolves a trusted incoming request to its configured upper. Both
// the From user and the packet source IP must match; source ports are ignored
// because UDP NAT may rewrite them.
func (s *Service) upperOf(req sip.Request) *upper {
	if req == nil {
		return nil
	}
	from, ok := req.From()
	if !ok || from.Address == nil {
		return nil
	}
	user := from.Address.User().String()
	for _, u := range s.uppers {
		if u.cfg.ServerDomain == user && sourceMatchesUpper(req.Source(), u) {
			return u
		}
	}
	return nil
}

func sourceMatchesUpper(source string, u *upper) bool {
	if u == nil || source == "" {
		return false
	}
	host, _, err := net.SplitHostPort(source)
	if err != nil {
		host = strings.Trim(source, "[]")
	}
	sourceIP := net.ParseIP(host)
	target, err := upperAddr(u)
	return sourceIP != nil && err == nil && sourceIP.Equal(target.IP)
}

// requireUpper rejects untrusted requests without logging headers or bodies.
// In particular, Authorization may contain a digest secret and must never be
// included in diagnostics.
func (s *Service) requireUpper(req sip.Request) *upper {
	if u := s.upperOf(req); u != nil {
		return u
	}
	if req != nil {
		fromUser := ""
		if from, ok := req.From(); ok && from.Address != nil {
			fromUser = from.Address.User().String()
		}
		slog.Warn("gb28181-cascade: unauthorized upper request",
			"method", req.Method(), "from", fromUser, "source", req.Source())
		if s.srv != nil {
			_, _ = s.srv.RespondOnRequest(req, 403, "Forbidden", "", nil)
		}
	}
	return nil
}

func requestCallID(req sip.Request) string {
	if req == nil {
		return ""
	}
	if h, ok := req.CallID(); ok {
		return h.String()
	}
	return ""
}

// requireDialogOwner enforces Call-ID ownership after source authentication
// and before any dialog-specific gate or Store lookup. The caller owns the
// admission/lifecycle lock when it needs ordering against BYE or replacement.
func (s *Service) requireDialogOwner(req sip.Request, u *upper) bool {
	callID := requestCallID(req)
	if callID == "" {
		return true
	}
	s.mu.Lock()
	foreign := false
	if ms := s.sessions[callID]; ms != nil && ms.upper != u {
		foreign = true
	}
	if ps := s.playbacks[callID]; ps != nil && ps.upper != u {
		foreign = true
	}
	if sub := s.subs[callID]; sub != nil && sub.upper != u {
		foreign = true
	}
	s.mu.Unlock()
	if foreign {
		_, _ = s.srv.RespondOnRequest(req, 403, "Forbidden", "", nil)
		return false
	}
	return true
}

func (s *Service) onOptions(req sip.Request, _ sip.ServerTransaction) {
	if s.requireUpper(req) == nil {
		return
	}
	allow := sip.AllowHeader{sip.REGISTER, sip.MESSAGE, sip.INVITE, sip.ACK, sip.BYE, sip.CANCEL, sip.OPTIONS}
	_, _ = s.srv.RespondOnRequest(req, 200, "OK", "", []sip.Header{&allow})
}

func (s *Service) upperForDeviceStatus(req sip.Request) *upper {
	return s.requireUpper(req)
}

// buildCoreRequest assembles a REGISTER/MESSAGE request toward the upper
// platform on the cascade's own SIP listening port.
func (s *Service) buildCoreRequest(u *upper, method sip.RequestMethod, localHost string, localPort int, body, contentType string) (sip.Request, error) {
	dst, err := upperAddr(u)
	if err != nil {
		return nil, err
	}
	port := sip.Port(localPort)
	rb := sip.NewRequestBuilder()
	rb.SetMethod(method)
	rb.SetFrom(&sip.Address{
		Uri:    &sip.SipUri{FUser: sip.String{Str: u.cfg.LocalDeviceID}, FHost: localHost, FPort: &port},
		Params: sip.NewParams().Add("tag", sip.String{Str: sip.GenerateBranch()}),
	})
	rb.SetTo(&sip.Address{
		Uri: &sip.SipUri{FUser: sip.String{Str: u.cfg.ServerDomain}, FHost: dst.IP.String()},
	})
	dstPort := sip.Port(dst.Port)
	rb.SetRecipient(&sip.SipUri{FUser: sip.String{Str: u.cfg.ServerDomain}, FHost: dst.IP.String(), FPort: &dstPort})
	rb.SetHost(localHost)
	rb.SetContact(&sip.Address{
		Uri: &sip.SipUri{FUser: sip.String{Str: u.cfg.LocalDeviceID}, FHost: localHost, FPort: &port},
	})
	rb.AddVia(&sip.ViaHop{
		Host: localHost,
		Port: &port,
		Params: sip.NewParams().
			Add("branch", sip.String{Str: sip.GenerateBranch()}).
			Add("rport", sip.String{}),
	})
	rb.SetSeqNo(1)
	if contentType != "" {
		ct := sip.ContentType(contentType)
		rb.SetContentType(&ct)
	}
	if body != "" {
		rb.SetBody(body)
	}
	return rb.Build()
}

func (s *Service) localHostPort(u *upper) (string, int) {
	listen := s.cfg.SIPListen
	if listen == "" {
		listen = ":5061"
	}
	if u == nil {
		return "127.0.0.1", 5061
	}
	if dst, err := upperAddr(u); err == nil {
		// Route via the interface that reaches the upper platform.
		if conn, err := (&net.Dialer{}).DialContext(s.ctx, "udp", dst.String()); err == nil {
			defer func() { _ = conn.Close() }()
			if local, ok := conn.LocalAddr().(*net.UDPAddr); ok {
				host := local.IP.String()
				if host == "::" || host == "" {
					host = "127.0.0.1"
				}
				_, portStr, _ := net.SplitHostPort(listen)
				p, _ := strconv.Atoi(portStr)
				if p == 0 {
					p = 5061
				}
				return host, p
			}
		}
	}
	return "127.0.0.1", 5061
}

// sendRegister performs the REGISTER + digest challenge round. expires=0
// unregisters.
func (s *Service) sendRegister(u *upper, expires int) error {
	if s.srv == nil {
		return fmt.Errorf("not started")
	}
	host, port := s.localHostPort(u)
	req, err := s.buildCoreRequest(u, sip.REGISTER, host, port, "", "")
	if err != nil {
		return err
	}
	exp := sip.Expires(uint32(expires))
	req.AppendHeader(&exp)
	s.appendProtocolVersion(req)

	resp, err := s.request(req)
	if err != nil {
		return err
	}
	if resp.StatusCode() == 401 {
		auth, err2 := s.digestFrom(u, resp, req)
		if err2 != nil {
			return err2
		}
		req2, err2 := s.buildCoreRequest(u, sip.REGISTER, host, port, "", "")
		if err2 != nil {
			return err2
		}
		exp2 := sip.Expires(uint32(expires))
		req2.AppendHeader(&exp2)
		s.appendProtocolVersion(req2)
		req2.AppendHeader(auth)
		resp, err = s.request(req2)
		if err != nil {
			return err
		}
	}
	if !resp.IsSuccess() {
		return fmt.Errorf("register: status %d (%s)", resp.StatusCode(), resp.Reason())
	}
	s.saveUpperProtocolVersion(u, responseProtocolVersion(resp))
	return nil
}

func (s *Service) appendProtocolVersion(req sip.Request) {
	req.AppendHeader(&sip.GenericHeader{
		HeaderName: "X-GB-Ver",
		Contents:   profileVersionMarker(s.cfg.EffectiveProtocolVersion()),
	})
}

func responseProtocolVersion(resp sip.Response) string {
	if headers := resp.GetHeaders("X-GB-Ver"); len(headers) > 0 {
		return strings.TrimSpace(headers[0].Value())
	}
	return ""
}

func (s *Service) saveUpperProtocolVersion(u *upper, version string) {
	s.mu.Lock()
	u.protocolVersion = strings.TrimSpace(version)
	u.protocolVersionSeen = true
	s.mu.Unlock()
}

func (s *Service) upperVersionMismatchLocked(u *upper) bool {
	return u.protocolVersionSeen && s.requiresVersionGate() && u.protocolVersion != profileVersionMarker("2022")
}

var challengeRe = regexp.MustCompile(`(\w+)\s*=\s*"([^"]+)"`)

// digestFrom computes the Authorization header for a 401 challenge.
func (s *Service) digestFrom(u *upper, resp sip.Response, origReq sip.Request) (sip.Header, error) {
	var hdrVal string
	for _, h := range resp.GetHeaders("WWW-Authenticate") {
		if g, ok := h.(*sip.GenericHeader); ok {
			hdrVal = g.Contents
			break
		}
	}
	if hdrVal == "" {
		return nil, fmt.Errorf("401 without WWW-Authenticate")
	}
	vals := map[string]string{}
	for _, m := range challengeRe.FindAllStringSubmatch(hdrVal, -1) {
		vals[m[1]] = m[2]
	}
	realm := vals["realm"]
	if realm == "" {
		realm = u.cfg.Realm
	}
	nonce := vals["nonce"]
	if nonce == "" {
		return nil, fmt.Errorf("challenge without nonce")
	}

	uri := fmt.Sprintf("sip:%s@%s", u.cfg.ServerDomain, addrHost(u.cfg.ServerAddr))
	ha1 := md5hex(u.cfg.LocalDeviceID, realm, u.cfg.Password)
	ha2 := md5hex("REGISTER", uri)
	response := md5hex(ha1, nonce, ha2)

	value := fmt.Sprintf(`Digest realm="%s",algorithm=MD5,nonce="%s",username="%s",uri="%s",response="%s"`,
		realm, nonce, u.cfg.LocalDeviceID, uri, response)
	return &sip.GenericHeader{HeaderName: "Authorization", Contents: value}, nil
}

func md5hex(parts ...string) string {
	h := md5.New() //nolint:gosec // GB28181 digest mandates MD5
	_, _ = h.Write([]byte(strings.Join(parts, ":")))
	return hex.EncodeToString(h.Sum(nil))
}

func (s *Service) request(req sip.Request) (sip.Response, error) {
	tx, err := s.srv.Request(req)
	if err != nil {
		return nil, err
	}
	responses := tx.Responses()
	defer tx.Done()
	deadline := time.NewTimer(8 * time.Second)
	defer deadline.Stop()
	for {
		select {
		case resp, ok := <-responses:
			if !ok {
				return nil, fmt.Errorf("no response")
			}
			if resp.IsProvisional() {
				continue
			}
			return resp, nil
		case <-time.After(8 * time.Second):
			return nil, fmt.Errorf("timeout")
		case <-s.ctx.Done():
			return nil, s.ctx.Err()
		}
	}
}

func (s *Service) sendKeepalive(u *upper) error {
	body, err := manscdp.Encode(manscdp.Keepalive{
		CmdType:  manscdp.CmdKeepalive,
		SN:       int(s.sn.Add(1)),
		DeviceID: u.cfg.LocalDeviceID,
		Status:   "OK",
	})
	if err != nil {
		return err
	}
	host, port := s.localHostPort(u)
	req, err := s.buildCoreRequest(u, sip.MESSAGE, host, port, string(body), "Application/MANSCDP+xml")
	if err != nil {
		return err
	}
	resp, err := s.request(req)
	if err != nil {
		return err
	}
	if !resp.IsSuccess() {
		return fmt.Errorf("keepalive: status %d", resp.StatusCode())
	}
	return nil
}

// ---- upper-platform requests (UAS side) ----

func (s *Service) onMessage(req sip.Request, _ sip.ServerTransaction) {
	u := s.requireUpper(req)
	if u == nil {
		return
	}
	cmd, payload, err := manscdp.Decode([]byte(req.Body()))
	if err != nil {
		_, _ = s.srv.RespondOnRequest(req, 400, "Bad MANSCDP", "", nil)
		return
	}
	_, _ = s.srv.RespondOnRequest(req, 200, "OK", "", nil)
	switch cmd {
	case manscdp.CmdCatalog:
		// Queries (root <Query>) come from the upper platform; Response-root
		// Catalogs are other devices' answers and never reach the cascade.
		if q, ok := payload.(manscdp.CatalogQuery); ok && q.SN > 0 {
			s.answerCatalog(u, q.SN)
		}
	case manscdp.CmdDeviceInfo:
		if d, ok := payload.(manscdp.DeviceInfo); ok && d.SN > 0 {
			s.answerDeviceInfo(u, d.SN)
		}
	case manscdp.CmdDeviceStatus:
		if q, ok := payload.(manscdp.DeviceStatusQuery); ok && q.SN > 0 {
			s.answerDeviceStatus(u, q.SN, q.DeviceID)
		}
	case manscdp.CmdRecordInfo:
		// Root <Query> carries CmdType RecordInfo (decoded as
		// RecordInfoQuery); the Response-root form is a device answer that
		// never reaches the cascade.
		if q, ok := payload.(manscdp.RecordInfoQuery); ok && q.SN > 0 {
			s.admissionMu.Lock()
			if s.stopping.Load() || s.ctx == nil || s.ctx.Err() != nil {
				s.admissionMu.Unlock()
				return
			}
			ctx := s.storeContext()
			s.wg.Add(1)
			s.admissionMu.Unlock()
			go func() {
				defer s.wg.Done()
				s.answerRecordInfo(ctx, u, q)
			}()
		}
	case manscdp.CmdDeviceControl:
		if dc, ok := payload.(manscdp.DeviceControl); ok {
			go s.forwardDeviceControl(dc)
		}
	}
}

func (s *Service) answerCatalog(u *upper, sn int) {
	items, err := s.catalogItems()
	if err != nil {
		s.observe(metrics.GatewayEvent{Name: "catalog_failure", ErrorCode: safeErrorCode(err)}, nil)
		slog.Warn("gb28181-cascade: catalog build failed",
			"error_code", safeErrorCode(err), "diagnostic", safeDiagnostic(err))
		return
	}
	body, err := manscdp.Encode(manscdp.Catalog{
		CmdType:  manscdp.CmdCatalog,
		SN:       sn,
		DeviceID: u.cfg.LocalDeviceID,
		SumNum:   len(items),
		Item:     items,
	})
	if err != nil {
		s.observe(metrics.GatewayEvent{Name: "catalog_failure", ErrorCode: safeErrorCode(err)}, nil)
		return
	}
	if err := s.sendMessageBodyTo(u, body, "Application/MANSCDP+xml"); err != nil {
		s.observe(metrics.GatewayEvent{Name: "catalog_failure", ErrorCode: safeErrorCode(err)}, nil)
		slog.Warn("gb28181-cascade: catalog response failed", "channels", len(items),
			"error_code", safeErrorCode(err), "diagnostic", safeDiagnostic(err))
	} else {
		s.observe(metrics.GatewayEvent{Name: "catalog_success", Value: int64(len(items))}, nil)
		slog.Info("gb28181-cascade: catalog response sent", "channels", len(items))
	}
}

func (s *Service) answerDeviceInfo(u *upper, sn int) {
	body, err := manscdp.Encode(manscdp.DeviceInfo{
		CmdType:      manscdp.CmdDeviceInfo,
		SN:           sn,
		DeviceID:     u.cfg.LocalDeviceID,
		DeviceName:   orDefault(s.cfg.DeviceName, "GB28181 Platform"),
		Manufacturer: orDefault(s.cfg.Manufacturer, "Unknown"),
		Model:        orDefault(s.cfg.Model, "Unknown"),
	})
	if err == nil {
		if err := s.sendMessageBodyTo(u, body, "Application/MANSCDP+xml"); err != nil {
			slog.Warn("gb28181-cascade: deviceinfo response failed",
				"error_code", safeErrorCode(err), "diagnostic", safeDiagnostic(err))
		}
	}
}

func (s *Service) answerDeviceStatus(u *upper, sn int, deviceID string) {
	status := "OFF"
	if deviceID == u.cfg.LocalDeviceID {
		status = "ON"
	} else if cameraID, ok := s.cameraOfChannel(deviceID); ok {
		if cam, exists := s.cameraInfo(cameraID); exists && !cam.CascadeHidden {
			status = s.cameraStatus(cameraID)
		}
	}
	body, err := manscdp.Encode(manscdp.DeviceStatus{
		CmdType:  manscdp.CmdDeviceStatus,
		SN:       sn,
		DeviceID: deviceID,
		Status:   status,
		Time:     time.Now().In(s.gbTZ()).Format(gbTimeLayout),
	})
	if err == nil {
		if err := s.sendMessageBodyTo(u, body, "Application/MANSCDP+xml"); err != nil {
			slog.Warn("gb28181-cascade: device status response failed", "device", deviceID,
				"error_code", safeErrorCode(err), "diagnostic", safeDiagnostic(err))
		}
	}
}

func (s *Service) sendMessageBodyTo(u *upper, body []byte, contentType string) error {
	host, port := s.localHostPort(u)
	req, err := s.buildCoreRequest(u, sip.MESSAGE, host, port, string(body), contentType)
	if err != nil {
		return err
	}
	resp, err := s.request(req)
	if err != nil {
		return err
	}
	if !resp.IsSuccess() {
		return fmt.Errorf("message: status %d", resp.StatusCode())
	}
	return nil
}

// addrHost extracts the host part of host:port.
func addrHost(addr string) string {
	if h, _, err := net.SplitHostPort(addr); err == nil {
		return h
	}
	return strings.TrimSpace(addr)
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func waitCtx(ctx context.Context, d time.Duration, wake <-chan struct{}) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-wake:
		return true
	case <-t.C:
		return true
	}
}

// newSIPServer builds a gosip UDP server bound to listen (":5061") — the
// same construction the platform-role server uses.
func newSIPServer(listen, userAgent string) (gosip.Server, error) {
	host, portStr, err := net.SplitHostPort(listen)
	if err != nil || portStr == "" {
		return nil, fmt.Errorf("invalid listen %q", listen)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, err
	}
	if host == "" {
		host = "0.0.0.0"
	}
	srv := gosip.NewServer(gosip.ServerConfig{
		Host:      host,
		UserAgent: userAgent,
	}, nil, nil, mbsip.SlogLogger(slog.Default().With("component", "gb28181_cascade")))
	if err := srv.Listen("UDP", net.JoinHostPort(host, strconv.Itoa(port))); err != nil {
		return nil, err
	}
	return srv, nil
}

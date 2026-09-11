package main

import (
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/mickeyzzc/gb28181-go/edgeipc"
	"github.com/mickeyzzc/gb28181-go/platform"
	"github.com/mickeyzzc/gb28181-go/platform/cascade"
)

const HealthTimeout = 15 * time.Second

var (
	ErrUnknownCamera   = errors.New("gateway: unknown camera")
	ErrInvalidEpoch    = errors.New("gateway: stream epoch must be non-zero")
	ErrInvalidCodec    = errors.New("gateway: invalid codec")
	ErrInvalidCameraID = errors.New("gateway: camera id is empty")
)

// CameraSpec describes the configured, stable part of one local camera.
type CameraSpec struct {
	ID            string
	Name          string
	Brand         string
	Model         string
	Codec         edgeipc.Codec
	PTZMode       string
	SubStream     bool
	CascadeHidden bool
}

// CameraSnapshot is a copy of the mutable registry state. Hub is the shared
// cascade seam; all other fields are values copied under the registry lock.
type CameraSnapshot struct {
	ID            string
	Name          string
	Brand         string
	Model         string
	Codec         edgeipc.Codec
	PTZMode       string
	StreamEpoch   uint64
	Online        bool
	LastHealth    time.Time
	Hub           *platform.FrameHub
	SubStream     bool
	CascadeHidden bool
}

type cameraState struct {
	spec        CameraSpec
	codec       edgeipc.Codec
	streamEpoch uint64
	online      bool
	lastHealth  time.Time
	closed      bool
	hub         *platform.FrameHub
	retired     map[uint64]struct{}
}

// CameraRegistry is the single in-process owner of camera liveness, stream
// epochs and their FrameHubs.
type CameraRegistry struct {
	mu      sync.Mutex
	cameras map[string]*cameraState
}

// NewCameraRegistry creates a registry with known cameras initially OFF.
// A non-zero codec must be H.264 or H.265; zero selects the H.265 default.
func NewCameraRegistry(specs ...CameraSpec) (*CameraRegistry, error) {
	r := &CameraRegistry{cameras: make(map[string]*cameraState, len(specs))}
	for _, spec := range specs {
		if spec.ID == "" {
			return nil, ErrInvalidCameraID
		}
		if spec.Codec != 0 && spec.Codec != edgeipc.CodecH264 && spec.Codec != edgeipc.CodecH265 {
			return nil, ErrInvalidCodec
		}
		if spec.Codec == 0 {
			spec.Codec = edgeipc.DefaultCodec
		}
		if spec.Name == "" {
			spec.Name = spec.ID
		}
		hub := platform.NewFrameHub()
		hub.SetCameraID(spec.ID)
		r.cameras[spec.ID] = &cameraState{
			spec: spec, codec: spec.Codec, hub: hub, retired: make(map[uint64]struct{}),
		}
	}
	return r, nil
}

// HandleHello accepts a validated encoder hello and starts or replaces its
// stream epoch. An older or duplicate hello is harmless.
func (r *CameraRegistry) HandleHello(message edgeipc.ControlMessage) error {
	if err := edgeipc.ValidateControlMessage(message); err != nil {
		return err
	}
	if message.Type != edgeipc.MessageHello {
		return edgeipc.ErrUnknownMessageType
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	state := r.cameras[message.CameraID]
	if state == nil {
		return ErrUnknownCamera
	}
	if state.streamEpoch == message.StreamEpoch || hasRetiredEpoch(state, message.StreamEpoch) {
		return nil
	}
	codec, err := helloCodec(state.spec.Codec, message.Codecs)
	if err != nil {
		return err
	}
	if state.streamEpoch != 0 {
		state.retired[state.streamEpoch] = struct{}{}
		state.hub.Close()
	}
	state.codec = codec
	state.streamEpoch = message.StreamEpoch
	state.online = false
	state.lastHealth = time.Time{}
	state.closed = false
	state.hub = platform.NewFrameHub()
	state.hub.SetCameraID(message.CameraID)
	return nil
}

// HandleHealth records health for the matching live epoch. Health messages
// carry no required identity on the wire, so the control connection supplies
// cameraID and streamEpoch explicitly.
func (r *CameraRegistry) HandleHealth(cameraID string, streamEpoch uint64, message edgeipc.ControlMessage) error {
	if err := edgeipc.ValidateControlMessage(message); err != nil {
		return err
	}
	if message.Type != edgeipc.MessageHealth {
		return edgeipc.ErrUnknownMessageType
	}
	if streamEpoch == 0 {
		return ErrInvalidEpoch
	}
	if message.CameraID != "" && message.CameraID != cameraID {
		return ErrUnknownCamera
	}
	if message.StreamEpoch != 0 && message.StreamEpoch != streamEpoch {
		return nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	state := r.cameras[cameraID]
	if state == nil {
		return ErrUnknownCamera
	}
	if state.streamEpoch != streamEpoch || state.closed {
		return nil
	}
	state.online = true
	state.lastHealth = time.Now()
	return nil
}

// HandleDisconnect marks only the matching epoch offline. A later epoch is
// never affected by a delayed disconnect from an older connection.
func (r *CameraRegistry) HandleDisconnect(cameraID string, streamEpoch uint64) bool {
	if streamEpoch == 0 {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	state := r.cameras[cameraID]
	if state == nil || state.streamEpoch != streamEpoch {
		return false
	}
	retired := state.online && !state.closed
	closeStateLocked(state)
	return retired
}

// Publish sends a frame only when it belongs to the current healthy epoch.
// This is the media-side epoch guard; stale control and media events share the
// same state owner.
func (r *CameraRegistry) Publish(cameraID string, streamEpoch uint64, pts int64, au [][]byte, isIDR bool) bool {
	if streamEpoch == 0 {
		return false
	}
	r.mu.Lock()
	state := r.cameras[cameraID]
	now := time.Now()
	if state == nil || state.streamEpoch != streamEpoch || !state.online || state.closed || stale(state.lastHealth, now) {
		if state != nil && state.streamEpoch == streamEpoch && stale(state.lastHealth, now) {
			closeStateLocked(state)
		}
		r.mu.Unlock()
		return false
	}
	hub := state.hub
	hub.Broadcast(pts, au, isIDR)
	r.mu.Unlock()
	return true
}

// Expire marks cameras without health for at least HealthTimeout OFF. Hosts
// may call this from their own event loop; reads also perform this check.
func (r *CameraRegistry) Expire(now time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.expireLocked(now)
}

// Camera returns one read-only state snapshot.
func (r *CameraRegistry) Camera(cameraID string) (CameraSnapshot, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.expireLocked(time.Now())
	state, ok := r.cameras[cameraID]
	if !ok {
		return CameraSnapshot{}, false
	}
	return snapshotOf(state), true
}

// Snapshot returns all cameras in stable ID order.
func (r *CameraRegistry) Snapshot() []CameraSnapshot {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.expireLocked(time.Now())
	ids := make([]string, 0, len(r.cameras))
	for id := range r.cameras {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	views := make([]CameraSnapshot, 0, len(ids))
	for _, id := range ids {
		views = append(views, snapshotOf(r.cameras[id]))
	}
	return views
}

// Cameras implements cascade.CameraSource.
func (r *CameraRegistry) Cameras() []cascade.CameraInfo {
	views := r.Snapshot()
	cameras := make([]cascade.CameraInfo, 0, len(views))
	for _, view := range views {
		cameras = append(cameras, cascade.CameraInfo{
			ID: view.ID, Name: view.Name, Brand: view.Brand, Model: view.Model,
			Encoding: codecName(view.Codec), PTZMode: view.PTZMode, SubStream: view.SubStream,
			CascadeHidden: view.CascadeHidden,
		})
	}
	return cameras
}

// Hub implements cascade.CameraSource.
func (r *CameraRegistry) Hub(cameraID string) *platform.FrameHub {
	view, ok := r.Camera(cameraID)
	if !ok {
		return nil
	}
	return view.Hub
}

// CameraStatus implements cascade.CameraStatusSource.
func (r *CameraRegistry) CameraStatus(cameraID string) string {
	view, ok := r.Camera(cameraID)
	if !ok || !view.Online {
		return "OFF"
	}
	return "ON"
}

func (r *CameraRegistry) expireLocked(now time.Time) {
	for _, state := range r.cameras {
		if state.online && stale(state.lastHealth, now) {
			closeStateLocked(state)
		}
	}
}

func closeStateLocked(state *cameraState) {
	state.online = false
	state.closed = true
	state.hub.Close()
}

func snapshotOf(state *cameraState) CameraSnapshot {
	return CameraSnapshot{
		ID: state.spec.ID, Name: state.spec.Name, Brand: state.spec.Brand,
		Model: state.spec.Model, Codec: state.codec, PTZMode: state.spec.PTZMode, StreamEpoch: state.streamEpoch,
		Online: state.online, LastHealth: state.lastHealth, Hub: state.hub,
		SubStream: state.spec.SubStream, CascadeHidden: state.spec.CascadeHidden,
	}
}

func stale(lastHealth, now time.Time) bool {
	return lastHealth.IsZero() || !now.Before(lastHealth.Add(HealthTimeout))
}

func helloCodec(configured edgeipc.Codec, offered []edgeipc.Codec) (edgeipc.Codec, error) {
	if configured == 0 {
		configured = edgeipc.DefaultCodec
	}
	for _, codec := range offered {
		if codec == configured {
			return codec, nil
		}
	}
	return 0, edgeipc.ErrCodecMismatch
}

func hasRetiredEpoch(state *cameraState, epoch uint64) bool {
	_, ok := state.retired[epoch]
	return ok
}

func codecName(codec edgeipc.Codec) string {
	switch codec {
	case edgeipc.CodecH264:
		return "h264"
	case edgeipc.CodecH265:
		return "h265"
	default:
		return ""
	}
}

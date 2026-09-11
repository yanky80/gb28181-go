package cascade

import (
	"log/slog"
	"strings"
	"time"
)

// PTZAdapter is the smallest local-camera control contract. The adapter is
// registered for one camera, so it does not need to carry camera identity.
// Stop must be safe when motion is already stopped and when called repeatedly.
type PTZAdapter interface {
	Move(direction string, speed byte) error
	Stop() error
}

// PTZState is the only state the inference side needs for the fixed-view ROI
// seam. ROI policy itself remains outside this package.
type PTZState string

const (
	PTZMoving  PTZState = "moving"
	PTZStopped PTZState = "stopped"
)

// PTZStateEvent describes one observable PTZ motion transition.
type PTZStateEvent struct {
	CameraID  string
	Direction string
	Speed     byte
	State     PTZState
}

// PTZStateSink observes motion transitions. It is optional and never owns
// PTZ lifecycle.
type PTZStateSink func(PTZStateEvent)

const ptzStop = "stop"

// PTZAuditEvent is emitted when a control request is rejected or an adapter
// fails. Error text is diagnostic only; callers decide how to persist it.
type PTZAuditEvent struct {
	Kind      string
	CameraID  string
	ChannelID string
	Command   string
	Reason    string
	Error     string
}

// PTZAuditSink receives structured PTZ rejection/failure events.
type PTZAuditSink func(PTZAuditEvent)

type ptzMotion struct {
	adapter    PTZAdapter
	timer      *time.Timer
	generation uint64
	moving     bool
}

type forwarderAdapter struct {
	cameraID string
	forward  PTZForwarder
}

func (a forwarderAdapter) Move(direction string, speed byte) error {
	return a.forward(a.cameraID, direction, speed)
}

func (a forwarderAdapter) Stop() error {
	return a.forward(a.cameraID, ptzStop, 0)
}

// SetPTZAdapter wires the adapter for one stable local camera. Replacing or
// removing an adapter first stops any active motion on the old one.
func (s *Service) SetPTZAdapter(cameraID string, adapter PTZAdapter) {
	s.stopPTZ(cameraID, 0, nil)
	s.ptzMu.Lock()
	// Keep a nil entry so an explicit disconnect cannot fall back to the
	// legacy process-wide forwarder.
	s.ptzAdapters[cameraID] = adapter
	s.ptzMu.Unlock()
}

// SetPTZLeaseTimeout changes the no-command safety deadline. Non-positive
// values restore the five-second default.
func (s *Service) SetPTZLeaseTimeout(timeout time.Duration) {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	s.ptzMu.Lock()
	s.ptzLease = timeout
	s.ptzMu.Unlock()
}

func (s *Service) SetPTZStateSink(sink PTZStateSink) {
	s.mu.Lock()
	s.ptzState = sink
	s.mu.Unlock()
}

func (s *Service) SetPTZAuditSink(sink PTZAuditSink) {
	s.mu.Lock()
	s.ptzAudit = sink
	s.mu.Unlock()
}

func (s *Service) ptzAdapter(cameraID string) PTZAdapter {
	s.ptzMu.Lock()
	adapter, configured := s.ptzAdapters[cameraID]
	s.ptzMu.Unlock()
	if configured {
		return adapter
	}
	s.mu.Lock()
	forward := s.ptzForward
	s.mu.Unlock()
	if forward == nil {
		return nil
	}
	return &forwarderAdapter{cameraID: cameraID, forward: forward}
}

func (s *Service) ptzCapable(cameraID string) bool {
	cam, ok := s.cameraInfo(cameraID)
	if !ok || strings.EqualFold(strings.TrimSpace(cam.PTZMode), "none") {
		return false
	}
	return s.ptzAdapter(cameraID) != nil
}

func (s *Service) movePTZ(cameraID, channelID, direction string, speed byte) {
	if s.stopping.Load() {
		s.auditPTZ(cameraID, channelID, direction, "gateway_stopping", nil)
		return
	}
	adapter := s.ptzAdapter(cameraID)
	if adapter == nil || !s.ptzCapable(cameraID) {
		s.auditPTZ(cameraID, channelID, direction, "adapter_unavailable", nil)
		return
	}

	s.ptzMu.Lock()
	if s.stopping.Load() {
		s.ptzMu.Unlock()
		s.auditPTZ(cameraID, channelID, direction, "gateway_stopping", nil)
		return
	}
	motion := s.ptzMotion[cameraID]
	if motion == nil {
		motion = &ptzMotion{}
		s.ptzMotion[cameraID] = motion
	}
	if motion.timer != nil {
		motion.timer.Stop()
		motion.timer = nil
	}
	motion.adapter = adapter
	motion.generation++
	generation := motion.generation
	if err := adapter.Move(direction, speed); err != nil {
		motion.moving = false
		stopErr := adapter.Stop()
		s.ptzMu.Unlock()
		s.auditPTZ(cameraID, channelID, direction, "adapter_error", err)
		if stopErr != nil {
			s.auditPTZ(cameraID, channelID, ptzStop, "stop_error", stopErr)
		}
		return
	}
	motion.moving = true
	lease := s.ptzLease
	motion.timer = time.AfterFunc(lease, func() { s.stopPTZ(cameraID, generation, nil) })
	s.emitPTZState(PTZStateEvent{CameraID: cameraID, Direction: direction, Speed: speed, State: PTZMoving})
	s.ptzMu.Unlock()
}

func (s *Service) stopPTZ(cameraID string, generation uint64, explicit PTZAdapter) {
	s.ptzMu.Lock()
	motion := s.ptzMotion[cameraID]
	if motion == nil || !motion.moving {
		if explicit == nil {
			s.ptzMu.Unlock()
			return
		}
		err := explicit.Stop()
		s.ptzMu.Unlock()
		if err != nil {
			s.auditPTZ(cameraID, "", ptzStop, "stop_error", err)
		}
		return
	}
	if generation != 0 && motion.generation != generation {
		s.ptzMu.Unlock()
		return
	}
	if motion.timer != nil {
		motion.timer.Stop()
		motion.timer = nil
	}
	adapter := motion.adapter
	err := adapter.Stop()
	if err != nil {
		s.ptzMu.Unlock()
		s.auditPTZ(cameraID, "", ptzStop, "stop_error", err)
		return
	}
	motion.moving = false
	motion.generation++
	s.emitPTZState(PTZStateEvent{CameraID: cameraID, Direction: ptzStop, State: PTZStopped})
	s.ptzMu.Unlock()
}

func (s *Service) stopAllPTZ() {
	s.ptzMu.Lock()
	cameras := make([]string, 0, len(s.ptzMotion))
	for cameraID, motion := range s.ptzMotion {
		if motion.moving {
			cameras = append(cameras, cameraID)
		}
	}
	s.ptzMu.Unlock()
	for _, cameraID := range cameras {
		s.stopPTZ(cameraID, 0, nil)
	}
}

func (s *Service) emitPTZState(event PTZStateEvent) {
	s.mu.Lock()
	sink := s.ptzState
	s.mu.Unlock()
	if sink != nil {
		sink(event)
	}
}

func (s *Service) auditPTZ(cameraID, channelID, command, reason string, err error) {
	event := PTZAuditEvent{Kind: "ptz", CameraID: cameraID, ChannelID: channelID, Command: strings.ToLower(command), Reason: reason}
	if err != nil {
		event.Error = err.Error()
	}
	s.mu.Lock()
	sink := s.ptzAudit
	s.mu.Unlock()
	slogArgs := []any{"event", "ptz_rejected", "camera", cameraID, "channel", channelID, "command", event.Command, "reason", reason}
	if err != nil {
		slogArgs = append(slogArgs, "error_code", safeErrorCode(err), "diagnostic", safeDiagnostic(err))
	}
	slog.Warn("gb28181-cascade: PTZ control rejected", slogArgs...)
	if sink != nil {
		sink(event)
	}
}

func (s *Service) forwardPTZ(cameraID, channelID, direction string, speed byte) {
	if direction == ptzStop {
		if s.ptzAdapter(cameraID) == nil || !s.ptzCapable(cameraID) {
			s.auditPTZ(cameraID, channelID, direction, "adapter_unavailable", nil)
			return
		}
		s.stopPTZ(cameraID, 0, s.ptzAdapter(cameraID))
		return
	}
	s.movePTZ(cameraID, channelID, direction, speed)
}

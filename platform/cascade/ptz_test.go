package cascade

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/mickeyzzc/gb28181-go/manscdp"
	"github.com/stretchr/testify/require"
)

type recordingPTZAdapter struct {
	mu    sync.Mutex
	moves []struct {
		direction string
		speed     byte
	}
	stopErr error
	moveErr error
	stops   int
}

func (a *recordingPTZAdapter) Move(direction string, speed byte) error {
	a.mu.Lock()
	a.moves = append(a.moves, struct {
		direction string
		speed     byte
	}{direction, speed})
	err := a.moveErr
	a.mu.Unlock()
	return err
}

func (a *recordingPTZAdapter) Stop() error {
	a.mu.Lock()
	a.stops++
	err := a.stopErr
	a.mu.Unlock()
	return err
}

func (a *recordingPTZAdapter) snapshot() (int, int, string, byte) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.moves) == 0 {
		return 0, a.stops, "", 0
	}
	move := a.moves[len(a.moves)-1]
	return len(a.moves), a.stops, move.direction, move.speed
}

func TestPTZAdapterWiresStableChannelAndStateSeam(t *testing.T) {
	db := newCascadeTestDB(t)
	svc := New(testCfg(), fakeSource{cams: []CameraInfo{{ID: "front", Name: "Front", PTZMode: "onvif"}}}, db)
	_, err := svc.catalogItems()
	require.NoError(t, err)

	adapter := &recordingPTZAdapter{}
	var states []PTZStateEvent
	svc.SetPTZAdapter("front", adapter)
	svc.SetPTZStateSink(func(event PTZStateEvent) { states = append(states, event) })

	svc.forwardDeviceControl(manscdp.DeviceControl{
		CmdType: manscdp.CmdDeviceControl, DeviceID: "34020000001320000001",
		PTZCmd: "A50F0108002000DD",
	})
	moves, stops, direction, speed := adapter.snapshot()
	require.Equal(t, 1, moves)
	require.Zero(t, stops)
	require.Equal(t, "up", direction)
	require.Equal(t, byte(0x20), speed)

	svc.forwardDeviceControl(manscdp.DeviceControl{
		CmdType: manscdp.CmdDeviceControl, DeviceID: "34020000001320000001",
		PTZCmd: "A50F0100000000B5",
	})
	_, stops, _, _ = adapter.snapshot()
	require.Equal(t, 1, stops)
	require.Equal(t, []PTZState{PTZMoving, PTZStopped}, []PTZState{states[0].State, states[1].State})
}

func TestPTZCapabilityIsTruthful(t *testing.T) {
	newService := func(mode string) *Service {
		return New(testCfg(), fakeSource{cams: []CameraInfo{{ID: "front", PTZMode: mode}}}, newCascadeTestDB(t))
	}

	items, err := newService("onvif").catalogItems()
	require.NoError(t, err)
	require.Zero(t, items[0].PTZType, "configured mode without an adapter is not a capability")

	withAdapter := newService("onvif")
	withAdapter.SetPTZAdapter("front", &recordingPTZAdapter{})
	items, err = withAdapter.catalogItems()
	require.NoError(t, err)
	require.Equal(t, 3, items[0].PTZType)

	disabled := newService("none")
	disabled.SetPTZAdapter("front", &recordingPTZAdapter{})
	items, err = disabled.catalogItems()
	require.NoError(t, err)
	require.Zero(t, items[0].PTZType, "none must override an accidentally wired adapter")
}

func TestPTZStopIsIdempotentAndLeaseExpires(t *testing.T) {
	cfg := testCfg()
	cfg.PTZLeaseTimeout = 15 * time.Millisecond
	svc := New(cfg, fakeSource{cams: []CameraInfo{{ID: "front", PTZMode: "vendor"}}}, newCascadeTestDB(t))
	_, err := svc.catalogItems()
	require.NoError(t, err)
	adapter := &recordingPTZAdapter{}
	svc.SetPTZAdapter("front", adapter)

	svc.forwardDeviceControl(manscdp.DeviceControl{
		CmdType: manscdp.CmdDeviceControl, DeviceID: "34020000001320000001",
		PTZCmd: "A50F0108002000DD",
	})
	require.Eventually(t, func() bool {
		_, stops, _, _ := adapter.snapshot()
		return stops == 1
	}, time.Second, time.Millisecond)

	svc.forwardDeviceControl(manscdp.DeviceControl{
		CmdType: manscdp.CmdDeviceControl, DeviceID: "34020000001320000001",
		PTZCmd: "A50F0100000000B5",
	})
	_, stops, _, _ := adapter.snapshot()
	require.Equal(t, 1, stops, "an already-stopped command must be harmless")
}

func TestPTZFailureStopsSafelyAndAuditsRejections(t *testing.T) {
	db := newCascadeTestDB(t)
	svc := New(testCfg(), fakeSource{cams: []CameraInfo{{ID: "front", PTZMode: "onvif"}}}, db)
	_, err := svc.catalogItems()
	require.NoError(t, err)

	var audits []PTZAuditEvent
	svc.SetPTZAuditSink(func(event PTZAuditEvent) { audits = append(audits, event) })
	adapter := &recordingPTZAdapter{moveErr: errors.New("disconnected")}
	svc.SetPTZAdapter("front", adapter)
	svc.forwardDeviceControl(manscdp.DeviceControl{
		CmdType: manscdp.CmdDeviceControl, DeviceID: "34020000001320000001",
		PTZCmd: "A50F0108002000DD",
	})
	_, stops, _, _ := adapter.snapshot()
	require.Equal(t, 1, stops, "a failed move must fail safe")

	noAdapter := New(testCfg(), fakeSource{cams: []CameraInfo{{ID: "front", PTZMode: "onvif"}}}, db)
	svc = noAdapter
	svc.SetPTZAuditSink(func(event PTZAuditEvent) { audits = append(audits, event) })
	svc.forwardDeviceControl(manscdp.DeviceControl{
		CmdType: manscdp.CmdDeviceControl, DeviceID: "34020000001320000001",
		PTZCmd: "A50F0108002000DD",
	})
	svc.forwardDeviceControl(manscdp.DeviceControl{
		CmdType: manscdp.CmdDeviceControl, DeviceID: "34020000001320000001",
		TeleBoot: "Reboot",
	})
	require.GreaterOrEqual(t, len(audits), 3)
	require.Equal(t, "ptz", audits[0].Kind)
	require.Equal(t, "adapter_error", audits[0].Reason)
	require.Equal(t, "adapter_unavailable", audits[1].Reason)
	require.Equal(t, "teleboot", audits[2].Command)
}

func TestPTZAdapterDisconnectStopsCurrentMotion(t *testing.T) {
	svc := New(testCfg(), fakeSource{cams: []CameraInfo{{ID: "front", PTZMode: "local-gb28181"}}}, newCascadeTestDB(t))
	_, err := svc.catalogItems()
	require.NoError(t, err)
	adapter := &recordingPTZAdapter{}
	svc.SetPTZAdapter("front", adapter)
	svc.forwardDeviceControl(manscdp.DeviceControl{DeviceID: "34020000001320000001", PTZCmd: "A50F0108002000DD"})

	svc.SetPTZAdapter("front", nil)
	_, stops, _, _ := adapter.snapshot()
	require.Equal(t, 1, stops)

	_ = svc.Stop()
	_, stops, _, _ = adapter.snapshot()
	require.Equal(t, 1, stops, "service stop must not repeat an idempotent stop")
}

func TestPTZConcurrentCommandsSerializeAdapterAccess(t *testing.T) {
	svc := New(testCfg(), fakeSource{cams: []CameraInfo{{ID: "front", PTZMode: "vendor"}}}, newCascadeTestDB(t))
	_, err := svc.catalogItems()
	require.NoError(t, err)
	adapter := &recordingPTZAdapter{}
	svc.SetPTZAdapter("front", adapter)

	done := make(chan struct{})
	for i := 0; i < 20; i++ {
		go func() {
			svc.forwardDeviceControl(manscdp.DeviceControl{
				DeviceID: "34020000001320000001", PTZCmd: "A50F0108002000DD",
			})
			done <- struct{}{}
		}()
	}
	for i := 0; i < 20; i++ {
		<-done
	}
	svc.forwardDeviceControl(manscdp.DeviceControl{DeviceID: "34020000001320000001", PTZCmd: "A50F0100000000B5"})
	_, stops, _, _ := adapter.snapshot()
	require.Equal(t, 1, stops)
}

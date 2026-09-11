package main

import (
	"sync"
	"testing"
	"time"

	"github.com/mickeyzzc/gb28181-go/edgeipc"
	"github.com/mickeyzzc/gb28181-go/platform/cascade"
)

func TestCameraRegistryHelloHealthAndReadOnlyViews(t *testing.T) {
	r := NewCameraRegistry(CameraSpec{ID: "cam-a", Name: "Front", Codec: edgeipc.CodecH265})
	var _ cascade.CameraSource = r
	var _ cascade.CameraStatusSource = r

	view, ok := r.Camera("cam-a")
	if !ok || view.Online {
		t.Fatalf("initial view = %+v, %v; want known OFF camera", view, ok)
	}

	if err := r.HandleHello(hello("cam-a", 1, edgeipc.CodecH265)); err != nil {
		t.Fatal(err)
	}
	if r.CameraStatus("cam-a") != "OFF" {
		t.Fatal("hello must not mark a camera online before health")
	}
	if err := r.HandleHealth("cam-a", 1, health()); err != nil {
		t.Fatal(err)
	}

	view, ok = r.Camera("cam-a")
	if !ok || !view.Online || view.StreamEpoch != 1 || view.Codec != edgeipc.CodecH265 || view.Name != "Front" || view.LastHealth.IsZero() || view.Hub == nil {
		t.Fatalf("online view = %+v, %v", view, ok)
	}
	if r.CameraStatus("cam-a") != "ON" {
		t.Fatal("healthy camera must be ON")
	}
	if got := r.Cameras(); len(got) != 1 || got[0].ID != "cam-a" || got[0].Name != "Front" || got[0].Encoding != "h265" {
		t.Fatalf("cascade view = %+v", got)
	}
}

func TestCameraRegistryTimeoutDisconnectAndIsolation(t *testing.T) {
	r := NewCameraRegistry(
		CameraSpec{ID: "cam-a", Name: "Front"},
		CameraSpec{ID: "cam-b", Name: "Back"},
	)
	if err := r.HandleHello(hello("cam-a", 1, edgeipc.CodecH265)); err != nil {
		t.Fatal(err)
	}
	if err := r.HandleHealth("cam-a", 1, health()); err != nil {
		t.Fatal(err)
	}
	last := r.mustCamera("cam-a").LastHealth
	r.Expire(last.Add(HealthTimeout))
	if r.CameraStatus("cam-a") != "OFF" || r.CameraStatus("cam-b") != "OFF" {
		t.Fatalf("statuses after timeout = %q, %q", r.CameraStatus("cam-a"), r.CameraStatus("cam-b"))
	}

	if err := r.HandleHealth("cam-a", 1, health()); err != nil {
		t.Fatal(err)
	}
	r.HandleDisconnect("cam-a", 1)
	if r.CameraStatus("cam-a") != "OFF" {
		t.Fatal("disconnect must mark the matching epoch OFF")
	}
	if r.CameraStatus("cam-b") != "OFF" {
		t.Fatal("one camera's disconnect must not affect another")
	}
}

func TestCameraRegistryNewEpochRejectsStaleEventsAndMedia(t *testing.T) {
	r := NewCameraRegistry(CameraSpec{ID: "cam-a", Name: "Front"})
	if err := r.HandleHello(hello("cam-a", 1, edgeipc.CodecH265)); err != nil {
		t.Fatal(err)
	}
	if err := r.HandleHealth("cam-a", 1, health()); err != nil {
		t.Fatal(err)
	}
	oldHub := r.Hub("cam-a")
	oldFrames := make(chan struct{}, 1)
	if err := oldHub.Subscribe("old", func(int64, [][]byte, bool) { oldFrames <- struct{}{} }); err != nil {
		t.Fatal(err)
	}

	if err := r.HandleHello(hello("cam-a", 2, edgeipc.CodecH265)); err != nil {
		t.Fatal(err)
	}
	if r.CameraStatus("cam-a") != "OFF" || r.mustCamera("cam-a").Codec != edgeipc.CodecH265 || r.Hub("cam-a") == oldHub {
		t.Fatalf("new epoch did not replace old state: %+v", r.mustCamera("cam-a"))
	}
	if err := oldHub.Subscribe("after-close", func(int64, [][]byte, bool) {}); err == nil {
		t.Fatal("replaced hub must reject new subscribers")
	}
	if r.Publish("cam-a", 1, 10, [][]byte{{1}}, false) {
		t.Fatal("old epoch media must be rejected after replacement")
	}
	select {
	case <-oldFrames:
		t.Fatal("replaced hub received stale media")
	case <-time.After(20 * time.Millisecond):
	}

	if err := r.HandleHealth("cam-a", 1, health()); err != nil {
		t.Fatal(err)
	}
	r.HandleDisconnect("cam-a", 1)
	if r.CameraStatus("cam-a") != "OFF" {
		t.Fatal("stale epoch changed the replacement state")
	}
	if err := r.HandleHealth("cam-a", 2, health()); err != nil {
		t.Fatal(err)
	}
	if r.CameraStatus("cam-a") != "ON" {
		t.Fatal("current epoch health must turn the camera ON")
	}
	if err := r.HandleHealth("cam-a", 1, health()); err != nil {
		t.Fatal(err)
	}
	r.HandleDisconnect("cam-a", 1)
	if r.CameraStatus("cam-a") != "ON" {
		t.Fatal("stale events must not turn an active replacement OFF")
	}

	got := make(chan int64, 1)
	if err := r.Hub("cam-a").Subscribe("probe", func(pts int64, _ [][]byte, _ bool) { got <- pts }); err != nil {
		t.Fatal(err)
	}
	defer r.Hub("cam-a").Unsubscribe("probe")
	if r.Publish("cam-a", 1, 10, [][]byte{{1}}, false) {
		t.Fatal("stale media must be rejected")
	}
	if !r.Publish("cam-a", 2, 20, [][]byte{{1}}, true) {
		t.Fatal("current media must be accepted")
	}
	select {
	case pts := <-got:
		if pts != 20 {
			t.Fatalf("published pts = %d, want 20", pts)
		}
	case <-time.After(time.Second):
		t.Fatal("current media did not reach the current hub")
	}
}

func TestCameraRegistryDuplicateEventsAreIdempotent(t *testing.T) {
	r := NewCameraRegistry(CameraSpec{ID: "cam-a"})
	msg := hello("cam-a", 7, edgeipc.CodecH265)
	if err := r.HandleHello(msg); err != nil {
		t.Fatal(err)
	}
	if err := r.HandleHello(msg); err != nil {
		t.Fatal(err)
	}
	if err := r.HandleHealth("cam-a", 7, health()); err != nil {
		t.Fatal(err)
	}
	last := r.mustCamera("cam-a").LastHealth
	if err := r.HandleHealth("cam-a", 7, health()); err != nil {
		t.Fatal(err)
	}
	if r.CameraStatus("cam-a") != "ON" || !r.mustCamera("cam-a").LastHealth.After(last) {
		t.Fatal("duplicate health should refresh the active epoch")
	}
	r.HandleDisconnect("cam-a", 7)
	r.HandleDisconnect("cam-a", 7)
	if r.CameraStatus("cam-a") != "OFF" {
		t.Fatal("duplicate disconnect should remain harmless")
	}
}

func TestCameraRegistryRejectsUnknownAndPreservesDefaultCodec(t *testing.T) {
	r := NewCameraRegistry(CameraSpec{ID: "cam-a"})
	if r.CameraStatus("missing") != "OFF" {
		t.Fatal("unknown camera must be OFF")
	}
	if err := r.HandleHello(hello("missing", 1, edgeipc.CodecH265)); err != ErrUnknownCamera {
		t.Fatalf("unknown hello error = %v, want %v", err, ErrUnknownCamera)
	}
	if err := r.HandleHello(hello("cam-a", 1, edgeipc.CodecH264)); err != edgeipc.ErrCodecMismatch {
		t.Fatalf("default codec error = %v, want %v", err, edgeipc.ErrCodecMismatch)
	}
}

func TestCameraRegistryAcceptsNonMonotonicNewEpochAndRetiresOld(t *testing.T) {
	r := NewCameraRegistry(CameraSpec{ID: "cam-a", Codec: edgeipc.CodecH264})
	if err := r.HandleHello(hello("cam-a", 9, edgeipc.CodecH264)); err != nil {
		t.Fatal(err)
	}
	if err := r.HandleHello(hello("cam-a", 2, edgeipc.CodecH264)); err != nil {
		t.Fatal(err)
	}
	if got := r.mustCamera("cam-a").StreamEpoch; got != 2 {
		t.Fatalf("epoch = %d, want 2", got)
	}
	if err := r.HandleHello(hello("cam-a", 9, edgeipc.CodecH264)); err != nil {
		t.Fatal(err)
	}
	if got := r.mustCamera("cam-a").StreamEpoch; got != 2 {
		t.Fatalf("retired epoch revived as %d", got)
	}
}

func TestCameraRegistryConcurrentEvents(t *testing.T) {
	r := NewCameraRegistry(CameraSpec{ID: "cam-a"}, CameraSpec{ID: "cam-b"})
	var wg sync.WaitGroup
	for _, cameraID := range []string{"cam-a", "cam-b"} {
		cameraID := cameraID
		wg.Add(1)
		go func() {
			defer wg.Done()
			for epoch := uint64(1); epoch <= 100; epoch++ {
				_ = r.HandleHello(hello(cameraID, epoch, edgeipc.CodecH265))
				_ = r.HandleHealth(cameraID, epoch, health())
				r.Publish(cameraID, epoch, int64(epoch), [][]byte{{1}}, false)
				r.CameraStatus(cameraID)
				r.Snapshot()
			}
		}()
	}
	wg.Wait()
	if got := len(r.Snapshot()); got != 2 {
		t.Fatalf("camera count = %d, want 2", got)
	}
}

func hello(cameraID string, epoch uint64, codec edgeipc.Codec) edgeipc.ControlMessage {
	return edgeipc.ControlMessage{
		Type: edgeipc.MessageHello, Version: edgeipc.ProtocolVersion,
		CameraID: cameraID, PID: 1, Codecs: []edgeipc.Codec{codec}, StreamEpoch: epoch,
	}
}

func health() edgeipc.ControlMessage {
	return edgeipc.ControlMessage{
		Type: edgeipc.MessageHealth, Version: edgeipc.ProtocolVersion,
		RTSP: "rtsp://camera", Encode: "ok", InferFPS: 25,
	}
}

func (r *CameraRegistry) mustCamera(id string) CameraSnapshot {
	view, ok := r.Camera(id)
	if !ok {
		panic("camera not found: " + id)
	}
	return view
}

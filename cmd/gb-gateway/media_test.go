package main

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/mickeyzzc/gb28181-go/edgeipc"
)

func TestMediaHostWaitsForIDRThenPublishes(t *testing.T) {
	r := newRegistry(t, CameraSpec{ID: "cam-a"})
	if err := r.HandleHello(hello("cam-a", 1, edgeipc.CodecH265)); err != nil {
		t.Fatal(err)
	}
	if err := r.HandleHealth("cam-a", 1, health()); err != nil {
		t.Fatal(err)
	}

	var requests atomic.Int32
	s := NewMediaHost(r, MediaHostConfig{
		IDRTimeout: time.Second,
		RequestIDR: func(string) { requests.Add(1) },
	})
	frames := make(chan struct{}, 1)
	hub := r.Hub("cam-a")
	if err := hub.Subscribe("test", func(pts int64, au [][]byte, idr bool) {
		if pts != 9000 || len(au) != 4 || !idr {
			t.Errorf("frame = pts %d, %d NALs, idr %v", pts, len(au), idr)
		}
		frames <- struct{}{}
	}); err != nil {
		t.Fatal(err)
	}
	defer hub.Unsubscribe("test")

	serverConn, peer := net.Pipe()
	done := make(chan error, 1)
	go func() { done <- s.ServeConn(serverConn) }()
	if err := edgeipc.WriteMediaFrame(peer, edgeipc.MediaFrame{
		Codec: edgeipc.CodecH265, CameraID: "cam-a", PTS90kHz: 3000,
		Sequence: 1, Payload: h265PFrame(),
	}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for requests.Load() != 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("IDR requests after non-IDR = %d, want 1", got)
	}
	if err := edgeipc.WriteMediaFrame(peer, edgeipc.MediaFrame{
		Codec: edgeipc.CodecH265, Flags: edgeipc.FlagIDR, CameraID: "cam-a",
		PTS90kHz: 9000, Sequence: 2, Payload: h265IDR(),
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-frames:
	case <-time.After(time.Second):
		t.Fatal("valid IDR was not broadcast")
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("IDR requests after recovery = %d, want 1", got)
	}
	peer.Close()
	if err := <-done; err != nil && err != io.EOF {
		t.Fatalf("ServeConn error = %v", err)
	}
}

func TestMediaHostDiscontinuityRequestsOnceAndRecovers(t *testing.T) {
	r := onlineRegistry(t, edgeipc.CodecH265)
	var requests atomic.Int32
	s := NewMediaHost(r, MediaHostConfig{IDRTimeout: time.Second, RequestIDR: func(string) { requests.Add(1) }})
	frames := make(chan int64, 4)
	hub := r.Hub("cam-a")
	if err := hub.Subscribe("test", func(pts int64, _ [][]byte, _ bool) { frames <- pts }); err != nil {
		t.Fatal(err)
	}
	defer hub.Unsubscribe("test")

	serverConn, peer := net.Pipe()
	done := make(chan error, 1)
	go func() { done <- s.ServeConn(serverConn) }()
	writeMedia(t, peer, mediaFrame(edgeipc.CodecH265, 1, 9000, edgeipc.FlagIDR, h265IDR()))
	writeMedia(t, peer, mediaFrame(edgeipc.CodecH265, 2, 12000, 0, h265PFrame()))
	writeMedia(t, peer, mediaFrame(edgeipc.CodecH265, 4, 15000, 0, h265PFrame()))
	writeMedia(t, peer, mediaFrame(edgeipc.CodecH265, 5, 18000, 0, h265PFrame()))
	writeMedia(t, peer, mediaFrame(edgeipc.CodecH265, 6, 21000, edgeipc.FlagIDR, h265IDR()))

	if got := requests.Load(); got != 2 {
		t.Fatalf("IDR requests = %d, want initial request plus one after gap", got)
	}
	if got := readPTS(t, frames, 3); got[0] != 9000 || got[1] != 12000 || got[2] != 21000 {
		t.Fatalf("published PTS = %v, want [9000 12000 21000]", got)
	}
	peer.Close()
	if err := <-done; err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("ServeConn error = %v", err)
	}
}

func TestMediaHostPTSRegressionWaitsForIDR(t *testing.T) {
	r := onlineRegistry(t, edgeipc.CodecH265)
	var requests atomic.Int32
	s := NewMediaHost(r, MediaHostConfig{IDRTimeout: time.Second, RequestIDR: func(string) { requests.Add(1) }})
	frames := make(chan int64, 4)
	if err := r.Hub("cam-a").Subscribe("test", func(pts int64, _ [][]byte, _ bool) { frames <- pts }); err != nil {
		t.Fatal(err)
	}
	defer r.Hub("cam-a").Unsubscribe("test")
	serverConn, peer := net.Pipe()
	done := make(chan error, 1)
	go func() { done <- s.ServeConn(serverConn) }()
	writeMedia(t, peer, mediaFrame(edgeipc.CodecH265, 1, 9000, edgeipc.FlagIDR, h265IDR()))
	writeMedia(t, peer, mediaFrame(edgeipc.CodecH265, 2, 12000, 0, h265PFrame()))
	writeMedia(t, peer, mediaFrame(edgeipc.CodecH265, 3, 11000, 0, h265PFrame()))
	writeMedia(t, peer, mediaFrame(edgeipc.CodecH265, 4, 15000, edgeipc.FlagIDR, h265IDR()))
	if requests.Load() != 2 {
		t.Fatalf("IDR requests = %d, want initial request plus one after PTS regression", requests.Load())
	}
	if got := readPTS(t, frames, 3); got[0] != 9000 || got[1] != 12000 || got[2] != 15000 {
		t.Fatalf("published PTS = %v, want [9000 12000 15000]", got)
	}
	peer.Close()
	if err := <-done; err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("ServeConn error = %v", err)
	}
}

func TestMediaHostWaitIDRDoesNotAdvanceDroppedTimeline(t *testing.T) {
	r := onlineRegistry(t, edgeipc.CodecH265)
	s := NewMediaHost(r, MediaHostConfig{IDRTimeout: time.Second})
	c := &mediaConnection{
		server:       s,
		cameraID:     "cam-a",
		epoch:        1,
		codec:        edgeipc.CodecH265,
		bound:        true,
		haveSequence: true,
		lastSequence: 2,
		havePTS:      true,
		lastPTS:      12000,
		waiting:      true,
	}
	s.mu.Lock()
	s.connections["cam-a"] = c
	s.active[c] = struct{}{}
	s.mu.Unlock()

	if err := c.handle(mediaFrame(edgeipc.CodecH265, 99, 99000, 0, h265PFrame())); err != nil {
		t.Fatal(err)
	}
	if c.lastSequence != 2 || c.lastPTS != 12000 {
		t.Fatalf("dropped WAIT_IDR frame advanced timeline to seq=%d pts=%d", c.lastSequence, c.lastPTS)
	}
	if err := c.handle(mediaFrame(edgeipc.CodecH265, 3, 15000, edgeipc.FlagIDR, h265IDR())); err != nil {
		t.Fatal(err)
	}
	if c.waiting || c.lastSequence != 3 || c.lastPTS != 15000 {
		t.Fatalf("recovery state = waiting %v, seq=%d, pts=%d", c.waiting, c.lastSequence, c.lastPTS)
	}
}

func TestMediaHostRejectsConfiguredCodecMismatch(t *testing.T) {
	r := onlineRegistry(t, edgeipc.CodecH265)
	s := NewMediaHost(r, MediaHostConfig{IDRTimeout: time.Second})
	serverConn, peer := net.Pipe()
	done := make(chan error, 1)
	go func() { done <- s.ServeConn(serverConn) }()
	wire, err := edgeipc.MarshalMediaFrame(mediaFrame(edgeipc.CodecH264, 1, 9000, edgeipc.FlagIDR, h264IDR()))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := peer.Write(wire); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, edgeipc.ErrCodecMismatch) {
		t.Fatalf("codec mismatch error = %v", err)
	}
	peer.Close()
}

func TestMediaHostSupportsH264ParameterSets(t *testing.T) {
	r := onlineRegistry(t, edgeipc.CodecH264)
	s := NewMediaHost(r, MediaHostConfig{IDRTimeout: time.Second})
	frames := make(chan struct{}, 1)
	if err := r.Hub("cam-a").Subscribe("test", func(_ int64, au [][]byte, idr bool) {
		if len(au) != 3 || !idr {
			t.Errorf("H.264 AU = %d NALs, IDR %v", len(au), idr)
		}
		frames <- struct{}{}
	}); err != nil {
		t.Fatal(err)
	}
	defer r.Hub("cam-a").Unsubscribe("test")
	serverConn, peer := net.Pipe()
	done := make(chan error, 1)
	go func() { done <- s.ServeConn(serverConn) }()
	writeMedia(t, peer, mediaFrame(edgeipc.CodecH264, 1, 9000, edgeipc.FlagIDR, h264IDR()))
	select {
	case <-frames:
	case <-time.After(time.Second):
		t.Fatal("valid H.264 IDR was not broadcast")
	}
	peer.Close()
	if err := <-done; err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("ServeConn error = %v", err)
	}
}

func TestMediaHostTimeoutNotifiesOnce(t *testing.T) {
	r := onlineRegistry(t, edgeipc.CodecH265)
	var requests, failures atomic.Int32
	s := NewMediaHost(r, MediaHostConfig{
		IDRTimeout: 20 * time.Millisecond,
		RequestIDR: func(string) { requests.Add(1) },
		OnIDRTimeout: func(cameraID string, err error) {
			if cameraID != "cam-a" || !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("timeout = %q, %v", cameraID, err)
			}
			failures.Add(1)
		},
	})
	serverConn, peer := net.Pipe()
	done := make(chan error, 1)
	go func() { done <- s.ServeConn(serverConn) }()
	writeMedia(t, peer, mediaFrame(edgeipc.CodecH265, 1, 9000, 0, h265PFrame()))
	deadline := time.Now().Add(time.Second)
	for failures.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if requests.Load() != 1 || failures.Load() != 1 {
		t.Fatalf("requests/failures = %d/%d, want 1/1", requests.Load(), failures.Load())
	}
	writeMedia(t, peer, mediaFrame(edgeipc.CodecH265, 2, 12000, 0, h265PFrame()))
	time.Sleep(30 * time.Millisecond)
	if requests.Load() != 1 || failures.Load() != 1 {
		t.Fatalf("repeated wait notifications = %d/%d, want 1/1", requests.Load(), failures.Load())
	}
	peer.Close()
	if err := <-done; err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("ServeConn error = %v", err)
	}
}

func TestMediaHostReplacesSameCameraConnection(t *testing.T) {
	r := onlineRegistry(t, edgeipc.CodecH265)
	var published atomic.Int32
	if err := r.Hub("cam-a").Subscribe("test", func(_ int64, _ [][]byte, _ bool) { published.Add(1) }); err != nil {
		t.Fatal(err)
	}
	defer r.Hub("cam-a").Unsubscribe("test")
	s := NewMediaHost(r, MediaHostConfig{IDRTimeout: time.Second})
	oldServer, oldPeer := net.Pipe()
	oldDone := make(chan error, 1)
	go func() { oldDone <- s.ServeConn(oldServer) }()
	writeMedia(t, oldPeer, mediaFrame(edgeipc.CodecH265, 1, 9000, 0, h265PFrame()))

	newServer, newPeer := net.Pipe()
	newDone := make(chan error, 1)
	go func() { newDone <- s.ServeConn(newServer) }()
	writeMedia(t, newPeer, mediaFrame(edgeipc.CodecH265, 1, 9000, edgeipc.FlagIDR, h265IDR()))
	deadline := time.Now().Add(time.Second)
	for published.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if published.Load() != 1 {
		t.Fatalf("published frames = %d, want replacement IDR only", published.Load())
	}
	if err := <-oldDone; err == nil {
		t.Fatal("old connection was not replaced")
	}
	oldPeer.Close()
	newPeer.Close()
	if err := <-newDone; err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("new ServeConn error = %v", err)
	}
}

func TestMediaHostKeepsSlowCameraSeparate(t *testing.T) {
	r := newRegistry(t, CameraSpec{ID: "cam-a"}, CameraSpec{ID: "cam-b"})
	for _, cameraID := range []string{"cam-a", "cam-b"} {
		if err := r.HandleHello(hello(cameraID, 1, edgeipc.CodecH265)); err != nil {
			t.Fatal(err)
		}
		if err := r.HandleHealth(cameraID, 1, health()); err != nil {
			t.Fatal(err)
		}
	}
	var published atomic.Int32
	if err := r.Hub("cam-b").Subscribe("test", func(_ int64, _ [][]byte, _ bool) { published.Add(1) }); err != nil {
		t.Fatal(err)
	}
	defer r.Hub("cam-b").Unsubscribe("test")
	s := NewMediaHost(r, MediaHostConfig{IDRTimeout: time.Second})
	aServer, aPeer := net.Pipe()
	aDone := make(chan error, 1)
	go func() { aDone <- s.ServeConn(aServer) }()
	writeMedia(t, aPeer, mediaFrame(edgeipc.CodecH265, 1, 9000, 0, h265PFrame()))

	bServer, bPeer := net.Pipe()
	bDone := make(chan error, 1)
	go func() { bDone <- s.ServeConn(bServer) }()
	writeMedia(t, bPeer, edgeipc.MediaFrame{Codec: edgeipc.CodecH265, Flags: edgeipc.FlagIDR,
		CameraID: "cam-b", PTS90kHz: 9000, Sequence: 1, Payload: h265IDR()})
	deadline := time.Now().Add(time.Second)
	for published.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if published.Load() != 1 {
		t.Fatalf("cam-b frames = %d, want 1 while cam-a input is slow", published.Load())
	}
	aPeer.Close()
	bPeer.Close()
	if err := <-aDone; err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("cam-a ServeConn error = %v", err)
	}
	if err := <-bDone; err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("cam-b ServeConn error = %v", err)
	}
}

func TestMediaHostParserFailureEntersRecovery(t *testing.T) {
	r := onlineRegistry(t, edgeipc.CodecH265)
	var requests atomic.Int32
	s := NewMediaHost(r, MediaHostConfig{IDRTimeout: time.Second, RequestIDR: func(string) { requests.Add(1) }})
	frames := make(chan struct{}, 1)
	if err := r.Hub("cam-a").Subscribe("test", func(_ int64, _ [][]byte, _ bool) { frames <- struct{}{} }); err != nil {
		t.Fatal(err)
	}
	defer r.Hub("cam-a").Unsubscribe("test")
	serverConn, peer := net.Pipe()
	done := make(chan error, 1)
	go func() { done <- s.ServeConn(serverConn) }()
	bad := mediaFrame(edgeipc.CodecH265, 1, 9000, 0, h265PFrame())
	badWire, err := edgeipc.MarshalMediaFrame(bad)
	if err != nil {
		t.Fatal(err)
	}
	badWire[5] = byte(edgeipc.FlagIDR)
	if _, err := peer.Write(badWire); err != nil {
		t.Fatal(err)
	}
	writeMedia(t, peer, mediaFrame(edgeipc.CodecH265, 1, 9000, edgeipc.FlagIDR, h265IDR()))
	select {
	case <-frames:
	case <-time.After(time.Second):
		t.Fatal("recovery IDR was not broadcast")
	}
	if requests.Load() != 1 {
		t.Fatalf("IDR requests = %d, want 1", requests.Load())
	}
	peer.Close()
	if err := <-done; err != nil && !errors.Is(err, io.EOF) {
		t.Fatalf("ServeConn error = %v", err)
	}
}

func TestMediaHostOverflowNotifiesTimeoutAfterRequest(t *testing.T) {
	r := onlineRegistry(t, edgeipc.CodecH265)
	var requests atomic.Int32
	failures := make(chan string, 1)
	s := NewMediaHost(r, MediaHostConfig{
		MaxAUBytes: 1024,
		IDRTimeout: 20 * time.Millisecond,
		RequestIDR: func(string) { requests.Add(1) },
		OnIDRTimeout: func(cameraID string, err error) {
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Errorf("timeout error = %v", err)
			}
			failures <- cameraID
		},
	})
	serverConn, peer := net.Pipe()
	done := make(chan error, 1)
	go func() { done <- s.ServeConn(serverConn) }()
	writeMedia(t, peer, mediaFrame(edgeipc.CodecH265, 1, 9000, edgeipc.FlagIDR, h265IDR()))
	var header [edgeipc.MediaHeaderSize]byte
	header[0], header[1], header[2], header[3] = 'E', 'G', 'A', 'U'
	header[4], header[6] = edgeipc.ProtocolVersion, byte(edgeipc.CodecH265)
	binary.BigEndian.PutUint16(header[8:10], edgeipc.MediaHeaderSize)
	binary.BigEndian.PutUint16(header[10:12], 5)
	binary.BigEndian.PutUint32(header[12:16], 1025)
	if _, err := peer.Write(header[:]); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, edgeipc.ErrPayloadTooLarge) {
		t.Fatalf("overflow error = %v, want payload-too-large", err)
	}
	if got := requests.Load(); got != 2 {
		t.Fatalf("IDR requests = %d, want initial request plus overflow recovery", got)
	}
	select {
	case cameraID := <-failures:
		if cameraID != "cam-a" {
			t.Fatalf("failure camera = %q, want cam-a", cameraID)
		}
	case <-time.After(time.Second):
		t.Fatal("overflow timeout notification did not arrive")
	}
}

func TestMediaHostRejectsMalformedFrameWithoutLargeAllocation(t *testing.T) {
	r := onlineRegistry(t, edgeipc.CodecH265)
	s := NewMediaHost(r, MediaHostConfig{MaxAUBytes: 1024})
	serverConn, peer := net.Pipe()
	done := make(chan error, 1)
	go func() { done <- s.ServeConn(serverConn) }()
	var header [edgeipc.MediaHeaderSize]byte
	header[0], header[1], header[2], header[3] = 'E', 'G', 'A', 'U'
	header[4], header[6] = edgeipc.ProtocolVersion, byte(edgeipc.CodecH265)
	header[8], header[9] = 0, edgeipc.MediaHeaderSize
	header[10], header[11] = 0, 1
	header[12], header[13], header[14], header[15] = 0x00, 0x80, 0x00, 0x01
	if _, err := peer.Write(header[:]); err != nil {
		t.Fatal(err)
	}
	if err := <-done; !errors.Is(err, edgeipc.ErrPayloadTooLarge) {
		t.Fatalf("malicious length error = %v, want payload-too-large", err)
	}
	peer.Close()
}

func TestMediaHostUses0660Socket(t *testing.T) {
	r := newRegistry(t, CameraSpec{ID: "cam-a"})
	path := filepath.Join(t.TempDir(), "media.sock")
	s := NewMediaHost(r, MediaHostConfig{Path: path})
	l, err := s.Listen()
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0660 {
		t.Fatalf("socket mode = %o, want 660", got)
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestMediaHostCloseStopsUnboundConnection(t *testing.T) {
	r := newRegistry(t, CameraSpec{ID: "cam-a"})
	s := NewMediaHost(r, MediaHostConfig{})
	serverConn, peer := net.Pipe()
	done := make(chan error, 1)
	go func() { done <- s.ServeConn(serverConn) }()
	time.Sleep(time.Millisecond)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err == nil {
		t.Fatal("unbound connection survived server close")
	}
	peer.Close()
}

func onlineRegistry(t *testing.T, codec edgeipc.Codec) *CameraRegistry {
	t.Helper()
	r := newRegistry(t, CameraSpec{ID: "cam-a", Codec: codec})
	if err := r.HandleHello(hello("cam-a", 1, codec)); err != nil {
		t.Fatal(err)
	}
	if err := r.HandleHealth("cam-a", 1, health()); err != nil {
		t.Fatal(err)
	}
	return r
}

func mediaFrame(codec edgeipc.Codec, sequence, pts uint64, flags edgeipc.MediaFlags, payload []byte) edgeipc.MediaFrame {
	return edgeipc.MediaFrame{Codec: codec, Flags: flags, CameraID: "cam-a", Sequence: sequence, PTS90kHz: pts, Payload: payload}
}

func writeMedia(t *testing.T, peer net.Conn, frame edgeipc.MediaFrame) {
	t.Helper()
	if err := edgeipc.WriteMediaFrame(peer, frame); err != nil {
		t.Fatal(err)
	}
}

func readPTS(t *testing.T, frames <-chan int64, count int) []int64 {
	t.Helper()
	got := make([]int64, 0, count)
	for len(got) < count {
		select {
		case pts := <-frames:
			got = append(got, pts)
		case <-time.After(time.Second):
			t.Fatalf("received %d/%d frames", len(got), count)
		}
	}
	return got
}

func h265PFrame() []byte { return []byte{0, 0, 0, 1, 0x02, 0x01, 0x80} }

func h265IDR() []byte {
	payload := annexB(0x40)
	payload = append(payload, annexB(0x42)...)
	payload = append(payload, annexB(0x44)...)
	return append(payload, annexB(0x26)...)
}

func annexB(header byte) []byte { return []byte{0, 0, 0, 1, header, 1, 0x80} }

func h264IDR() []byte {
	payload := annexB(0x67)
	payload = append(payload, annexB(0x68)...)
	return append(payload, annexB(0x65)...)
}

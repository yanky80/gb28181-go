package main

import (
	"context"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/mickeyzzc/gb28181-go/edgeipc"
	"github.com/mickeyzzc/gb28181-go/platform"
)

func TestControlServerHelloReplacementAndAcquireCommands(t *testing.T) {
	registry := newRegistry(t, CameraSpec{ID: "cam-a"})
	server := NewControlServer(filepath.Join(t.TempDir(), "control.sock"), registry, time.Second)
	if err := server.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Stop() })

	old := dialPeer(t, server.path)
	writePeerMessage(t, old, hello("cam-a", 1, edgeipc.CodecH265))
	writePeerMessage(t, old, health())
	waitForCamera(t, registry, 1)

	current := dialPeer(t, server.path)
	writePeerMessage(t, current, hello("cam-a", 2, edgeipc.CodecH265))
	writePeerMessage(t, current, health())
	deadline := time.Now().Add(time.Second)
	for {
		view, ok := registry.Camera("cam-a")
		server.mu.Lock()
		bound := server.peers["cam-a"] != nil && server.peers["cam-a"].epoch == 2
		server.mu.Unlock()
		if ok && view.Online && view.StreamEpoch == 2 && bound {
			break
		}
		if time.Now().After(deadline) {
			server.mu.Lock()
			peer := server.peers["cam-a"]
			var peerEpoch uint64
			if peer != nil {
				peerEpoch = peer.epoch
			}
			server.mu.Unlock()
			t.Fatalf("current peer did not become healthy: online=%v epoch=%d bound=%v peerEpoch=%d", view.Online, view.StreamEpoch, bound, peerEpoch)
		}
		time.Sleep(time.Millisecond)
	}

	_ = old.SetReadDeadline(time.Now().Add(time.Second))
	var one [1]byte
	if _, err := old.Read(one[:]); err == nil {
		t.Fatal("replaced control connection remained open")
	}

	hub, release, err := server.AcquireMainHub(context.Background(), "cam-a")
	if err != nil {
		t.Fatal(err)
	}
	if hub == nil {
		t.Fatal("AcquireMainHub returned nil hub")
	}
	defer release()

	got := readPeerMessages(t, current, 2)
	if got[0].Type != edgeipc.MessageStart || got[0].RequestID != 1 {
		t.Fatalf("first command = %+v, want START request_id 1", got[0])
	}
	if got[1].Type != edgeipc.MessageRequestIDR || got[1].RequestID != 2 {
		t.Fatalf("second command = %+v, want REQUEST_IDR request_id 2", got[1])
	}
}

func waitForCamera(t *testing.T, registry *CameraRegistry, epoch uint64) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		view, ok := registry.Camera("cam-a")
		if ok && view.Online && view.StreamEpoch == epoch {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("camera did not become healthy: online=%v epoch=%d", view.Online, view.StreamEpoch)
		}
		time.Sleep(time.Millisecond)
	}
}

func dialPeer(t *testing.T, path string) net.Conn {
	t.Helper()
	conn, err := net.Dial("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func writePeerMessage(t *testing.T, conn net.Conn, message edgeipc.ControlMessage) {
	t.Helper()
	if err := edgeipc.WriteControlMessage(conn, message); err != nil {
		t.Fatal(err)
	}
}

func readPeerMessages(t *testing.T, conn net.Conn, count int) []edgeipc.ControlMessage {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	reader := edgeipc.NewControlReader(conn)
	messages := make([]edgeipc.ControlMessage, 0, count)
	for len(messages) < count {
		message, err := reader.Read()
		if err != nil {
			t.Fatalf("read peer command %d: %v", len(messages), err)
		}
		messages = append(messages, message)
	}
	return messages
}

func TestControlServerRejectsFirstNonHello(t *testing.T) {
	registry := newRegistry(t, CameraSpec{ID: "cam-a"})
	server := NewControlServer(filepath.Join(t.TempDir(), "control.sock"), registry, time.Second)
	if err := server.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Stop() })

	conn := dialPeer(t, server.path)
	writePeerMessage(t, conn, health())
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	var one [1]byte
	if _, err := conn.Read(one[:]); err == nil {
		t.Fatal("invalid first message connection remained usable")
	} else if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
		t.Fatal("invalid first message connection remained open")
	}
}

func TestControlServerLeaseGraceAndCommandIdempotence(t *testing.T) {
	registry := newRegistry(t, CameraSpec{ID: "cam-a"})
	server := NewControlServer(filepath.Join(t.TempDir(), "control.sock"), registry, 40*time.Millisecond)
	if err := server.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Stop() })

	peer := dialPeer(t, server.path)
	writePeerMessage(t, peer, hello("cam-a", 1, edgeipc.CodecH265))
	writePeerMessage(t, peer, health())
	waitForCamera(t, registry, 1)

	_, release1, err := server.AcquireMainHub(context.Background(), "cam-a")
	if err != nil {
		t.Fatal(err)
	}
	_, release2, err := server.AcquireMainHub(context.Background(), "cam-a")
	if err != nil {
		t.Fatal(err)
	}
	commands := readPeerMessages(t, peer, 2)
	if commands[0].Type != edgeipc.MessageStart || commands[1].Type != edgeipc.MessageRequestIDR {
		t.Fatalf("acquire commands = %+v", commands)
	}
	if commands[1].RequestID != commands[0].RequestID+1 {
		t.Fatalf("request ids = %d, %d; want process-local increment", commands[0].RequestID, commands[1].RequestID)
	}

	writePeerMessage(t, peer, edgeipc.ControlMessage{Type: edgeipc.MessageAck, Version: edgeipc.ProtocolVersion, RequestID: commands[1].RequestID, State: "started"})
	writePeerMessage(t, peer, edgeipc.ControlMessage{Type: edgeipc.MessageAck, Version: edgeipc.ProtocolVersion, RequestID: commands[0].RequestID, State: "stopped"})
	release1()
	release1()
	release2()
	assertNoPeerMessage(t, peer, 20*time.Millisecond)

	_, release3, err := server.AcquireMainHub(context.Background(), "cam-a")
	if err != nil {
		t.Fatal(err)
	}
	assertNoPeerMessage(t, peer, 20*time.Millisecond)
	release3()
	stop := readPeerMessages(t, peer, 1)[0]
	if stop.Type != edgeipc.MessageStop || stop.RequestID != commands[1].RequestID+1 || stop.GraceMS != 0 {
		t.Fatalf("stop command = %+v", stop)
	}
}

func TestControlServerReconnectReplaysOnlyCurrentDesiredState(t *testing.T) {
	registry := newRegistry(t, CameraSpec{ID: "cam-a"})
	server := NewControlServer(filepath.Join(t.TempDir(), "control.sock"), registry, time.Second)
	if err := server.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Stop() })

	old := dialPeer(t, server.path)
	writePeerMessage(t, old, hello("cam-a", 1, edgeipc.CodecH265))
	writePeerMessage(t, old, health())
	waitForCamera(t, registry, 1)
	_, release, err := server.AcquireMainHub(context.Background(), "cam-a")
	if err != nil {
		t.Fatal(err)
	}
	initial := readPeerMessages(t, old, 2)
	_ = old.Close()

	newPeer := dialPeer(t, server.path)
	writePeerMessage(t, newPeer, hello("cam-a", 2, edgeipc.CodecH265))
	writePeerMessage(t, newPeer, health())
	waitForCamera(t, registry, 2)
	replay := readPeerMessages(t, newPeer, 2)
	if replay[0].Type != edgeipc.MessageStart || replay[1].Type != edgeipc.MessageRequestIDR {
		t.Fatalf("replay = %+v", replay)
	}
	if replay[0].RequestID <= initial[1].RequestID || replay[1].RequestID != replay[0].RequestID+1 {
		t.Fatalf("replay request ids = %d, %d after %d", replay[0].RequestID, replay[1].RequestID, initial[1].RequestID)
	}
	release()
}

func TestControlServerDisconnectClosesRegistryHub(t *testing.T) {
	registry := newRegistry(t, CameraSpec{ID: "cam-a"})
	server := NewControlServer(filepath.Join(t.TempDir(), "control.sock"), registry, time.Second)
	if err := server.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Stop() })

	peer := dialPeer(t, server.path)
	writePeerMessage(t, peer, hello("cam-a", 1, edgeipc.CodecH265))
	writePeerMessage(t, peer, health())
	waitForCamera(t, registry, 1)
	hub, err := func() (*platform.FrameHub, error) {
		view, ok := registry.Camera("cam-a")
		if !ok {
			return nil, ErrControlUnavailable
		}
		return view.Hub, nil
	}()
	if err != nil {
		t.Fatal(err)
	}
	if err := hub.Subscribe("probe", func(int64, [][]byte, bool) {}); err != nil {
		t.Fatal(err)
	}
	_ = peer.Close()
	deadline := time.Now().Add(time.Second)
	for registry.CameraStatus("cam-a") != "OFF" || hub.ConsumerCount() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("disconnect did not converge: status=%s consumers=%d", registry.CameraStatus("cam-a"), hub.ConsumerCount())
		}
		time.Sleep(time.Millisecond)
	}
}

func assertNoPeerMessage(t *testing.T, conn net.Conn, wait time.Duration) {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(wait))
	reader := edgeipc.NewControlReader(conn)
	if _, err := reader.Read(); err == nil {
		t.Fatal("unexpected control command")
	}
}

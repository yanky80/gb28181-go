package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mickeyzzc/gb28181-go/edgeipc"
	"github.com/mickeyzzc/gb28181-go/platform"
)

const (
	defaultHealthInterval = 5 * time.Second
	controlWriteQueueSize = 32
)

var (
	ErrControlUnavailable = errors.New("gateway: control peer unavailable")
	ErrControlNotStarted  = errors.New("gateway: control server is not started")
)

// ControlServer owns the control.sock side of the edge IPC seam and exposes
// main-stream leases to cascade.
type ControlServer struct {
	path     string
	registry *CameraRegistry
	grace    time.Duration

	healthInterval time.Duration
	nextRequestID  atomic.Uint64
	nextConnection atomic.Uint64
	helloMu        sync.Mutex

	mu      sync.Mutex
	ln      net.Listener
	ctx     context.Context
	cancel  context.CancelFunc
	peers   map[string]*controlPeer
	leases  map[string]*leaseState
	wg      sync.WaitGroup
	stop    sync.Once
	stopErr error
}

type leaseState struct {
	count      int
	generation uint64
	desired    bool
	stopSent   bool
	peer       *controlPeer
	stopTimer  *time.Timer
}

type controlPeer struct {
	server    *ControlServer
	cameraID  string
	epoch     uint64
	sequence  uint64
	conn      net.Conn
	commands  chan controlCommand
	done      chan struct{}
	closeOnce sync.Once
}

type controlCommand struct {
	message    edgeipc.ControlMessage
	generation uint64
}

// NewControlServer creates a control.sock server. grace is the release grace
// period; callers normally pass Config.GB.StopGrace (the default is 5s).
func NewControlServer(path string, registry *CameraRegistry, grace time.Duration) *ControlServer {
	if grace < 0 {
		grace = 0
	}
	return &ControlServer{
		path: path, registry: registry, grace: grace,
		healthInterval: defaultHealthInterval,
		peers:          make(map[string]*controlPeer), leases: make(map[string]*leaseState),
	}
}

// IPCServer is kept as the descriptive name used by the gateway design.
type IPCServer = ControlServer

func NewIPCServer(path string, registry *CameraRegistry, grace time.Duration) *ControlServer {
	return NewControlServer(path, registry, grace)
}

// Start begins accepting one-camera control connections.
func (s *ControlServer) Start(ctx context.Context) error {
	if s.registry == nil {
		return errors.New("gateway: control server requires a registry")
	}
	if s.path == "" {
		return errors.New("gateway: control socket path is empty")
	}
	if err := removeStaleControlSocket(s.path); err != nil {
		return err
	}
	ln, err := net.Listen("unix", s.path)
	if err != nil {
		return fmt.Errorf("gateway: listen control socket: %w", err)
	}
	if err := os.Chmod(s.path, 0600); err != nil {
		_ = ln.Close()
		_ = os.Remove(s.path)
		return fmt.Errorf("gateway: secure control socket: %w", err)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	s.mu.Lock()
	s.ln = ln
	s.ctx, s.cancel = context.WithCancel(ctx)
	s.mu.Unlock()

	s.wg.Add(2)
	go s.acceptLoop()
	go s.healthLoop()
	if done := s.ctx.Done(); done != nil {
		go func() {
			<-done
			_ = s.Stop()
		}()
	}
	return nil
}

func removeStaleControlSocket(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("gateway: inspect control socket: %w", err)
	}
	if info.Mode()&os.ModeSocket == 0 {
		return fmt.Errorf("gateway: control socket path is not a socket")
	}
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("gateway: remove stale control socket: %w", err)
	}
	return nil
}

func (s *ControlServer) acceptLoop() {
	defer s.wg.Done()
	ln := s.listener()
	for {
		conn, err := ln.Accept()
		if err != nil {
			if s.isStopped() {
				return
			}
			continue
		}
		sequence := s.nextConnection.Add(1)
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleConn(conn, sequence)
		}()
	}
}

func (s *ControlServer) listener() net.Listener {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ln
}

func (s *ControlServer) healthLoop() {
	defer s.wg.Done()
	interval := s.healthInterval
	if interval <= 0 {
		interval = defaultHealthInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case now := <-ticker.C:
			s.registry.Expire(now)
		case <-s.context().Done():
			return
		}
	}
}

func (s *ControlServer) context() context.Context {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ctx == nil {
		return context.Background()
	}
	return s.ctx
}

func (s *ControlServer) isStopped() bool {
	select {
	case <-s.context().Done():
		return true
	default:
		return false
	}
}

// Stop closes the listener and all peers. It is safe to call more than once.
func (s *ControlServer) Stop() error {
	s.stop.Do(func() { s.stopErr = s.stopServer() })
	return s.stopErr
}

func (s *ControlServer) stopServer() error {
	s.mu.Lock()
	ln := s.ln
	cancel := s.cancel
	if ln == nil && cancel == nil {
		s.mu.Unlock()
		return nil
	}
	peers := make([]*controlPeer, 0, len(s.peers))
	for _, peer := range s.peers {
		peers = append(peers, peer)
	}
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if ln != nil {
		_ = ln.Close()
	}
	for _, peer := range peers {
		peer.close()
	}
	s.wg.Wait()
	s.mu.Lock()
	s.ln = nil
	s.cancel = nil
	s.mu.Unlock()
	if err := os.Remove(s.path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func (s *ControlServer) handleConn(conn net.Conn, sequence uint64) {
	reader := edgeipc.NewControlReader(conn)
	hello, err := reader.Read()
	if err != nil || hello.Type != edgeipc.MessageHello {
		_ = conn.Close()
		return
	}
	view, ok := s.registry.Camera(hello.CameraID)
	if !ok || edgeipc.ValidateControlCodec(view.Codec, hello) != nil {
		_ = conn.Close()
		return
	}
	s.helloMu.Lock()
	if !s.acceptsHello(hello.CameraID, sequence) {
		s.helloMu.Unlock()
		_ = conn.Close()
		return
	}
	if err := s.registry.HandleHello(hello); err != nil {
		s.helloMu.Unlock()
		_ = conn.Close()
		return
	}
	view, ok = s.registry.Camera(hello.CameraID)
	if !ok || view.StreamEpoch != hello.StreamEpoch {
		s.helloMu.Unlock()
		_ = conn.Close()
		return
	}
	peer := &controlPeer{
		server: s, cameraID: hello.CameraID, epoch: hello.StreamEpoch,
		sequence: sequence,
		conn:     conn, commands: make(chan controlCommand, controlWriteQueueSize),
		done: make(chan struct{}),
	}
	s.registerPeer(peer)
	s.helloMu.Unlock()
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		peer.writeLoop()
	}()
	defer peer.close()

	for {
		message, err := reader.Read()
		if err != nil {
			return
		}
		if err := s.handlePeerMessage(peer, message); err != nil {
			return
		}
	}
}

func (s *ControlServer) acceptsHello(cameraID string, sequence uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	peer := s.peers[cameraID]
	return peer == nil || sequence > peer.sequence
}

func (s *ControlServer) registerPeer(peer *controlPeer) {
	s.mu.Lock()
	state := s.leaseStateLocked(peer.cameraID)
	old := s.peers[peer.cameraID]
	s.peers[peer.cameraID] = peer
	state.peer = peer
	if state.count > 0 && state.desired {
		s.enqueueStartLocked(state, peer)
	} else if state.stopSent {
		s.enqueueStopLocked(state, peer, 0)
	}
	s.mu.Unlock()
	if old != nil && old != peer {
		old.close()
	}
}

func (s *ControlServer) handlePeerMessage(peer *controlPeer, message edgeipc.ControlMessage) error {
	if err := edgeipc.ValidateControlMessage(message); err != nil {
		return err
	}
	switch message.Type {
	case edgeipc.MessageHealth:
		return s.registry.HandleHealth(peer.cameraID, peer.epoch, message)
	case edgeipc.MessageAck:
		return nil // ACKs are observations; desired state never rolls back.
	case edgeipc.MessageReady:
		view, ok := s.registry.Camera(peer.cameraID)
		if !ok {
			return ErrControlUnavailable
		}
		return edgeipc.ValidateControlCodec(view.Codec, message)
	case edgeipc.MessageError:
		return nil
	case edgeipc.MessageStart, edgeipc.MessageRequestIDR, edgeipc.MessageStop:
		if message.CameraID != peer.cameraID {
			return errors.New("gateway: control message camera mismatch")
		}
		return nil
	default:
		return edgeipc.ErrUnknownMessageType
	}
}

func (s *ControlServer) peerGone(peer *controlPeer) {
	s.mu.Lock()
	if s.peers[peer.cameraID] == peer {
		delete(s.peers, peer.cameraID)
		state := s.leaseStateLocked(peer.cameraID)
		if state.peer == peer {
			state.peer = nil
		}
		s.registry.HandleDisconnect(peer.cameraID, peer.epoch)
	}
	s.mu.Unlock()
}

func (p *controlPeer) close() {
	p.closeOnce.Do(func() {
		close(p.done)
		_ = p.conn.Close()
		p.server.peerGone(p)
	})
}

func (p *controlPeer) writeLoop() {
	for {
		select {
		case command := <-p.commands:
			if !p.server.commandCurrent(p, command) {
				continue
			}
			if err := edgeipc.WriteControlMessage(p.conn, command.message); err != nil {
				p.close()
				return
			}
		case <-p.done:
			return
		}
	}
}

func (s *ControlServer) commandCurrent(peer *controlPeer, command controlCommand) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	state := s.leases[peer.cameraID]
	if s.peers[peer.cameraID] != peer || state == nil || state.peer != peer || state.generation != command.generation {
		return false
	}
	if command.message.Type == edgeipc.MessageStop {
		return !state.desired && state.stopSent
	}
	return state.desired && state.count > 0
}

func (p *controlPeer) enqueue(command controlCommand) error {
	select {
	case <-p.done:
		return ErrControlUnavailable
	case p.commands <- command:
		return nil
	default:
		return errors.New("gateway: control write queue full")
	}
}

// AcquireMainHub starts the encoder on the first live lease and returns its
// current registry hub. The returned release is idempotent.
func (s *ControlServer) AcquireMainHub(ctx context.Context, cameraID string) (*platform.FrameHub, func(), error) {
	if s.registry == nil {
		return nil, nil, ErrControlNotStarted
	}
	s.mu.Lock()
	started := s.ln != nil
	s.mu.Unlock()
	if !started {
		return nil, nil, ErrControlNotStarted
	}
	if ctx != nil {
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		default:
		}
	}
	view, ok := s.registry.Camera(cameraID)
	if !ok || !view.Online || view.Hub == nil {
		return nil, nil, ErrControlUnavailable
	}
	s.mu.Lock()
	state := s.leaseStateLocked(cameraID)
	peer := s.peers[cameraID]
	if peer == nil || state.peer != peer {
		s.mu.Unlock()
		return nil, nil, ErrControlUnavailable
	}
	gracePending := state.count == 0 && state.stopTimer != nil
	if state.stopTimer != nil {
		state.stopTimer.Stop()
		state.stopTimer = nil
	}
	if state.count == 0 {
		if !gracePending {
			state.generation++
			state.desired = true
			state.stopSent = false
			if err := s.enqueueStartLocked(state, peer); err != nil {
				s.mu.Unlock()
				return nil, nil, err
			}
		}
	}
	state.count++
	s.mu.Unlock()

	var once sync.Once
	release := func() { once.Do(func() { s.release(cameraID) }) }
	return view.Hub, release, nil
}

func (s *ControlServer) release(cameraID string) {
	s.mu.Lock()
	state := s.leaseStateLocked(cameraID)
	if state.count == 0 {
		s.mu.Unlock()
		return
	}
	state.count--
	if state.count == 0 {
		state.stopSent = false
		generation := state.generation
		state.stopTimer = time.AfterFunc(s.grace, func() { s.stopAfterGrace(cameraID, generation) })
	}
	s.mu.Unlock()
}

func (s *ControlServer) stopAfterGrace(cameraID string, generation uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	state := s.leases[cameraID]
	if state == nil || state.count != 0 || state.generation != generation {
		return
	}
	state.stopTimer = nil
	state.generation++
	state.desired = false
	state.stopSent = true
	if state.peer != nil {
		_ = s.enqueueStopLocked(state, state.peer, 0)
	}
}

func (s *ControlServer) enqueueStartLocked(state *leaseState, peer *controlPeer) error {
	start := controlCommand{generation: state.generation, message: edgeipc.ControlMessage{
		Type: edgeipc.MessageStart, Version: edgeipc.ProtocolVersion,
		CameraID: peer.cameraID, RequestID: s.nextRequestID.Add(1),
	}}
	if err := peer.enqueue(start); err != nil {
		return err
	}
	idr := controlCommand{generation: state.generation, message: edgeipc.ControlMessage{
		Type: edgeipc.MessageRequestIDR, Version: edgeipc.ProtocolVersion,
		CameraID: peer.cameraID, RequestID: s.nextRequestID.Add(1),
	}}
	return peer.enqueue(idr)
}

func (s *ControlServer) enqueueStopLocked(state *leaseState, peer *controlPeer, graceMS uint32) error {
	return peer.enqueue(controlCommand{generation: state.generation, message: edgeipc.ControlMessage{
		Type: edgeipc.MessageStop, Version: edgeipc.ProtocolVersion,
		CameraID: peer.cameraID, RequestID: s.nextRequestID.Add(1), GraceMS: graceMS,
	}})
}

func (s *ControlServer) leaseStateLocked(cameraID string) *leaseState {
	state := s.leases[cameraID]
	if state == nil {
		state = &leaseState{}
		s.leases[cameraID] = state
	}
	return state
}

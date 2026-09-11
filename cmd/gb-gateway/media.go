package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mickeyzzc/gb28181-go/edgeipc"
)

var (
	ErrMediaCameraMismatch     = errors.New("gateway: media connection camera mismatch")
	ErrMediaEpochMismatch      = errors.New("gateway: media connection stream epoch mismatch")
	ErrMediaPTSOverflow        = errors.New("gateway: media PTS exceeds FrameHub range")
	ErrMediaConnectionReplaced = errors.New("gateway: media connection was replaced")
)

// MediaHostConfig controls the media.sock listener and its recovery seam.
// RequestIDR and IDRTimeout are deliberately callbacks: control.sock and its
// command lifecycle belong to the control server, while media only observes
// the request and timeout boundary.
type MediaHostConfig struct {
	Path         string
	MaxAUBytes   int
	IDRTimeout   time.Duration
	RequestIDR   func(cameraID string)
	OnIDRTimeout func(cameraID string, err error)
}

// MediaHost accepts one Edge IPC media stream per connection and publishes
// complete, validated access units to the matching registry FrameHub.
type MediaHost struct {
	registry *CameraRegistry
	config   MediaHostConfig

	mu           sync.Mutex
	listener     net.Listener
	connections  map[string]*mediaConnection
	active       map[*mediaConnection]struct{}
	nextOrder    atomic.Uint64
	connectionWG sync.WaitGroup
	closeDone    chan struct{}
	closed       bool
}

// NewMediaHost creates a media.sock host. A zero timeout uses the gateway
// default, and a zero AU limit uses Edge IPC v1's 8 MiB wire limit.
func NewMediaHost(registry *CameraRegistry, config MediaHostConfig) *MediaHost {
	if config.IDRTimeout <= 0 {
		config.IDRTimeout = 3 * time.Second
	}
	if config.MaxAUBytes <= 0 || config.MaxAUBytes > edgeipc.MaxMediaPayload {
		config.MaxAUBytes = edgeipc.MaxMediaPayload
	}
	return &MediaHost{
		registry:    registry,
		config:      config,
		connections: make(map[string]*mediaConnection),
		active:      make(map[*mediaConnection]struct{}),
		closeDone:   make(chan struct{}),
	}
}

// Listen creates media.sock with the required owner-readable/group-writable
// mode. An existing path is removed only when it is already a Unix socket.
func (s *MediaHost) Listen() (net.Listener, error) {
	if s.registry == nil {
		return nil, errors.New("gateway: media registry is nil")
	}
	if s.config.Path == "" {
		return nil, errors.New("gateway: media socket path is empty")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, errors.New("gateway: media host is closed")
	}
	if s.listener != nil {
		return s.listener, nil
	}
	if info, err := os.Lstat(s.config.Path); err == nil {
		if info.Mode()&os.ModeSocket == 0 {
			return nil, fmt.Errorf("gateway: media socket path is not a socket")
		}
		if err := os.Remove(s.config.Path); err != nil {
			return nil, fmt.Errorf("gateway: remove stale media socket: %w", err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("gateway: inspect media socket: %w", err)
	}
	l, err := net.Listen("unix", s.config.Path)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(s.config.Path, 0660); err != nil {
		_ = l.Close()
		_ = os.Remove(s.config.Path)
		return nil, fmt.Errorf("gateway: chmod media socket: %w", err)
	}
	s.listener = l
	return l, nil
}

// Serve listens until ctx is cancelled or the listener fails.
func (s *MediaHost) Serve(ctx context.Context) error {
	l, err := s.Listen()
	if err != nil {
		return err
	}
	go func() {
		<-ctx.Done()
		_ = s.Close()
	}()
	for {
		conn, err := l.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			_ = conn.Close()
			continue
		}
		s.connectionWG.Add(1)
		s.mu.Unlock()
		go func() {
			defer s.connectionWG.Done()
			_ = s.ServeConn(conn)
		}()
	}
}

// ServeConn consumes one stream. It returns the peer/parser error after
// closing the connection; malformed access units are recoverable because the
// Edge IPC reader has already consumed their complete bounded frame.
func (s *MediaHost) ServeConn(conn net.Conn) error {
	if conn == nil {
		return errors.New("gateway: media connection is nil")
	}
	c := &mediaConnection{server: s, conn: conn, order: s.nextOrder.Add(1)}
	if err := s.trackConnection(c); err != nil {
		_ = conn.Close()
		return err
	}
	reader, err := edgeipc.NewMediaReaderWithMaxPayload(conn, s.config.MaxAUBytes)
	if err != nil {
		_ = conn.Close()
		return err
	}
	defer func() {
		c.close()
		s.removeConnection(c)
		_ = conn.Close()
	}()

	for {
		frame, err := reader.Read()
		if err != nil {
			if c.bound && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
				c.waitForIDR()
				c.keepTimeoutAfterClose()
			}
			if errors.Is(err, edgeipc.ErrInvalidAccessUnit) {
				continue
			}
			return err
		}
		if err := c.handle(frame); err != nil {
			return err
		}
	}
}

// Close stops the listener and all current camera connections.
func (s *MediaHost) Close() error {
	s.mu.Lock()
	if s.closed {
		done := s.closeDone
		s.mu.Unlock()
		<-done
		return nil
	}
	s.closed = true
	l := s.listener
	s.listener = nil
	connections := make([]*mediaConnection, 0, len(s.active))
	for c := range s.active {
		connections = append(connections, c)
	}
	s.mu.Unlock()

	var err error
	if l != nil {
		err = l.Close()
		if s.config.Path != "" {
			_ = os.Remove(s.config.Path)
		}
	}
	for _, c := range connections {
		c.close()
	}
	s.connectionWG.Wait()
	close(s.closeDone)
	return err
}

type mediaConnection struct {
	server *MediaHost
	conn   net.Conn
	order  uint64

	mu           sync.Mutex
	cameraID     string
	epoch        uint64
	codec        edgeipc.Codec
	bound        bool
	haveSequence bool
	lastSequence uint64
	havePTS      bool
	lastPTS      uint64
	waiting      bool
	notified     bool
	closed       bool
	keepTimer    bool
	timer        *time.Timer
}

func (c *mediaConnection) handle(frame edgeipc.MediaFrame) error {
	if !c.bound {
		view, ok := c.server.registry.Camera(frame.CameraID)
		if !ok {
			return ErrUnknownCamera
		}
		if view.StreamEpoch == 0 {
			return ErrMediaEpochMismatch
		}
		if frame.Codec != view.Codec {
			return edgeipc.ErrCodecMismatch
		}
		if frame.PTS90kHz > math.MaxInt64 {
			return ErrMediaPTSOverflow
		}
		c.cameraID, c.epoch, c.codec, c.bound = frame.CameraID, view.StreamEpoch, view.Codec, true
		if !c.server.replaceConnection(c) {
			return ErrMediaConnectionReplaced
		}
		c.waitForIDR()
	} else if frame.CameraID != c.cameraID {
		return ErrMediaCameraMismatch
	}

	view, ok := c.server.registry.Camera(c.cameraID)
	if !ok || view.StreamEpoch != c.epoch {
		return ErrMediaEpochMismatch
	}
	if frame.Codec != c.codec {
		return edgeipc.ErrCodecMismatch
	}
	if frame.PTS90kHz > math.MaxInt64 {
		return ErrMediaPTSOverflow
	}

	discontinuous := frame.Flags&edgeipc.FlagDiscontinuity != 0
	if !c.haveSequence && frame.Sequence != 1 {
		discontinuous = true
	}
	if c.haveSequence && (c.lastSequence == math.MaxUint64 || frame.Sequence != c.lastSequence+1) {
		discontinuous = true
	}
	if c.havePTS && frame.PTS90kHz <= c.lastPTS {
		discontinuous = true
	}
	if discontinuous {
		c.waitForIDR()
	}
	if c.isWaiting() {
		if frame.Flags&edgeipc.FlagIDR == 0 {
			return nil
		}
		c.recover()
	}
	c.lastSequence, c.haveSequence = frame.Sequence, true
	c.lastPTS, c.havePTS = frame.PTS90kHz, true

	return c.publish(frame)
}

func (c *mediaConnection) publish(frame edgeipc.MediaFrame) error {
	nals := annexBNALs(frame.Payload)
	if len(nals) == 0 {
		return edgeipc.ErrInvalidAccessUnit
	}
	c.server.mu.Lock()
	defer c.server.mu.Unlock()
	if c.server.closed || c.server.connections[c.cameraID] != c || c.isClosed() {
		return ErrMediaConnectionReplaced
	}
	c.server.registry.Publish(c.cameraID, c.epoch, int64(frame.PTS90kHz), nals, frame.Flags&edgeipc.FlagIDR != 0)
	return nil
}

func (c *mediaConnection) waitForIDR() {
	c.mu.Lock()
	if c.waiting || c.closed {
		c.mu.Unlock()
		return
	}
	c.waiting = true
	c.notified = false
	c.timer = time.AfterFunc(c.server.config.IDRTimeout, c.idrTimeout)
	c.mu.Unlock()
	if c.server.config.RequestIDR != nil {
		c.server.config.RequestIDR(c.cameraID)
	}
}

func (c *mediaConnection) recover() {
	c.mu.Lock()
	if !c.waiting {
		c.mu.Unlock()
		return
	}
	c.waiting = false
	if c.timer != nil {
		c.timer.Stop()
		c.timer = nil
	}
	c.mu.Unlock()
}

func (c *mediaConnection) isWaiting() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.waiting
}

func (c *mediaConnection) idrTimeout() {
	c.mu.Lock()
	if (c.closed && !c.keepTimer) || !c.waiting || c.notified {
		c.mu.Unlock()
		return
	}
	c.notified = true
	c.timer = nil
	c.mu.Unlock()
	if c.server.config.OnIDRTimeout != nil {
		c.server.config.OnIDRTimeout(c.cameraID, context.DeadlineExceeded)
	}
}

func (c *mediaConnection) keepTimeoutAfterClose() {
	c.mu.Lock()
	c.keepTimer = true
	c.mu.Unlock()
}

func (c *mediaConnection) close() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	if c.timer != nil && !c.keepTimer {
		c.timer.Stop()
		c.timer = nil
	}
	c.mu.Unlock()
	_ = c.conn.Close()
}

func (c *mediaConnection) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

func (s *MediaHost) replaceConnection(c *mediaConnection) bool {
	s.mu.Lock()
	old := s.connections[c.cameraID]
	if old != nil && old.order > c.order {
		s.mu.Unlock()
		c.close()
		return false
	}
	s.connections[c.cameraID] = c
	s.mu.Unlock()
	if old != nil && old != c {
		old.close()
	}
	return true
}

func (s *MediaHost) removeConnection(c *mediaConnection) {
	s.mu.Lock()
	delete(s.active, c)
	if !c.bound {
		s.mu.Unlock()
		return
	}
	if s.connections[c.cameraID] == c {
		delete(s.connections, c.cameraID)
	}
	s.mu.Unlock()
}

func (s *MediaHost) trackConnection(c *mediaConnection) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("gateway: media host is closed")
	}
	s.active[c] = struct{}{}
	return nil
}

func annexBNALs(payload []byte) [][]byte {
	var nals [][]byte
	for i := 0; i < len(payload); {
		start, prefix := annexBStartCode(payload, i)
		if prefix == 0 {
			break
		}
		body := start + prefix
		end := len(payload)
		for j := body; j < len(payload); j++ {
			if _, nextPrefix := annexBStartCode(payload, j); nextPrefix != 0 {
				end = j
				break
			}
		}
		for end > body && payload[end-1] == 0 {
			end--
		}
		if end == body {
			return nil
		}
		nals = append(nals, payload[body:end])
		i = end
	}
	return nals
}

func annexBStartCode(data []byte, at int) (int, int) {
	if at+3 <= len(data) && data[at] == 0 && data[at+1] == 0 && data[at+2] == 1 {
		return at, 3
	}
	if at+4 <= len(data) && data[at] == 0 && data[at+1] == 0 && data[at+2] == 0 && data[at+3] == 1 {
		return at, 4
	}
	return 0, 0
}

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/mickeyzzc/gb28181-go/platform/cascade"
)

const (
	channelStorePreviousVersion = 1
	channelStoreVersion         = 2
)

var (
	ErrChannelStoreCorrupt  = errors.New("channel store is corrupt")
	ErrChannelStoreVersion  = errors.New("unsupported channel store version")
	ErrChannelStoreConflict = errors.New("channel store mapping conflict")
)

type channelStoreFile struct {
	Version  int                   `json:"version"`
	Channels []channelStoreChannel `json:"channels"`
}

type channelStoreChannel struct {
	CameraID    string    `json:"camera_id"`
	GBChannelID string    `json:"gb_channel_id"`
	Name        string    `json:"name,omitempty"`
	UpdatedAt   time.Time `json:"updated_at,omitempty"`
}

// ChannelStore persists the stable local-camera-to-GB-channel mapping used by
// the gateway. Mutations are serialized so a failed disk write cannot publish
// an in-memory mapping that was never persisted.
type ChannelStore struct {
	mu       sync.RWMutex
	path     string
	channels map[string]cascade.CascadeChannel
}

var _ cascade.Store = (*ChannelStore)(nil)

// NewChannelStore opens path, or creates an empty store when it does not yet
// exist. The parent directory must already exist.
func NewChannelStore(path string) (*ChannelStore, error) {
	if path == "" {
		return nil, fmt.Errorf("channel store path is empty")
	}
	s := &ChannelStore{path: path, channels: make(map[string]cascade.CascadeChannel)}
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *ChannelStore) load() error {
	data, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read channel store: %w", err)
	}

	var file channelStoreFile
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := decoder.Decode(&file); err != nil {
		return fmt.Errorf("%w: decode: %v", ErrChannelStoreCorrupt, err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return fmt.Errorf("%w: trailing JSON", ErrChannelStoreCorrupt)
		}
		return fmt.Errorf("%w: trailing data: %v", ErrChannelStoreCorrupt, err)
	}
	if file.Version != channelStorePreviousVersion && file.Version != channelStoreVersion {
		return fmt.Errorf("%w: %d", ErrChannelStoreVersion, file.Version)
	}

	channels, err := validateChannelRows(file.Channels)
	if err != nil {
		return err
	}
	s.channels = channels
	return nil
}

func validateChannelRows(rows []channelStoreChannel) (map[string]cascade.CascadeChannel, error) {
	channels := make(map[string]cascade.CascadeChannel, len(rows))
	gbIDs := make(map[string]string, len(rows))
	for _, row := range rows {
		if err := validateChannelMapping(row.CameraID, row.GBChannelID); err != nil {
			return nil, fmt.Errorf("%w: %v", ErrChannelStoreCorrupt, err)
		}
		if _, exists := channels[row.CameraID]; exists {
			return nil, fmt.Errorf("%w: duplicate camera_id %q", ErrChannelStoreCorrupt, row.CameraID)
		}
		if cameraID, exists := gbIDs[row.GBChannelID]; exists {
			return nil, fmt.Errorf("%w: gb_channel_id %q belongs to %q and %q", ErrChannelStoreCorrupt, row.GBChannelID, cameraID, row.CameraID)
		}
		channels[row.CameraID] = cascade.CascadeChannel{
			CameraID:    row.CameraID,
			GBChannelID: row.GBChannelID,
			Name:        row.Name,
			UpdatedAt:   row.UpdatedAt,
		}
		gbIDs[row.GBChannelID] = row.CameraID
	}
	return channels, nil
}

func validGBChannelID(id string) bool {
	if len(id) != 20 {
		return false
	}
	for i := range id {
		if id[i] < '0' || id[i] > '9' {
			return false
		}
	}
	return true
}

func validateChannelMapping(cameraID, gbChannelID string) error {
	if cameraID == "" {
		return errors.New("empty camera_id")
	}
	if !validGBChannelID(gbChannelID) {
		return fmt.Errorf("gb_channel_id %q must be 20 digits", gbChannelID)
	}
	return nil
}

// UpsertCascadeChannel adds or updates a local camera's persisted metadata. A
// local camera's assigned GB channel ID is immutable once it has been stored.
func (s *ChannelStore) UpsertCascadeChannel(ctx context.Context, ch cascade.CascadeChannel) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := validateChannelMapping(ch.CameraID, ch.GBChannelID); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if previous, exists := s.channels[ch.CameraID]; exists && previous.GBChannelID != ch.GBChannelID {
		return fmt.Errorf("%w: camera_id %q already maps to %q", ErrChannelStoreConflict, ch.CameraID, previous.GBChannelID)
	}
	for cameraID, previous := range s.channels {
		if cameraID != ch.CameraID && previous.GBChannelID == ch.GBChannelID {
			return fmt.Errorf("%w: gb_channel_id %q already maps to %q", ErrChannelStoreConflict, ch.GBChannelID, cameraID)
		}
	}

	next := cloneChannels(s.channels)
	next[ch.CameraID] = ch
	if err := s.write(next); err != nil {
		return err
	}
	s.channels = next
	return nil
}

func (s *ChannelStore) ListCascadeChannels(ctx context.Context) ([]cascade.CascadeChannel, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	channels := make([]cascade.CascadeChannel, 0, len(s.channels))
	for _, ch := range s.channels {
		channels = append(channels, ch)
	}
	sort.Slice(channels, func(i, j int) bool {
		return channels[i].CameraID < channels[j].CameraID
	})
	return channels, nil
}

// Recordings belong to a later gateway capability. Returning an empty result
// keeps the channel-mapping store compatible with cascade.Store without
// claiming that playback data exists.
func (s *ChannelStore) ListRecordings(ctx context.Context, _ cascade.RecordingFilter) ([]cascade.Recording, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return nil, nil
}

func cloneChannels(channels map[string]cascade.CascadeChannel) map[string]cascade.CascadeChannel {
	clone := make(map[string]cascade.CascadeChannel, len(channels)+1)
	for cameraID, ch := range channels {
		clone[cameraID] = ch
	}
	return clone
}

func (s *ChannelStore) write(channels map[string]cascade.CascadeChannel) error {
	rows := make([]channelStoreChannel, 0, len(channels))
	for _, ch := range channels {
		rows = append(rows, channelStoreChannel{
			CameraID:    ch.CameraID,
			GBChannelID: ch.GBChannelID,
			Name:        ch.Name,
			UpdatedAt:   ch.UpdatedAt,
		})
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].CameraID < rows[j].CameraID })
	data, err := json.MarshalIndent(channelStoreFile{Version: channelStoreVersion, Channels: rows}, "", "  ")
	if err != nil {
		return fmt.Errorf("encode channel store: %w", err)
	}
	data = append(data, '\n')

	dir := filepath.Dir(s.path)
	tmp, err := os.CreateTemp(dir, ".channels.json.tmp-*")
	if err != nil {
		return fmt.Errorf("create channel store temporary file: %w", err)
	}
	tmpName := tmp.Name()
	removeTemp := true
	defer func() {
		if removeTemp {
			_ = os.Remove(tmpName)
		}
	}()

	if n, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write channel store temporary file: %w", err)
	} else if n != len(data) {
		_ = tmp.Close()
		return fmt.Errorf("write channel store temporary file: %w", io.ErrShortWrite)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("sync channel store temporary file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close channel store temporary file: %w", err)
	}
	if err := os.Rename(tmpName, s.path); err != nil {
		return fmt.Errorf("replace channel store: %w", err)
	}
	removeTemp = false

	directory, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open channel store directory: %w", err)
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return fmt.Errorf("sync channel store directory: %w", err)
	}
	return nil
}

package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/mickeyzzc/gb28181-go/platform/cascade"
)

const recordingIndexVersion = 1

var (
	ErrRecordingIndexCorrupt = errors.New("recording index is corrupt")
	ErrRecordingProbe        = errors.New("recording probe failed")
)

type recordingKeyframe struct {
	TimeMS int64 `json:"time_ms"`
	Offset int64 `json:"offset"`
	Size   int64 `json:"size"`
}

type recordingProbeResult struct {
	Codec      string
	Timescale  uint32
	Frames     int
	Keyframes  int
	KeyframeAt []recordingKeyframe
	StartedAt  time.Time
	EndedAt    time.Time
}

type recordingProbe func(context.Context, string) (recordingProbeResult, error)

type recordingIndexEvent struct {
	Version   int                 `json:"version"`
	Op        string              `json:"op"`
	ID        string              `json:"id"`
	CameraID  string              `json:"camera_id,omitempty"`
	File      string              `json:"file,omitempty"`
	Format    cascade.Format      `json:"format,omitempty"`
	Size      int64               `json:"size,omitempty"`
	StartedAt time.Time           `json:"started_at,omitempty"`
	EndedAt   time.Time           `json:"ended_at,omitempty"`
	Duration  float64             `json:"duration,omitempty"`
	Timescale uint32              `json:"timescale,omitempty"`
	Frames    int                 `json:"frames,omitempty"`
	Keyframes []recordingKeyframe `json:"keyframes,omitempty"`
}

type recordingObservation struct {
	Size        int64
	ModTime     time.Time
	Info        os.FileInfo
	StableScans int
	Verified    bool
}

type recordingIndexEntry struct {
	ID        string
	CameraID  string
	File      string
	Format    cascade.Format
	Size      int64
	StartedAt time.Time
	EndedAt   time.Time
	Duration  float64
	Timescale uint32
	Frames    int
	Keyframes []recordingKeyframe
}

// RecordingStore is the gateway-owned in-memory view and append-only journal
// of local recording segments. The journal stores paths relative to root so a
// restart keeps the index portable when the recording root is configured the
// same way on the host.
type RecordingStore struct {
	mu        sync.RWMutex
	scanMu    sync.Mutex
	indexPath string
	root      string
	probe     recordingProbe
	entries   map[string]recordingIndexEntry
	observed  map[string]recordingObservation
	// snapshotStep is a test-only seam for holding the read snapshot open.
	snapshotStep func()
	// removeBeforeLock is a test-only seam for proving Remove overlaps a snapshot.
	removeBeforeLock func()
}

// NewRecordingStore loads indexPath, accepting an incomplete final JSONL line
// as a crash tail. The index parent directory must already exist.
func NewRecordingStore(indexPath, root string) (*RecordingStore, error) {
	return newRecordingStore(indexPath, root, probeRecording)
}

func newRecordingStore(indexPath, root string, probe recordingProbe) (*RecordingStore, error) {
	if indexPath == "" || root == "" {
		return nil, errors.New("recording index path and root are required")
	}
	if probe == nil {
		return nil, errors.New("recording probe is nil")
	}
	s := &RecordingStore{
		indexPath: indexPath,
		root:      filepath.Clean(root),
		probe:     probe,
		entries:   make(map[string]recordingIndexEntry),
		observed:  make(map[string]recordingObservation),
	}
	if err := s.load(); err != nil {
		return nil, err
	}
	return s, nil
}

// Scan checks only root/{camera}/{today}/*.mp4. A file must have the same
// size in two scans before it is probed and added to the index.
func (s *RecordingStore) Scan(ctx context.Context) error {
	return s.scanAt(ctx, time.Now())
}

func (s *RecordingStore) scanAt(ctx context.Context, now time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.scanMu.Lock()
	defer s.scanMu.Unlock()

	day := now.In(now.Location()).Format("20060102")
	rootEntries, err := os.ReadDir(s.root)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read recording root: %w", err)
	}
	seen := make(map[string]bool)
	for _, camera := range rootEntries {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !camera.IsDir() || camera.Type()&os.ModeSymlink != 0 {
			continue
		}
		dayDir := filepath.Join(s.root, camera.Name(), day)
		files, err := os.ReadDir(dayDir)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return fmt.Errorf("read recording day directory: %w", err)
		}
		for _, file := range files {
			if err := ctx.Err(); err != nil {
				return err
			}
			if file.IsDir() || file.Type()&os.ModeSymlink != 0 || !strings.EqualFold(filepath.Ext(file.Name()), ".mp4") {
				continue
			}
			path := filepath.Join(dayDir, file.Name())
			info, err := file.Info()
			if err != nil || !info.Mode().IsRegular() {
				continue
			}
			rel, err := filepath.Rel(s.root, path)
			if err != nil || !validRecordingPath(rel) {
				continue
			}
			id := filepath.ToSlash(rel)
			seen[id] = true
			if err := s.indexFile(ctx, id, path, camera.Name(), info); err != nil {
				return err
			}
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	for id, entry := range s.entries {
		if isRecordingDay(entry.File, day) && !seen[id] {
			if err := s.deleteLocked(ctx, id); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *RecordingStore) indexFile(ctx context.Context, id, path, cameraID string, info os.FileInfo) error {
	size := info.Size()
	s.mu.Lock()
	obs, observed := s.observed[id]
	sameIdentity := observed && obs.Info != nil && os.SameFile(obs.Info, info)
	sameObservation := sameIdentity && obs.Size == size && obs.ModTime.Equal(info.ModTime())
	observationChanged := observed && !sameObservation
	if !sameObservation {
		obs = recordingObservation{Size: size, ModTime: info.ModTime(), Info: info, StableScans: 1}
	} else {
		obs.StableScans++
	}
	if observationChanged {
		if _, ok := s.entries[id]; ok {
			if err := s.deleteLocked(ctx, id); err != nil {
				s.mu.Unlock()
				return err
			}
		}
	}
	s.observed[id] = obs
	alreadyIndexed := false
	if entry, ok := s.entries[id]; ok && entry.Size == size && sameObservation && obs.Verified {
		alreadyIndexed = true
	}
	s.mu.Unlock()
	if alreadyIndexed || obs.StableScans < 2 {
		return nil
	}

	target, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer target.Close()
	targetInfo, err := target.Stat()
	if err != nil || !targetInfo.Mode().IsRegular() || targetInfo.Size() != size || !os.SameFile(info, targetInfo) {
		return nil
	}
	metadata, err := s.probe(ctx, path)
	if err != nil {
		// A file can still be written or removed between directory enumeration
		// and probing. It remains eligible for the next scan.
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		return nil
	}
	if err := validateProbeResult(metadata); err != nil {
		return nil
	}
	currentInfo, err := os.Stat(path)
	if err != nil || !currentInfo.Mode().IsRegular() {
		s.mu.Lock()
		delete(s.observed, id)
		s.mu.Unlock()
		return nil
	}
	if currentInfo.Size() != size || !os.SameFile(targetInfo, currentInfo) || !currentInfo.ModTime().Equal(targetInfo.ModTime()) {
		s.mu.Lock()
		delete(s.observed, id)
		s.mu.Unlock()
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.observed[id]
	if !ok || current.Size != size || !current.ModTime.Equal(targetInfo.ModTime()) || current.StableScans < 2 {
		return nil
	}
	entry := recordingIndexEntry{
		ID: id, CameraID: cameraID, File: filepath.ToSlash(id),
		Format: cascade.Format(strings.ToLower(metadata.Codec)), Size: size,
		StartedAt: metadata.StartedAt, EndedAt: metadata.EndedAt,
		Duration:  metadata.EndedAt.Sub(metadata.StartedAt).Seconds(),
		Timescale: metadata.Timescale, Frames: metadata.Frames,
		Keyframes: append([]recordingKeyframe(nil), metadata.KeyframeAt...),
	}
	if err := s.appendLocked(ctx, upsertEvent(entry)); err != nil {
		return err
	}
	s.entries[id] = entry
	current.Verified = true
	s.observed[id] = current
	return nil
}

// ListRecordings returns a stable snapshot. It never opens the media files,
// so cleanup of one file cannot invalidate the other results in the query.
func (s *RecordingStore) ListRecordings(ctx context.Context, filter cascade.RecordingFilter) ([]cascade.Recording, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.RLock()
	entries := make([]recordingIndexEntry, 0, len(s.entries))
	for _, entry := range s.entries {
		if filter.CameraID != "" && entry.CameraID != filter.CameraID {
			continue
		}
		if !entry.EndedAt.After(filter.StartTime) || entry.StartedAt.After(filter.EndTime) {
			continue
		}
		entry.Keyframes = nil
		entries = append(entries, entry)
		if len(entries) == 1 && s.snapshotStep != nil {
			s.snapshotStep()
		}
	}
	s.mu.RUnlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	sort.SliceStable(entries, func(i, j int) bool {
		if entries[i].StartedAt.Equal(entries[j].StartedAt) {
			return entries[i].ID < entries[j].ID
		}
		return entries[i].StartedAt.Before(entries[j].StartedAt)
	})
	if filter.Limit > 0 && len(entries) > filter.Limit {
		entries = entries[:filter.Limit]
	}
	out := make([]cascade.Recording, 0, len(entries))
	for _, entry := range entries {
		out = append(out, cascade.Recording{
			ID: entry.ID, CameraID: entry.CameraID,
			FilePath: filepath.Join(s.root, filepath.FromSlash(entry.File)),
			Format:   entry.Format, StartedAt: entry.StartedAt,
			EndedAt: entry.EndedAt, Duration: entry.Duration,
		})
	}
	return out, nil
}

// Remove appends a tombstone for an index ID. It does not remove the media
// file; file ownership remains with the recorder/retention process.
func (s *RecordingStore) Remove(ctx context.Context, id string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.removeBeforeLock != nil {
		s.removeBeforeLock()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.deleteLocked(ctx, id)
}

func (s *RecordingStore) deleteLocked(ctx context.Context, id string) error {
	if _, ok := s.entries[id]; !ok {
		return nil
	}
	if err := s.appendLocked(ctx, recordingIndexEvent{Version: recordingIndexVersion, Op: "delete", ID: id}); err != nil {
		return err
	}
	delete(s.entries, id)
	delete(s.observed, id)
	return nil
}

// Compact rewrites the current view to a temporary file and atomically swaps
// it into place. The store lock makes compaction and append mutually exclusive.
func (s *RecordingStore) Compact(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	entries := make([]recordingIndexEntry, 0, len(s.entries))
	for _, entry := range s.entries {
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].ID < entries[j].ID })

	dir := filepath.Dir(s.indexPath)
	tmp, err := os.CreateTemp(dir, ".recordings.jsonl.tmp-*")
	if err != nil {
		return fmt.Errorf("create recording index temporary file: %w", err)
	}
	tmpName := tmp.Name()
	removeTemp := true
	defer func() {
		if removeTemp {
			_ = os.Remove(tmpName)
		}
	}()
	if err := tmp.Chmod(0o600); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("chmod recording index temporary file: %w", err)
	}
	writer := bufio.NewWriter(tmp)
	for _, entry := range entries {
		data, err := json.Marshal(upsertEvent(entry))
		if err != nil {
			_ = tmp.Close()
			return fmt.Errorf("encode recording index: %w", err)
		}
		if _, err := writer.Write(append(data, '\n')); err != nil {
			_ = tmp.Close()
			return fmt.Errorf("write recording index: %w", err)
		}
	}
	if err := writer.Flush(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("flush recording index: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("sync recording index: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close recording index: %w", err)
	}
	if err := os.Rename(tmpName, s.indexPath); err != nil {
		return fmt.Errorf("replace recording index: %w", err)
	}
	removeTemp = false
	directory, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open recording index directory: %w", err)
	}
	defer directory.Close()
	if err := directory.Sync(); err != nil {
		return fmt.Errorf("sync recording index directory: %w", err)
	}
	return nil
}

func (s *RecordingStore) load() error {
	f, err := os.OpenFile(s.indexPath, os.O_RDWR, 0)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read recording index: %w", err)
	}
	defer f.Close()
	data, err := io.ReadAll(f)
	if err != nil {
		return fmt.Errorf("read recording index: %w", err)
	}
	lastComplete := 0
	lines := bytes.SplitAfter(data, []byte{'\n'})
	for i, line := range lines {
		complete := len(line) > 0 && line[len(line)-1] == '\n'
		if len(bytes.TrimSpace(line)) != 0 {
			var event recordingIndexEvent
			decodeErr := json.Unmarshal(bytes.TrimSpace(line), &event)
			if decodeErr != nil {
				if i == len(lines)-1 && !complete {
					if err := f.Truncate(int64(lastComplete)); err != nil {
						return fmt.Errorf("repair recording index tail: %w", err)
					}
					if err := f.Sync(); err != nil {
						return fmt.Errorf("sync repaired recording index: %w", err)
					}
					return nil // crash left a partial final event
				}
				return fmt.Errorf("%w: %w", ErrRecordingIndexCorrupt, decodeErr)
			}
			if err := s.applyEvent(event); err != nil {
				return err
			}
		}
		if complete {
			lastComplete += len(line)
			continue
		}
		if len(bytes.TrimSpace(line)) != 0 {
			if _, err := f.WriteAt([]byte{'\n'}, int64(len(data))); err != nil {
				return fmt.Errorf("repair recording index terminator: %w", err)
			}
			if err := f.Sync(); err != nil {
				return fmt.Errorf("sync repaired recording index: %w", err)
			}
		}
	}
	return nil
}

func (s *RecordingStore) applyEvent(event recordingIndexEvent) error {
	if event.Version != recordingIndexVersion {
		return fmt.Errorf("%w: unsupported version %d", ErrRecordingIndexCorrupt, event.Version)
	}
	if event.ID == "" {
		return fmt.Errorf("%w: empty event id", ErrRecordingIndexCorrupt)
	}
	switch event.Op {
	case "delete":
		delete(s.entries, event.ID)
	case "upsert":
		if err := validateEvent(event); err != nil {
			return err
		}
		s.entries[event.ID] = recordingIndexEntry{
			ID: event.ID, CameraID: event.CameraID, File: event.File,
			Format: event.Format, Size: event.Size, StartedAt: event.StartedAt,
			EndedAt: event.EndedAt, Duration: event.Duration,
			Timescale: event.Timescale, Frames: event.Frames,
			Keyframes: append([]recordingKeyframe(nil), event.Keyframes...),
		}
	default:
		return fmt.Errorf("%w: unsupported operation %q", ErrRecordingIndexCorrupt, event.Op)
	}
	return nil
}

func (s *RecordingStore) appendLocked(ctx context.Context, event recordingIndexEvent) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("encode recording index event: %w", err)
	}
	f, err := os.OpenFile(s.indexPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("open recording index: %w", err)
	}
	defer f.Close()
	if _, err := f.Write(append(data, '\n')); err != nil {
		return fmt.Errorf("append recording index: %w", err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("sync recording index: %w", err)
	}
	return nil
}

func upsertEvent(entry recordingIndexEntry) recordingIndexEvent {
	return recordingIndexEvent{
		Version: recordingIndexVersion, Op: "upsert", ID: entry.ID,
		CameraID: entry.CameraID, File: entry.File, Format: entry.Format,
		Size: entry.Size, StartedAt: entry.StartedAt, EndedAt: entry.EndedAt,
		Duration: entry.Duration, Timescale: entry.Timescale, Frames: entry.Frames,
		Keyframes: entry.Keyframes,
	}
}

func validateEvent(event recordingIndexEvent) error {
	if !validRecordingPath(event.File) || filepath.ToSlash(event.File) != event.ID {
		return fmt.Errorf("%w: invalid file path for %q", ErrRecordingIndexCorrupt, event.ID)
	}
	if event.CameraID == "" || (event.Format != cascade.FormatH264 && event.Format != cascade.FormatH265) ||
		event.Timescale == 0 || event.Frames < 1 || !validKeyframes(event.Keyframes) ||
		event.StartedAt.IsZero() || !event.EndedAt.After(event.StartedAt) {
		return fmt.Errorf("%w: invalid recording %q", ErrRecordingIndexCorrupt, event.ID)
	}
	return nil
}

func validKeyframes(keyframes []recordingKeyframe) bool {
	if len(keyframes) == 0 {
		return false
	}
	for _, keyframe := range keyframes {
		if keyframe.TimeMS < 0 || keyframe.Offset < 0 || keyframe.Size <= 0 {
			return false
		}
	}
	return true
}

func validateProbeResult(result recordingProbeResult) error {
	if (result.Codec != string(cascade.FormatH264) && result.Codec != string(cascade.FormatH265)) ||
		result.Timescale == 0 || result.Frames < 1 || result.Keyframes < 1 || len(result.KeyframeAt) != result.Keyframes || !validKeyframes(result.KeyframeAt) ||
		result.StartedAt.IsZero() || !result.EndedAt.After(result.StartedAt) {
		return ErrRecordingProbe
	}
	return nil
}

func validRecordingPath(path string) bool {
	clean := filepath.Clean(path)
	return path != "" && clean != "." && !filepath.IsAbs(path) && clean != ".." && !strings.HasPrefix(clean, ".."+string(filepath.Separator)) && filepath.ToSlash(clean) == filepath.ToSlash(path)
}

func isRecordingDay(path, day string) bool {
	parts := strings.Split(filepath.ToSlash(path), "/")
	return len(parts) == 3 && parts[1] == day
}

type ffprobeOutput struct {
	Format struct {
		StartTime string            `json:"start_time"`
		Duration  string            `json:"duration"`
		Tags      map[string]string `json:"tags"`
	} `json:"format"`
	Streams []struct {
		CodecName string `json:"codec_name"`
		TimeBase  string `json:"time_base"`
		StartTime string `json:"start_time"`
		Duration  string `json:"duration"`
	} `json:"streams"`
	Frames []struct {
		KeyFrame int    `json:"key_frame"`
		Time     string `json:"best_effort_timestamp_time"`
		Offset   string `json:"pkt_pos"`
		Size     string `json:"pkt_size"`
	} `json:"frames"`
}

func probeRecording(ctx context.Context, path string) (recordingProbeResult, error) {
	args := []string{
		"-v", "error", "-select_streams", "v:0",
		"-show_entries", "format=start_time,duration:format_tags=creation_time:stream=codec_name,time_base,start_time,duration:frame=key_frame,best_effort_timestamp_time,pkt_pos,pkt_size",
		"-of", "json", path,
	}
	cmd := exec.CommandContext(ctx, "ffprobe", args...)
	data, err := cmd.Output()
	if err != nil {
		return recordingProbeResult{}, fmt.Errorf("%w: %w", ErrRecordingProbe, err)
	}
	return parseRecordingProbeOutput(data)
}

func parseRecordingProbeOutput(data []byte) (recordingProbeResult, error) {
	var output ffprobeOutput
	if err := json.Unmarshal(data, &output); err != nil {
		return recordingProbeResult{}, fmt.Errorf("%w: invalid JSON: %w", ErrRecordingProbe, err)
	}
	if len(output.Streams) == 0 {
		return recordingProbeResult{}, fmt.Errorf("%w: no video stream", ErrRecordingProbe)
	}
	stream := output.Streams[0]
	result := recordingProbeResult{Codec: normalizeRecordingCodec(stream.CodecName)}
	if parts := strings.Split(stream.TimeBase, "/"); len(parts) == 2 {
		if denominator, err := strconv.ParseUint(parts[1], 10, 32); err == nil {
			result.Timescale = uint32(denominator)
		}
	}
	result.Frames = len(output.Frames)
	for _, frame := range output.Frames {
		if frame.KeyFrame == 1 {
			result.Keyframes++
			at, err := parseNonNegativeFloat(frame.Time)
			if err != nil {
				return recordingProbeResult{}, fmt.Errorf("%w: invalid keyframe time", ErrRecordingProbe)
			}
			offset, err := parseNonNegativeInt(frame.Offset)
			if err != nil {
				return recordingProbeResult{}, fmt.Errorf("%w: invalid keyframe offset", ErrRecordingProbe)
			}
			size, err := parsePositiveInt(frame.Size)
			if err != nil {
				return recordingProbeResult{}, fmt.Errorf("%w: invalid keyframe size", ErrRecordingProbe)
			}
			result.KeyframeAt = append(result.KeyframeAt, recordingKeyframe{TimeMS: int64(at * 1000), Offset: offset, Size: size})
		}
	}
	base, hasBase := parseCreationTime(output.Format.Tags["creation_time"])
	startSeconds, startOK := parseSeconds(output.Format.StartTime)
	if !startOK {
		startSeconds, startOK = parseSeconds(stream.StartTime)
	}
	if startOK {
		if hasBase && startSeconds < 1e9 {
			result.StartedAt = base.Add(time.Duration(startSeconds * float64(time.Second)))
		} else if startSeconds >= 1e9 {
			result.StartedAt = time.Unix(int64(startSeconds), int64((startSeconds-float64(int64(startSeconds)))*1e9))
		}
	}
	duration, durationOK := parseSeconds(output.Format.Duration)
	if !durationOK {
		duration, durationOK = parseSeconds(stream.Duration)
	}
	if result.StartedAt.IsZero() || !durationOK || duration <= 0 {
		return recordingProbeResult{}, fmt.Errorf("%w: missing absolute timing", ErrRecordingProbe)
	}
	result.EndedAt = result.StartedAt.Add(time.Duration(duration * float64(time.Second)))
	if err := validateProbeResult(result); err != nil {
		return recordingProbeResult{}, fmt.Errorf("%w: invalid metadata", ErrRecordingProbe)
	}
	return result, nil
}

func normalizeRecordingCodec(codec string) string {
	switch strings.ToLower(strings.TrimSpace(codec)) {
	case "h264", "avc":
		return string(cascade.FormatH264)
	case "h265", "hevc":
		return string(cascade.FormatH265)
	default:
		return ""
	}
}

func parseNonNegativeFloat(value string) (float64, error) {
	v, ok := parseSeconds(value)
	if !ok || math.IsNaN(v) || math.IsInf(v, 0) || v < 0 {
		return 0, errors.New("not a finite non-negative number")
	}
	return v, nil
}

func parseNonNegativeInt(value string) (int64, error) {
	v, err := strconv.ParseInt(value, 10, 64)
	if err != nil || v < 0 {
		return 0, errors.New("not a non-negative integer")
	}
	return v, nil
}

func parsePositiveInt(value string) (int64, error) {
	v, err := strconv.ParseInt(value, 10, 64)
	if err != nil || v <= 0 {
		return 0, errors.New("not a positive integer")
	}
	return v, nil
}

func parseSeconds(value string) (float64, bool) {
	if value == "" || value == "N/A" {
		return 0, false
	}
	v, err := strconv.ParseFloat(value, 64)
	return v, err == nil
}

func parseCreationTime(value string) (time.Time, bool) {
	if value == "" {
		return time.Time{}, false
	}
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02 15:04:05Z07:00"} {
		if parsed, err := time.Parse(layout, value); err == nil {
			return parsed, true
		}
	}
	return time.Time{}, false
}

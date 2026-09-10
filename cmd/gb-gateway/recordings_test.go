package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mickeyzzc/gb28181-go/platform/cascade"
)

func TestRecordingStoreIndexesOnlyAfterTwoStableScans(t *testing.T) {
	root := t.TempDir()
	now := time.Date(2026, 9, 11, 12, 0, 0, 0, time.FixedZone("CST", 8*60*60))
	path := filepath.Join(root, "front", now.Format("20060102"), "front_120000.mp4")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("segment"), 0o644); err != nil {
		t.Fatal(err)
	}

	probes := 0
	store, err := newRecordingStore(filepath.Join(root, "recordings.jsonl"), root,
		func(context.Context, string) (recordingProbeResult, error) {
			probes++
			return recordingProbeResult{
				Codec: "h265", Timescale: 90000, Frames: 2, Keyframes: 1,
				KeyframeAt: []recordingKeyframe{{TimeMS: 0, Offset: 12, Size: 4}},
				StartedAt:  now.Add(-time.Second), EndedAt: now,
			}, nil
		})
	if err != nil {
		t.Fatal(err)
	}

	if err := store.scanAt(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	filter := cascade.RecordingFilter{CameraID: "front", StartTime: now.Add(-time.Minute), EndTime: now.Add(time.Minute)}
	if got, err := store.ListRecordings(context.Background(), filter); err != nil {
		t.Fatal(err)
	} else if len(got) != 0 {
		t.Fatalf("first scan indexed %d recordings, want 0", len(got))
	}

	if err := store.scanAt(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	got, err := store.ListRecordings(context.Background(), filter)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].FilePath != path || got[0].Format != "h265" {
		t.Fatalf("recordings = %#v, want one indexed recording", got)
	}
	if probes != 1 {
		t.Fatalf("probe calls = %d, want 1", probes)
	}
}

func TestRecordingStoreReloadsWithoutDuplicateAndIgnoresCrashTail(t *testing.T) {
	root := t.TempDir()
	now := time.Date(2026, 9, 11, 12, 0, 0, 0, time.FixedZone("CST", 8*60*60))
	path := filepath.Join(root, "front", now.Format("20060102"), "front_120000.mp4")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("segment"), 0o644); err != nil {
		t.Fatal(err)
	}
	indexPath := filepath.Join(root, "recordings.jsonl")
	probe := func(context.Context, string) (recordingProbeResult, error) {
		return recordingProbeResult{
			Codec: "h264", Timescale: 90000, Frames: 1, Keyframes: 1,
			KeyframeAt: []recordingKeyframe{{TimeMS: 0, Offset: 0, Size: 7}},
			StartedAt:  now, EndedAt: now.Add(time.Minute),
		}, nil
	}
	store, err := newRecordingStore(indexPath, root, probe)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.scanAt(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	if err := store.scanAt(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(indexPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(indexPath, append(data, []byte(`{"version":1,"op":"upsert"`)...), 0o600); err != nil {
		t.Fatal(err)
	}

	restarted, err := newRecordingStore(indexPath, root, probe)
	if err != nil {
		t.Fatal(err)
	}
	got, err := restarted.ListRecordings(context.Background(), cascade.RecordingFilter{
		CameraID: "front", StartTime: now.Add(-time.Minute), EndTime: now.Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("reloaded recordings = %#v, want one complete event", got)
	}
}

func TestRecordingStoreQueryIncludesOverlapSortsAndLimits(t *testing.T) {
	root := t.TempDir()
	now := time.Date(2026, 9, 11, 12, 0, 0, 0, time.FixedZone("CST", 8*60*60))
	day := now.Format("20060102")
	files := []struct {
		name  string
		start time.Time
	}{
		{"front_b.mp4", now.Add(10 * time.Minute)},
		{"front_a.mp4", now.Add(10 * time.Minute)},
		{"front_old.mp4", now.Add(-10 * time.Minute)},
	}
	for _, file := range files {
		path := filepath.Join(root, "front", day, file.name)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(file.name), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	// A different date is deliberately ignored by the daily scanner.
	yesterday := filepath.Join(root, "front", now.Add(-24*time.Hour).Format("20060102"), "old.mp4")
	if err := os.MkdirAll(filepath.Dir(yesterday), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(yesterday, []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}

	probe := func(_ context.Context, path string) (recordingProbeResult, error) {
		name := filepath.Base(path)
		for _, file := range files {
			if file.name == name {
				end := file.start.Add(time.Minute)
				if name == "front_old.mp4" {
					end = file.start.Add(15 * time.Minute)
				}
				return recordingProbeResult{
					Codec: "h265", Timescale: 90000, Frames: 1, Keyframes: 1,
					KeyframeAt: []recordingKeyframe{{TimeMS: 0, Offset: 0, Size: 1}},
					StartedAt:  file.start, EndedAt: end,
				}, nil
			}
		}
		return recordingProbeResult{}, errors.New("unexpected file")
	}
	store, err := newRecordingStore(filepath.Join(root, "recordings.jsonl"), root, probe)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := store.scanAt(context.Background(), now); err != nil {
			t.Fatal(err)
		}
	}

	got, err := store.ListRecordings(context.Background(), cascade.RecordingFilter{
		CameraID: "front", StartTime: now, EndTime: now.Add(11 * time.Minute), Limit: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("recordings = %#v, want limited overlap results", got)
	}
	if filepath.Base(got[0].FilePath) != "front_old.mp4" || filepath.Base(got[1].FilePath) != "front_a.mp4" {
		t.Fatalf("recording order = %q, %q", filepath.Base(got[0].FilePath), filepath.Base(got[1].FilePath))
	}
}

func TestRecordingStoreTombstoneAndCompactAreAtomic(t *testing.T) {
	root := t.TempDir()
	now := time.Date(2026, 9, 11, 12, 0, 0, 0, time.FixedZone("CST", 8*60*60))
	path := filepath.Join(root, "front", now.Format("20060102"), "front.mp4")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("segment"), 0o644); err != nil {
		t.Fatal(err)
	}
	indexPath := filepath.Join(root, "recordings.jsonl")
	store, err := newRecordingStore(indexPath, root, func(context.Context, string) (recordingProbeResult, error) {
		return recordingProbeResult{
			Codec: "h264", Timescale: 90000, Frames: 1, Keyframes: 1,
			KeyframeAt: []recordingKeyframe{{TimeMS: 0, Offset: 0, Size: 7}},
			StartedAt:  now, EndedAt: now.Add(time.Minute),
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := store.scanAt(context.Background(), now); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := store.scanAt(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	if got, err := store.ListRecordings(context.Background(), cascade.RecordingFilter{CameraID: "front"}); err != nil {
		t.Fatal(err)
	} else if len(got) != 0 {
		t.Fatalf("recordings after tombstone = %#v", got)
	}
	if err := store.Compact(context.Background()); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(indexPath)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), `"op":"delete"`) {
		t.Fatalf("compacted index still contains tombstone: %s", data)
	}
	restarted, err := newRecordingStore(indexPath, root, store.probe)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := restarted.ListRecordings(context.Background(), cascade.RecordingFilter{CameraID: "front"}); err != nil {
		t.Fatal(err)
	} else if len(got) != 0 {
		t.Fatalf("reloaded recordings after compact = %#v", got)
	}
}

func TestRecordingStoreConcurrentCleanupDoesNotAffectOtherResults(t *testing.T) {
	root := t.TempDir()
	now := time.Date(2026, 9, 11, 12, 0, 0, 0, time.FixedZone("CST", 8*60*60))
	day := now.Format("20060102")
	for _, name := range []string{"one.mp4", "two.mp4"} {
		path := filepath.Join(root, "front", day, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(name), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	store, err := newRecordingStore(filepath.Join(root, "recordings.jsonl"), root, func(_ context.Context, path string) (recordingProbeResult, error) {
		start := now
		if filepath.Base(path) == "two.mp4" {
			start = now.Add(time.Minute)
		}
		return recordingProbeResult{
			Codec: "h265", Timescale: 90000, Frames: 1, Keyframes: 1,
			KeyframeAt: []recordingKeyframe{{TimeMS: 0, Offset: 0, Size: 1}},
			StartedAt:  start, EndedAt: start.Add(time.Minute),
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := store.scanAt(context.Background(), now); err != nil {
			t.Fatal(err)
		}
	}
	oneID := filepath.ToSlash(filepath.Join("front", day, "one.mp4"))
	twoID := filepath.ToSlash(filepath.Join("front", day, "two.mp4"))
	if err := os.Remove(filepath.Join(root, filepath.FromSlash(oneID))); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	cleanerDone := make(chan error, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		cleanerDone <- store.Remove(context.Background(), oneID)
	}()
	filter := cascade.RecordingFilter{CameraID: "front", StartTime: now.Add(-time.Hour), EndTime: now.Add(time.Hour)}
	for i := 0; i < 100; i++ {
		got, err := store.ListRecordings(context.Background(), filter)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) > 2 {
			t.Fatalf("concurrent query returned duplicate results: %#v", got)
		}
		for _, recording := range got {
			if recording.ID != oneID && recording.ID != twoID {
				t.Fatalf("unexpected recording during cleanup: %#v", recording)
			}
		}
	}
	wg.Wait()
	if err := <-cleanerDone; err != nil {
		t.Fatal(err)
	}
	got, err := store.ListRecordings(context.Background(), filter)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].ID != twoID {
		t.Fatalf("remaining recordings = %#v, want %s", got, twoID)
	}
}

func TestProbeRecordingUsesArgumentAndReadsMetadata(t *testing.T) {
	dir := t.TempDir()
	ffprobe := filepath.Join(dir, "ffprobe")
	output := `{"format":{"start_time":"0","duration":"2","tags":{"creation_time":"2026-09-11T04:00:00Z"}},"streams":[{"codec_name":"h265","time_base":"1/90000","start_time":"0","duration":"2"}],"frames":[{"key_frame":1,"best_effort_timestamp_time":"0","pkt_pos":"100","pkt_size":"42"},{"key_frame":0,"best_effort_timestamp_time":"1","pkt_pos":"142","pkt_size":"20"}]}`
	if err := os.WriteFile(ffprobe, []byte("#!/bin/sh\nprintf '%s' '"+output+"'\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	path := filepath.Join(dir, "clip;not-a-command.mp4")
	got, err := probeRecording(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	if got.Codec != "h265" || got.Timescale != 90000 || got.Frames != 2 || got.Keyframes != 1 {
		t.Fatalf("probe metadata = %#v", got)
	}
	if !got.StartedAt.Equal(time.Date(2026, 9, 11, 4, 0, 0, 0, time.UTC)) || !got.EndedAt.Equal(got.StartedAt.Add(2*time.Second)) {
		t.Fatalf("probe timing = %s..%s", got.StartedAt, got.EndedAt)
	}
}

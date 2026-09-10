package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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

func TestRecordingStoreDoesNotPublishProbeForChangedTarget(t *testing.T) {
	for _, mutate := range []struct {
		name string
		fn   func(string) error
	}{
		{"growth", func(path string) error { return os.WriteFile(path, []byte("changed-size"), 0o644) }},
		{"replacement", func(path string) error {
			if err := os.Remove(path); err != nil {
				return err
			}
			if err := os.WriteFile(path, []byte("segment"), 0o644); err != nil {
				return err
			}
			stamp := time.Unix(2_000_000_000, 0)
			return os.Chtimes(path, stamp, stamp)
		}},
		{"deletion", os.Remove},
	} {
		t.Run(mutate.name, func(t *testing.T) {
			root := t.TempDir()
			now := time.Date(2026, 9, 11, 12, 0, 0, 0, time.FixedZone("CST", 8*60*60))
			path := filepath.Join(root, "front", now.Format("20060102"), "front.mp4")
			if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, []byte("segment"), 0o644); err != nil {
				t.Fatal(err)
			}
			started := make(chan struct{})
			release := make(chan struct{})
			var probes int
			probe := func(_ context.Context, _ string) (recordingProbeResult, error) {
				probes++
				if probes == 1 {
					close(started)
					<-release
				}
				start := now
				if probes > 1 {
					start = now.Add(time.Minute)
				}
				return recordingProbeResult{
					Codec: "h265", Timescale: 90000, Frames: 1, Keyframes: 1,
					KeyframeAt: []recordingKeyframe{{TimeMS: 0, Offset: 0, Size: 7}},
					StartedAt:  start, EndedAt: start.Add(time.Minute),
				}, nil
			}
			store, err := newRecordingStore(filepath.Join(root, "recordings.jsonl"), root, probe)
			if err != nil {
				t.Fatal(err)
			}
			if err := store.scanAt(context.Background(), now); err != nil {
				t.Fatal(err)
			}
			scanDone := make(chan error, 1)
			go func() { scanDone <- store.scanAt(context.Background(), now) }()
			<-started
			if err := mutate.fn(path); err != nil {
				t.Fatal(err)
			}
			close(release)
			if err := <-scanDone; err != nil {
				t.Fatal(err)
			}
			filter := cascade.RecordingFilter{CameraID: "front", StartTime: now.Add(-time.Minute), EndTime: now.Add(time.Hour)}
			got, err := store.ListRecordings(context.Background(), filter)
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != 0 {
				t.Fatalf("changed target published stale metadata: %#v", got)
			}
			if mutate.name == "deletion" {
				return
			}
			if err := store.scanAt(context.Background(), now); err != nil {
				t.Fatal(err)
			}
			got, err = store.ListRecordings(context.Background(), filter)
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != 0 {
				t.Fatalf("changed target indexed before second stable scan: %#v", got)
			}
			if err := store.scanAt(context.Background(), now); err != nil {
				t.Fatal(err)
			}
			got, err = store.ListRecordings(context.Background(), filter)
			if err != nil {
				t.Fatal(err)
			}
			if len(got) != 1 || !got[0].StartedAt.Equal(now.Add(time.Minute)) {
				t.Fatalf("recording after two new stable scans = %#v", got)
			}
		})
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
	if err := restarted.Remove(context.Background(), got[0].ID); err != nil {
		t.Fatal(err)
	}
	restartedAgain, err := newRecordingStore(indexPath, root, probe)
	if err != nil {
		t.Fatal(err)
	}
	got, err = restartedAgain.ListRecordings(context.Background(), cascade.RecordingFilter{
		CameraID: "front", StartTime: now.Add(-time.Minute), EndTime: now.Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("recordings after restart, append, restart = %#v", got)
	}
}

func TestRecordingStoreRejectsCompleteMiddleBadLine(t *testing.T) {
	root := t.TempDir()
	indexPath := filepath.Join(root, "recordings.jsonl")
	valid := `{"version":1,"op":"upsert","id":"front/20260911/front.mp4","camera_id":"front","file":"front/20260911/front.mp4","format":"h264","size":7,"started_at":"2026-09-11T04:00:00Z","ended_at":"2026-09-11T04:01:00Z","duration":60,"timescale":90000,"frames":1,"keyframes":[{"time_ms":0,"offset":0,"size":7}]}`
	if err := os.WriteFile(indexPath, []byte(valid+"\nnot-json\n"+valid+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := newRecordingStore(indexPath, root, func(context.Context, string) (recordingProbeResult, error) {
		return recordingProbeResult{}, errors.New("must not probe corrupt index")
	}); !errors.Is(err, ErrRecordingIndexCorrupt) {
		t.Fatalf("newRecordingStore() error = %v, want corrupt index", err)
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

	var probedPaths []string
	probe := func(_ context.Context, path string) (recordingProbeResult, error) {
		probedPaths = append(probedPaths, path)
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
	for _, path := range probedPaths {
		if path == yesterday {
			t.Fatalf("yesterday recording was probed: %s", path)
		}
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
	for _, name := range []string{"one.mp4", "two.mp4", "three.mp4"} {
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
		} else if filepath.Base(path) == "three.mp4" {
			start = now.Add(2 * time.Minute)
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

	snapshotEntered := make(chan struct{})
	releaseSnapshot := make(chan struct{})
	store.snapshotStep = func() {
		close(snapshotEntered)
		<-releaseSnapshot
	}
	queryResult := make(chan struct {
		got []cascade.Recording
		err error
	}, 1)
	go func() {
		got, err := store.ListRecordings(context.Background(), cascade.RecordingFilter{
			CameraID: "front", StartTime: now.Add(-time.Hour), EndTime: now.Add(time.Hour),
		})
		queryResult <- struct {
			got []cascade.Recording
			err error
		}{got: got, err: err}
	}()
	<-snapshotEntered
	if store.mu.TryLock() {
		store.mu.Unlock()
		t.Fatal("ListRecordings did not hold its read snapshot lock")
	}
	removeAttempted := make(chan struct{})
	store.removeBeforeLock = func() { close(removeAttempted) }
	cleanerDone := make(chan error, 1)
	go func() {
		cleanerDone <- store.Remove(context.Background(), oneID)
	}()
	<-removeAttempted
	select {
	case err := <-cleanerDone:
		t.Fatalf("Remove completed while ListRecordings held its snapshot: %v", err)
	default:
	}
	close(releaseSnapshot)
	result := <-queryResult
	if result.err != nil {
		t.Fatal(result.err)
	}
	if err := assertRecordingIDs(result.got, oneID, twoID, filepath.ToSlash(filepath.Join("front", day, "three.mp4"))); err != nil {
		if errAfter := assertRecordingIDs(result.got, twoID, filepath.ToSlash(filepath.Join("front", day, "three.mp4"))); errAfter != nil {
			t.Fatalf("overlapping query returned neither before nor after snapshot: %v; after check: %v; got %#v", err, errAfter, result.got)
		}
	}
	if err := <-cleanerDone; err != nil {
		t.Fatal(err)
	}
	store.snapshotStep = nil
	store.removeBeforeLock = nil
	filter := cascade.RecordingFilter{CameraID: "front", StartTime: now.Add(-time.Hour), EndTime: now.Add(time.Hour)}
	got, err := store.ListRecordings(context.Background(), filter)
	if err != nil {
		t.Fatal(err)
	}
	if err := assertRecordingIDs(got, twoID, filepath.ToSlash(filepath.Join("front", day, "three.mp4"))); err != nil {
		t.Fatalf("remaining recordings: %v; got %#v", err, got)
	}
}

func assertRecordingIDs(got []cascade.Recording, want ...string) error {
	if len(got) != len(want) {
		return fmt.Errorf("got %d recordings, want %d", len(got), len(want))
	}
	wantSet := make(map[string]struct{}, len(want))
	for _, id := range want {
		if _, exists := wantSet[id]; exists {
			return fmt.Errorf("duplicate expected ID %s", id)
		}
		wantSet[id] = struct{}{}
	}
	for _, recording := range got {
		if _, exists := wantSet[recording.ID]; !exists {
			return fmt.Errorf("unexpected ID %s", recording.ID)
		}
		delete(wantSet, recording.ID)
	}
	if len(wantSet) != 0 {
		return fmt.Errorf("missing IDs %v", wantSet)
	}
	return nil
}

func TestProbeRecordingUsesArgumentAndReadsMetadata(t *testing.T) {
	dir := t.TempDir()
	ffprobe := filepath.Join(dir, "ffprobe")
	argsPath := filepath.Join(dir, "args.log")
	output := `{"format":{"start_time":"0","duration":"2","tags":{"creation_time":"2026-09-11T04:00:00Z"}},"streams":[{"codec_name":"hevc","time_base":"1/90000","start_time":"0","duration":"2"}],"frames":[{"key_frame":1,"best_effort_timestamp_time":"0","pkt_pos":"100","pkt_size":"42"},{"key_frame":0,"best_effort_timestamp_time":"1","pkt_pos":"142","pkt_size":"20"}]}`
	script := "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$FFPROBE_ARGS_LOG\"\nprintf '%s' '" + output + "'\n"
	if err := os.WriteFile(ffprobe, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("FFPROBE_ARGS_LOG", argsPath)
	path := filepath.Join(dir, "clip with spaces;not-a-command.mp4")
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
	args, err := os.ReadFile(argsPath)
	if err != nil {
		t.Fatal(err)
	}
	argv := strings.Split(strings.TrimSuffix(string(args), "\n"), "\n")
	if len(argv) < 1 || argv[len(argv)-1] != path {
		t.Fatalf("ffprobe argv tail = %q, want literal %q", argv, path)
	}
	h264Output := strings.Replace(output, `"codec_name":"hevc"`, `"codec_name":"h264"`, 1)
	if h264, err := parseRecordingProbeOutput([]byte(h264Output)); err != nil {
		t.Fatalf("parse h264 probe output: %v", err)
	} else if h264.Codec != "h264" {
		t.Fatalf("h264 codec = %q, want h264", h264.Codec)
	}
}

func TestRecordingCodecNormalizationKeepsH264AndAcceptsHEVCNames(t *testing.T) {
	for _, test := range []struct {
		input, want string
	}{
		{"h264", "h264"}, {"avc", "h264"}, {"hevc", "h265"}, {"h265", "h265"},
	} {
		if got := normalizeRecordingCodec(test.input); got != test.want {
			t.Errorf("normalizeRecordingCodec(%q) = %q, want %q", test.input, got, test.want)
		}
	}
}

func TestRecordingProbeRejectsInvalidKeyframeMetadata(t *testing.T) {
	base := `{"format":{"start_time":"0","duration":"2","tags":{"creation_time":"2026-09-11T04:00:00Z"}},"streams":[{"codec_name":"h264","time_base":"1/90000","start_time":"0","duration":"2"}],"frames":[{"key_frame":1,"best_effort_timestamp_time":"0","pkt_pos":"100","pkt_size":"42"}]}`
	for _, test := range []struct {
		name, frame string
	}{
		{"bad time", `{"key_frame":1,"best_effort_timestamp_time":"nope","pkt_pos":"100","pkt_size":"42"}`},
		{"N/A time", `{"key_frame":1,"best_effort_timestamp_time":"N/A","pkt_pos":"100","pkt_size":"42"}`},
		{"negative time", `{"key_frame":1,"best_effort_timestamp_time":"-0.001","pkt_pos":"100","pkt_size":"42"}`},
		{"NaN time", `{"key_frame":1,"best_effort_timestamp_time":"NaN","pkt_pos":"100","pkt_size":"42"}`},
		{"Inf time", `{"key_frame":1,"best_effort_timestamp_time":"Inf","pkt_pos":"100","pkt_size":"42"}`},
		{"negative offset", `{"key_frame":1,"best_effort_timestamp_time":"0","pkt_pos":"-1","pkt_size":"42"}`},
		{"negative size", `{"key_frame":1,"best_effort_timestamp_time":"0","pkt_pos":"100","pkt_size":"-1"}`},
		{"missing keyframe", `{"key_frame":0,"best_effort_timestamp_time":"0","pkt_pos":"100","pkt_size":"42"}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			data := strings.Replace(base, `{"key_frame":1,"best_effort_timestamp_time":"0","pkt_pos":"100","pkt_size":"42"}`, test.frame, 1)
			if _, err := parseRecordingProbeOutput([]byte(data)); !errors.Is(err, ErrRecordingProbe) {
				t.Fatalf("parseRecordingProbeOutput() error = %v, want %v", err, ErrRecordingProbe)
			}
		})
	}
}

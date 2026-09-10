package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/mickeyzzc/gb28181-go/platform/cascade"
)

func TestChannelStorePersistsCascadeChannelAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "channels.json")
	channel := cascade.CascadeChannel{
		CameraID:    "front",
		GBChannelID: "34020000001320000001",
		Name:        "Front",
	}

	store, err := NewChannelStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertCascadeChannel(context.Background(), channel); err != nil {
		t.Fatal(err)
	}

	restarted, err := NewChannelStore(path)
	if err != nil {
		t.Fatal(err)
	}
	got, err := restarted.ListCascadeChannels(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].CameraID != channel.CameraID || got[0].GBChannelID != channel.GBChannelID {
		t.Fatalf("channels = %#v, want %#v", got, []cascade.CascadeChannel{channel})
	}
}

func TestChannelStoreReadsPreviousVersionAndMigratesOnWrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "channels.json")
	legacy := `{"version":1,"channels":[{"camera_id":"front","gb_channel_id":"34020000001320000001","name":"Front"}]}`
	if err := os.WriteFile(path, []byte(legacy), 0600); err != nil {
		t.Fatal(err)
	}

	store, err := NewChannelStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertCascadeChannel(context.Background(), cascade.CascadeChannel{
		CameraID: "front", GBChannelID: "34020000001320000001", Name: "Renamed",
	}); err != nil {
		t.Fatal(err)
	}
	var file struct {
		Version int `json:"version"`
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &file); err != nil {
		t.Fatal(err)
	}
	if file.Version != channelStoreVersion {
		t.Fatalf("version = %d, want %d", file.Version, channelStoreVersion)
	}
}

func TestChannelStoreRejectsUnknownVersionWithoutOverwriting(t *testing.T) {
	path := filepath.Join(t.TempDir(), "channels.json")
	data := []byte(`{"version":99,"channels":[]}`)
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	_, err := NewChannelStore(path)
	if !errors.Is(err, ErrChannelStoreVersion) {
		t.Fatalf("error = %v, want unsupported version", err)
	}
	got, readErr := os.ReadFile(path)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if string(got) != string(data) {
		t.Fatalf("unknown-version file changed to %q", got)
	}
}

func TestChannelStoreIgnoresTemporaryFileFromInterruptedWrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "channels.json")
	valid := `{"version":2,"channels":[{"camera_id":"front","gb_channel_id":"34020000001320000001"}]}`
	if err := os.WriteFile(path, []byte(valid), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, ".channels.json.tmp-crashed"), []byte(`{"version":2,"channels":[]}`), 0600); err != nil {
		t.Fatal(err)
	}

	store, err := NewChannelStore(path)
	if err != nil {
		t.Fatal(err)
	}
	channels, err := store.ListCascadeChannels(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(channels) != 1 || channels[0].CameraID != "front" {
		t.Fatalf("channels = %#v, want interrupted write ignored", channels)
	}
}

func TestChannelStoreRejectsCorruptRows(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{"malformed JSON", `{"version":2`},
		{"half-written row", `{"version":2,"channels":[{"camera_id":"front","gb_channel_id":"340200000013200000`},
		{"trailing JSON", `{"version":2,"channels":[]} {}`},
		{"missing channels", `{"version":2}`},
		{"null channels", `{"version":2,"channels":null}`},
		{"duplicate local camera", `{"version":2,"channels":[{"camera_id":"front","gb_channel_id":"34020000001320000001"},{"camera_id":"front","gb_channel_id":"34020000001320000002"}]}`},
		{"duplicate GB channel ID", `{"version":2,"channels":[{"camera_id":"front","gb_channel_id":"34020000001320000001"},{"camera_id":"back","gb_channel_id":"34020000001320000001"}]}`},
		{"invalid GB channel ID", `{"version":2,"channels":[{"camera_id":"front","gb_channel_id":"not-an-id"}]}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "channels.json")
			if err := os.WriteFile(path, []byte(tt.body), 0600); err != nil {
				t.Fatal(err)
			}
			_, err := NewChannelStore(path)
			if !errors.Is(err, ErrChannelStoreCorrupt) {
				t.Fatalf("error = %v, want corrupt store", err)
			}
		})
	}
}

func TestChannelStoreKeepsLocalCameraMappingStableAndRejectsConflicts(t *testing.T) {
	store, err := NewChannelStore(filepath.Join(t.TempDir(), "channels.json"))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	original := cascade.CascadeChannel{CameraID: "front", GBChannelID: "34020000001320000001", Name: "Front"}
	if err := store.UpsertCascadeChannel(ctx, original); err != nil {
		t.Fatal(err)
	}
	if err := store.UpsertCascadeChannel(ctx, cascade.CascadeChannel{
		CameraID: "front", GBChannelID: "34020000001320000002", Name: "Front",
	}); !errors.Is(err, ErrChannelStoreConflict) {
		t.Fatalf("changed mapping error = %v, want conflict", err)
	}
	if err := store.UpsertCascadeChannel(ctx, cascade.CascadeChannel{
		CameraID: "back", GBChannelID: original.GBChannelID, Name: "Back",
	}); !errors.Is(err, ErrChannelStoreConflict) {
		t.Fatalf("duplicate GB ID error = %v, want conflict", err)
	}
	got, err := store.ListCascadeChannels(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Name != original.Name {
		t.Fatalf("channels after conflicts = %#v", got)
	}
}

func TestChannelStoreConcurrentUpsertsHaveUniqueGBChannelMappings(t *testing.T) {
	store, err := NewChannelStore(filepath.Join(t.TempDir(), "channels.json"))
	if err != nil {
		t.Fatal(err)
	}
	const count = 32
	var wg sync.WaitGroup
	errs := make(chan error, count)
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := "3402000000132" + formatSerial(i+1)
			errs <- store.UpsertCascadeChannel(context.Background(), cascade.CascadeChannel{
				CameraID: "camera-" + formatSerial(i+1), GBChannelID: id,
			})
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	channels, err := store.ListCascadeChannels(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(channels) != count {
		t.Fatalf("channel count = %d, want %d", len(channels), count)
	}
	seen := make(map[string]bool, len(channels))
	for _, channel := range channels {
		if seen[channel.GBChannelID] {
			t.Fatalf("duplicate GB channel ID %q", channel.GBChannelID)
		}
		seen[channel.GBChannelID] = true
	}
}

func TestChannelStoreAllocatesUniqueGBChannelsConcurrently(t *testing.T) {
	store, err := NewChannelStore(filepath.Join(t.TempDir(), "channels.json"))
	if err != nil {
		t.Fatal(err)
	}
	const count = 32
	var wg sync.WaitGroup
	results := make(chan cascade.CascadeChannel, count)
	errs := make(chan error, count)
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			channel, err := store.AllocateCascadeChannel(context.Background(), "camera-"+formatSerial(i+1), "3402000000132", "")
			if err != nil {
				errs <- err
				return
			}
			results <- channel
		}(i)
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	seen := make(map[string]bool, count)
	for channel := range results {
		if seen[channel.GBChannelID] {
			t.Fatalf("duplicate allocated GB channel ID %q", channel.GBChannelID)
		}
		seen[channel.GBChannelID] = true
	}
	if len(seen) != count {
		t.Fatalf("allocated channel count = %d, want %d", len(seen), count)
	}
}

func TestChannelStoreWriteFailureDoesNotPublishMapping(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "channels.json")
	store, err := NewChannelStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(dir); err != nil {
		t.Fatal(err)
	}
	err = store.UpsertCascadeChannel(context.Background(), cascade.CascadeChannel{
		CameraID: "front", GBChannelID: "34020000001320000001",
	})
	if err == nil || !strings.Contains(err.Error(), "temporary file") {
		t.Fatalf("write error = %v, want temporary file error", err)
	}
	channels, err := store.ListCascadeChannels(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(channels) != 0 {
		t.Fatalf("channels after failed write = %#v", channels)
	}
}

func formatSerial(n int) string {
	return fmt.Sprintf("%07d", n)
}

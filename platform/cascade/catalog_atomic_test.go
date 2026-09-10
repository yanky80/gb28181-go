package cascade

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/mickeyzzc/gb28181-go/manscdp"
	"github.com/stretchr/testify/require"
)

type failingCatalogStore struct{}

func (failingCatalogStore) UpsertCascadeChannel(context.Context, CascadeChannel) error {
	return errors.New("disk full")
}

func (failingCatalogStore) ListCascadeChannels(context.Context) ([]CascadeChannel, error) {
	return nil, nil
}

func (failingCatalogStore) ListRecordings(context.Context, RecordingFilter) ([]Recording, error) {
	return nil, nil
}

type atomicCatalogStore struct {
	mu       sync.Mutex
	channels map[string]CascadeChannel
	calls    int
}

func (s *atomicCatalogStore) UpsertCascadeChannel(_ context.Context, ch CascadeChannel) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.channels[ch.CameraID] = ch
	return nil
}

func (s *atomicCatalogStore) ListCascadeChannels(_ context.Context) ([]CascadeChannel, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows := make([]CascadeChannel, 0, len(s.channels))
	for _, ch := range s.channels {
		rows = append(rows, ch)
	}
	return rows, nil
}

func (s *atomicCatalogStore) ListRecordings(_ context.Context, _ RecordingFilter) ([]Recording, error) {
	return nil, nil
}

func (s *atomicCatalogStore) AllocateCascadeChannel(_ context.Context, cameraID, prefix, name string) (CascadeChannel, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ch, ok := s.channels[cameraID]; ok {
		return ch, nil
	}
	s.calls++
	ch := CascadeChannel{
		CameraID:    cameraID,
		GBChannelID: fmt.Sprintf("%s%07d", prefix, s.calls),
		Name:        name,
		UpdatedAt:   time.Now(),
	}
	s.channels[cameraID] = ch
	return ch, nil
}

func (s *atomicCatalogStore) stats() (int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls, len(s.channels)
}

func TestCatalogItemsUsesAtomicAllocatorForConcurrentRequests(t *testing.T) {
	store := &atomicCatalogStore{channels: make(map[string]CascadeChannel)}
	svc := New(testCfg(), fakeSource{cams: []CameraInfo{{ID: "front"}, {ID: "back"}}}, store)

	const requests = 16
	var wg sync.WaitGroup
	errCh := make(chan error, requests)
	itemsCh := make(chan []manscdp.Item, requests)
	for i := 0; i < requests; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			items, err := svc.catalogItems()
			if err != nil {
				errCh <- err
				return
			}
			itemsCh <- items
		}()
	}
	wg.Wait()
	close(errCh)
	close(itemsCh)
	for err := range errCh {
		t.Fatal(err)
	}

	var first map[string]bool
	for items := range itemsCh {
		require.Len(t, items, 2)
		ids := map[string]bool{items[0].DeviceID: true, items[1].DeviceID: true}
		if first == nil {
			first = ids
		} else {
			require.Equal(t, first, ids)
		}
	}
	calls, channels := store.stats()
	require.Equal(t, 2, calls, "one atomic allocation per local camera")
	require.Equal(t, 2, channels)
}

func TestCatalogItemsReturnsChannelStoreWriteError(t *testing.T) {
	svc := New(testCfg(), fakeSource{cams: []CameraInfo{{ID: "front"}}}, failingCatalogStore{})

	_, err := svc.catalogItems()
	require.ErrorContains(t, err, "persist GB channel allocation")
}

func TestCatalogItemsRejectsInvalidPersistedGBChannelID(t *testing.T) {
	store := &atomicCatalogStore{channels: map[string]CascadeChannel{
		"front": {CameraID: "front", GBChannelID: "short"},
	}}
	svc := New(testCfg(), fakeSource{cams: []CameraInfo{{ID: "front"}}}, store)

	_, err := svc.catalogItems()
	require.ErrorContains(t, err, "invalid GB channel ID")
}

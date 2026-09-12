package platform

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestFrameHubFanOut(t *testing.T) {
	h := NewFrameHub()
	h.SetCameraID("cam-1")

	var a, b atomic.Int64
	require.NoError(t, h.Subscribe("a", func(pts int64, au [][]byte, isIDR bool) { a.Add(1) }))
	require.NoError(t, h.Subscribe("b", func(pts int64, au [][]byte, isIDR bool) { b.Add(1) }))

	h.Broadcast(100, [][]byte{{0x65, 0x01}}, true)
	h.Broadcast(200, [][]byte{{0x41, 0x01}}, false)

	require.Eventually(t, func() bool { return a.Load() == 2 && b.Load() == 2 },
		2*time.Second, 5*time.Millisecond, "both consumers must receive both frames")
}

func TestFrameHubDuplicateSubscribe(t *testing.T) {
	h := NewFrameHub()
	require.NoError(t, h.Subscribe("dup", func(pts int64, au [][]byte, isIDR bool) {}))
	err := h.Subscribe("dup", func(pts int64, au [][]byte, isIDR bool) {})
	require.Error(t, err)
}

func TestFrameHubUnsubscribeStopsDelivery(t *testing.T) {
	h := NewFrameHub()
	var n atomic.Int64
	require.NoError(t, h.Subscribe("c", func(pts int64, au [][]byte, isIDR bool) { n.Add(1) }))
	h.Broadcast(1, [][]byte{{1}}, true)
	require.Eventually(t, func() bool { return n.Load() == 1 }, 2*time.Second, 5*time.Millisecond)

	h.Unsubscribe("c")
	h.Broadcast(2, [][]byte{{1}}, true)
	time.Sleep(50 * time.Millisecond)
	require.EqualValues(t, 1, n.Load(), "no frames after Unsubscribe")
	// Unsubscribing an unknown consumer is a no-op.
	h.Unsubscribe("never-subscribed")
}

func TestFrameHubDropOnFull(t *testing.T) {
	h := NewFrameHub()
	block := make(chan struct{})
	inflight := make(chan struct{}, 1)
	var got atomic.Int64
	require.NoError(t, h.Subscribe("slow", func(pts int64, au [][]byte, isIDR bool) {
		got.Add(1)
		select {
		case inflight <- struct{}{}:
		default:
		}
		<-block // consumer blocks: queue fills, further frames drop
	}))
	// Frame 0 lands in the callback; wait until it is provably in-flight so
	// the arithmetic below is deterministic (no scheduling race).
	h.Broadcast(0, [][]byte{{1}}, false)
	<-inflight
	// 200 more: 150 queued (capacity) + 50 dropped.
	for i := 1; i <= 200; i++ {
		h.Broadcast(int64(i), [][]byte{{1}}, false)
	}
	require.EqualValues(t, 50, h.Dropped())
	close(block)
	require.Eventually(t, func() bool { return got.Load() == 151 }, 2*time.Second, 5*time.Millisecond)
	require.EqualValues(t, 50, h.Dropped())
}

func TestFrameHubPassesIDRFlag(t *testing.T) {
	h := NewFrameHub()

	got := make(chan bool, 4)
	require.NoError(t, h.Subscribe("idr-probe", func(_ int64, _ [][]byte, isIDR bool) {
		got <- isIDR
	}))
	defer h.Unsubscribe("idr-probe")

	h.Broadcast(100, [][]byte{{0x65}}, true)
	h.Broadcast(200, [][]byte{{0x41}}, false)

	require.Equal(t, true, <-got, "keyframe flag must reach the consumer")
	require.Equal(t, false, <-got, "non-keyframe flag must reach the consumer")
}

func TestFrameHubCloseReleasesVideoAndAudioConsumers(t *testing.T) {
	h := NewFrameHub()
	video := make(chan struct{}, 1)
	audio := make(chan struct{}, 1)
	require.NoError(t, h.Subscribe("video", func(int64, [][]byte, bool) { video <- struct{}{} }))
	require.NoError(t, h.SubscribeAudio("audio", func(int64, string, []byte) { audio <- struct{}{} }))
	require.Equal(t, 2, h.ConsumerCount())

	h.Broadcast(1, [][]byte{{1}}, false)
	h.BroadcastAudio(1, "g711a", []byte{1})
	select {
	case <-video:
	case <-time.After(time.Second):
		t.Fatal("video consumer did not receive the frame")
	}
	select {
	case <-audio:
	case <-time.After(time.Second):
		t.Fatal("audio consumer did not receive the frame")
	}

	h.Close()
	h.Close()
	require.Equal(t, 0, h.ConsumerCount())
	require.Error(t, h.Subscribe("after-close", func(int64, [][]byte, bool) {}))
	require.Error(t, h.SubscribeAudio("after-close", func(int64, string, []byte) {}))
	h.Broadcast(2, [][]byte{{1}}, false)
	h.BroadcastAudio(2, "g711a", []byte{1})
	select {
	case <-video:
		t.Fatal("closed video consumer received a frame")
	case <-time.After(20 * time.Millisecond):
	}
	select {
	case <-audio:
		t.Fatal("closed audio consumer received a frame")
	case <-time.After(20 * time.Millisecond):
	}
}

func TestFrameHubCloseConvergesWithConcurrentOperations(t *testing.T) {
	h := NewFrameHub()
	var wg sync.WaitGroup
	for i := range 32 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := fmt.Sprintf("video-%d", i)
			_ = h.Subscribe(id, func(int64, [][]byte, bool) {})
			_ = h.SubscribeAudio("audio-"+id, func(int64, string, []byte) {})
			for j := range 20 {
				h.Broadcast(int64(j), [][]byte{{1}}, false)
				h.BroadcastAudio(int64(j), "g711a", []byte{1})
			}
		}(i)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		h.Close()
		h.Close()
	}()
	wg.Wait()
	require.Equal(t, 0, h.ConsumerCount())
	h.Close()
}

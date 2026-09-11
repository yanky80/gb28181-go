package cascade

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/ghettovoice/gosip/sip"
	"github.com/mickeyzzc/gb28181-go/manscdp"
	"github.com/stretchr/testify/require"
)

func TestLoopbackRecordInfoNilStoreReturnsEmptyResponse(t *testing.T) {
	svc, up := startLoopbackService(t, fakeSource{cams: []CameraInfo{{ID: "cam-1", Name: "Front"}}}, nil)
	_, err := svc.catalogItems()
	require.NoError(t, err)

	query, err := manscdp.Encode(manscdp.RecordInfoQuery{
		CmdType: manscdp.CmdRecordInfo, SN: 41, DeviceID: lbChannelOne,
		StartTime: "2026-09-01T00:00:00", EndTime: "2026-09-01T01:00:00",
	})
	require.NoError(t, err)
	res := up.roundTrip(up.request(sip.MESSAGE, lbChannelOne, string(query), "Application/MANSCDP+xml"))
	require.Equal(t, 200, int(res.StatusCode()))

	answer := up.awaitServerRequest(sip.MESSAGE, "<CmdType>RecordInfo</CmdType>")
	_, payload, err := manscdp.Decode([]byte(answer.Body()))
	require.NoError(t, err)
	records, ok := payload.(manscdp.RecordInfo)
	require.True(t, ok)
	require.Equal(t, 41, records.SN)
	require.Equal(t, 0, records.SumNum)
	require.Empty(t, records.RecordList)
}

func TestLoopbackPlaybackCapabilityGates(t *testing.T) {
	tests := []struct {
		name string
		db   Store
		set  func(*Service)
	}{
		{name: "nil store", set: func(*Service) {}},
		{name: "nil parser", db: newFakeCascadeStore(), set: func(s *Service) { s.SetSegmentParser(nil) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc, up := startLoopbackService(t, fakeSource{cams: []CameraInfo{
				{ID: "cam-1", Name: "Front", Encoding: "h264"},
			}}, tt.db)
			tt.set(svc)
			_, err := svc.catalogItems()
			require.NoError(t, err)

			res := up.roundTrip(up.request(sip.INVITE, lbChannelOne, issue14PlaybackSDP(t), "application/sdp"))
			require.Equal(t, 503, int(res.StatusCode()))
			require.Empty(t, playbackIDs(svc))
		})
	}
}

func TestLoopbackPlaybackEmptyWindowAndBadRange(t *testing.T) {
	tests := []struct {
		name string
		body string
		code int
	}{
		{name: "empty window", body: issue14PlaybackSDP(t), code: 404},
		{name: "bad range", body: issue14PlaybackSDPAt(t, time.Unix(100, 0), time.Unix(100, 0)), code: 400},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc, up := startLoopbackService(t, fakeSource{cams: []CameraInfo{
				{ID: "cam-1", Name: "Front", Encoding: "h264"},
			}}, newFakeCascadeStore())
			_, err := svc.catalogItems()
			require.NoError(t, err)
			res := up.roundTrip(up.request(sip.INVITE, lbChannelOne, tt.body, "application/sdp"))
			require.Equal(t, tt.code, int(res.StatusCode()))
		})
	}
}

func TestRecordInfoStoreQueryIsCancelledOnNetworkChange(t *testing.T) {
	db := &blockingRecordStore{fakeCascadeStore: *newFakeCascadeStore(), entered: make(chan struct{}), cancelled: make(chan struct{})}
	svc, up := startLoopbackService(t, fakeSource{cams: []CameraInfo{{ID: "cam-1", Name: "Front"}}}, db)
	_, err := svc.catalogItems()
	require.NoError(t, err)
	query, err := manscdp.Encode(manscdp.RecordInfoQuery{
		CmdType: manscdp.CmdRecordInfo, SN: 44, DeviceID: lbChannelOne,
		StartTime: "2026-09-01T00:00:00", EndTime: "2026-09-01T01:00:00",
	})
	require.NoError(t, err)
	res := up.roundTrip(up.request(sip.MESSAGE, lbChannelOne, string(query), "Application/MANSCDP+xml"))
	require.Equal(t, 200, int(res.StatusCode()))
	select {
	case <-db.entered:
	case <-time.After(time.Second):
		t.Fatal("recording query did not reach the Store")
	}
	svc.NotifyNetworkChange()
	select {
	case <-db.cancelled:
	case <-time.After(time.Second):
		t.Fatal("network change did not cancel the recording query")
	}
	require.NoError(t, svc.Stop())
}

func TestLoopbackPlaybackRejectsInvalidSegmentBeforeOK(t *testing.T) {
	invalid := []struct {
		name string
		seg  *SegmentInfo
	}{
		{name: "timescale", seg: &SegmentInfo{Codec: "h264", Timescale: 0, Samples: []SegmentSample{{Size: 1, Duration: 1}}}},
		{name: "negative offset", seg: &SegmentInfo{Codec: "h264", Timescale: 1000, Samples: []SegmentSample{{Offset: -1, Size: 1, Duration: 1}}}},
		{name: "zero size", seg: &SegmentInfo{Codec: "h264", Timescale: 1000, Samples: []SegmentSample{{Size: 0, Duration: 1}}}},
		{name: "oversize", seg: &SegmentInfo{Codec: "h264", Timescale: 1000, Samples: []SegmentSample{{Size: 16<<20 + 1, Duration: 1}}}},
		{name: "codec", seg: &SegmentInfo{Codec: "h265", Timescale: 1000, Samples: []SegmentSample{{Size: 1, Duration: 1}}}},
	}
	for _, tt := range invalid {
		t.Run(tt.name, func(t *testing.T) {
			db := newFakeCascadeStore()
			svc, up := startLoopbackService(t, fakeSource{cams: []CameraInfo{
				{ID: "cam-1", Name: "Front", Encoding: "h264"},
			}}, db)
			_, err := svc.catalogItems()
			require.NoError(t, err)
			path := filepath.Join(t.TempDir(), "segment.mp4")
			require.NoError(t, db.InsertRecording(context.Background(), &Recording{
				ID: "bad", CameraID: "cam-1", FilePath: path, Format: FormatH264,
				StartedAt: time.Now().Add(-time.Minute), EndedAt: time.Now(),
			}))
			svc.SetSegmentParser(func(string) (*SegmentInfo, error) { return tt.seg, nil })

			res := up.roundTrip(up.request(sip.INVITE, lbChannelOne, issue14PlaybackSDP(t), "application/sdp"))
			require.Equal(t, 488, int(res.StatusCode()))
			require.Empty(t, playbackIDs(svc))
		})
	}
}

func TestLoopbackRecordInfoAsiaShanghaiWindowAndTimes(t *testing.T) {
	loc := time.FixedZone("Asia/Shanghai", 8*60*60)
	db := newFakeCascadeStore()
	svc, up := startLoopbackService(t, fakeSource{cams: []CameraInfo{{ID: "cam-1", Name: "Front"}}}, db)
	svc.SetGBTimezone(loc)
	_, err := svc.catalogItems()
	require.NoError(t, err)
	start := time.Date(2026, 9, 1, 10, 30, 0, 0, time.UTC)
	require.NoError(t, db.InsertRecording(context.Background(), &Recording{
		ID: "shanghai", CameraID: "cam-1", Format: FormatH264,
		StartedAt: start, EndedAt: start.Add(30 * time.Minute),
	}))
	query, err := manscdp.Encode(manscdp.RecordInfoQuery{
		CmdType: manscdp.CmdRecordInfo, SN: 42, DeviceID: lbChannelOne,
		StartTime: "2026-09-01T18:00:00", EndTime: "2026-09-01T19:00:00",
	})
	require.NoError(t, err)
	res := up.roundTrip(up.request(sip.MESSAGE, lbChannelOne, string(query), "Application/MANSCDP+xml"))
	require.Equal(t, 200, int(res.StatusCode()))
	answer := up.awaitServerRequest(sip.MESSAGE, "<CmdType>RecordInfo</CmdType>")
	_, payload, err := manscdp.Decode([]byte(answer.Body()))
	require.NoError(t, err)
	records, ok := payload.(manscdp.RecordInfo)
	require.True(t, ok)
	require.Len(t, records.RecordList, 1)
	require.Equal(t, "2026-09-01T18:30:00", records.RecordList[0].StartTime)
	require.Equal(t, "2026-09-01T19:00:00", records.RecordList[0].EndTime)
}

func TestLoopbackPlaybackNaturalEndSendsBYE(t *testing.T) {
	db := newFakeCascadeStore()
	svc, up := startLoopbackService(t, fakeSource{cams: []CameraInfo{{ID: "cam-1", Name: "Front", Encoding: "h264"}}}, db)
	_, err := svc.catalogItems()
	require.NoError(t, err)
	start := time.Now().Add(-time.Minute)
	createPlaybackSegment(t, db, "cam-1", start)
	res := up.roundTrip(up.request(sip.INVITE, lbChannelOne, issue14PlaybackSDPAt(t, start.Add(-time.Second), time.Now().Add(time.Minute)), "application/sdp"))
	require.Equal(t, 200, int(res.StatusCode()))
	bye := up.awaitServerRequest(sip.BYE, "")
	callID, ok := bye.CallID()
	require.True(t, ok)
	require.NotEmpty(t, callID.String())
	require.Eventually(t, func() bool { return len(playbackIDs(svc)) == 0 }, time.Second, time.Millisecond)
}

func TestLoopbackPlaybackSkipsBrokenSegments(t *testing.T) {
	db := newFakeCascadeStore()
	svc, up := startLoopbackService(t, fakeSource{cams: []CameraInfo{{ID: "cam-1", Name: "Front", Encoding: "h264"}}}, db)
	_, err := svc.catalogItems()
	require.NoError(t, err)

	start := time.Now().Add(-time.Second)
	badPath := filepath.Join(t.TempDir(), "deleted.mp4")
	validPath, validSeg := writeRawSegment(t, t.TempDir(), []byte{0x67, 1}, []byte{0x68, 2}, [][]byte{{0x65, 1}}, 33, 1000)
	segByPath[validPath] = validSeg
	require.NoError(t, db.InsertRecording(context.Background(), &Recording{
		ID: "deleted", CameraID: "cam-1", FilePath: badPath, Format: FormatH264,
		StartedAt: start, EndedAt: start.Add(time.Second),
	}))
	require.NoError(t, db.InsertRecording(context.Background(), &Recording{
		ID: "valid", CameraID: "cam-1", FilePath: validPath, Format: FormatH264,
		StartedAt: start, EndedAt: start.Add(time.Second),
	}))
	svc.SetSegmentParser(func(path string) (*SegmentInfo, error) {
		if path == badPath {
			return nil, fmt.Errorf("segment is still being written")
		}
		return fakeSegmentParser(path)
	})

	res := up.roundTrip(up.request(sip.INVITE, lbChannelOne,
		issue14PlaybackSDPAt(t, start, start.Add(time.Second)), "application/sdp"))
	require.Equal(t, 200, int(res.StatusCode()))
	up.awaitServerRequest(sip.BYE, "")
}

func issue14PlaybackSDP(t *testing.T) string {
	return issue14PlaybackSDPAt(t, time.Now().Add(-time.Minute), time.Now().Add(time.Minute))
}

func issue14PlaybackSDPAt(t *testing.T, start, end time.Time) string {
	t.Helper()
	return fmt.Sprintf("v=0\r\no=%s 0 0 IN IP4 %s\r\ns=Playback\r\nc=IN IP4 %s\r\nt=%d %d\r\nm=video %d RTP/AVP 96\r\na=recvonly\r\na=rtpmap:96 PS/90000\r\ny=12345678\r\n",
		lbUpperDevice, lbLocalHost, lbLocalHost, start.Unix(), end.Unix(), freeUDPPort(t))
}

type blockingRecordStore struct {
	fakeCascadeStore
	entered   chan struct{}
	cancelled chan struct{}
}

func (s *blockingRecordStore) ListRecordings(ctx context.Context, _ RecordingFilter) ([]Recording, error) {
	close(s.entered)
	<-ctx.Done()
	close(s.cancelled)
	return nil, ctx.Err()
}

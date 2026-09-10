package mp4_test

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/mickeyzzc/gb28181-go/platform/mp4"
	"github.com/stretchr/testify/require"
)

func TestParseSegmentH264MultipleFragments(t *testing.T) {
	sps := []byte{0x67, 0x42, 0x00, 0x1f}
	pps := []byte{0x68, 0xce, 0x06, 0xe2}
	data := [][]byte{
		{0, 0, 0, 3, 0x65, 0x01, 0x02},
		{0, 0, 0, 2, 0x41, 0x03},
		{0, 0, 0, 3, 0x65, 0x04, 0x05},
	}
	path := writeSegment(t, h264Segment(sps, pps, [][]sample{
		{{duration: 40, data: data[0], key: true}, {duration: 40, data: data[1]}},
		{{duration: 40, data: data[2], key: true}},
	}))

	got, err := mp4.ParseSegment(path)
	require.NoError(t, err)
	require.Equal(t, "h264", got.Codec)
	require.Equal(t, uint32(1000), got.Timescale)
	require.Equal(t, sps, got.SPS)
	require.Equal(t, pps, got.PPS)
	require.Len(t, got.Samples, 3)
	require.Equal(t, uint32(40), got.Samples[0].Duration)
	require.True(t, got.Samples[0].IsKeyFrame)
	require.False(t, got.Samples[1].IsKeyFrame)
	require.Equal(t, int64(got.Samples[0].Offset)+int64(got.Samples[0].Size), got.Samples[1].Offset)
	require.Greater(t, got.Samples[2].Offset, got.Samples[1].Offset+int64(got.Samples[1].Size))
}

func TestParseSegmentH265HVCC(t *testing.T) {
	vps := []byte{0x40, 0x01, 0x0c}
	sps := []byte{0x42, 0x01, 0x01}
	pps := []byte{0x44, 0x01, 0xc0}
	path := writeSegment(t, h265Segment(vps, sps, pps, [][]sample{
		{{duration: 3000, data: []byte{0, 0, 0, 2, 0x26, 0x01}, key: true}},
	}))

	got, err := mp4.ParseSegment(path)
	require.NoError(t, err)
	require.Equal(t, "h265", got.Codec)
	require.Equal(t, vps, got.VPS)
	require.Equal(t, sps, got.SPS)
	require.Equal(t, pps, got.PPS)
	require.Len(t, got.Samples, 1)
	require.True(t, got.Samples[0].IsKeyFrame)
}

func TestParseSegmentUsesTrackDefaults(t *testing.T) {
	sampleData := []byte{0, 0, 0, 1, 0x65}
	moof, offset := defaultMoof(sampleData)
	binary.BigEndian.PutUint32(moof[offset:offset+4], uint32(len(moof)+8))
	data := append(box("ftyp", []byte("isom\x00\x00\x02\x00isomiso6")), moov(avc1Box(avcC([]byte{0x67, 1}, []byte{0x68, 2})))...)
	data = append(data, moof...)
	data = append(data, box("mdat", sampleData)...)

	got, err := mp4.ParseSegment(writeSegment(t, data))
	require.NoError(t, err)
	require.Equal(t, uint32(40), got.Samples[0].Duration)
	require.Equal(t, uint32(len(sampleData)), got.Samples[0].Size)
	require.True(t, got.Samples[0].IsKeyFrame)
}

func TestParseSegmentRejectsTruncatedAndInvalidFiles(t *testing.T) {
	sps, pps := []byte{0x67, 1}, []byte{0x68, 2}
	valid := h264Segment(sps, pps, [][]sample{{{duration: 40, data: []byte{0, 0, 0, 1, 0x65}, key: true}}})

	truncated := append([]byte(nil), valid[:len(valid)-1]...)
	err := parseError(t, truncated)
	require.ErrorIs(t, err, mp4.ErrTruncated)

	badSize := append([]byte(nil), valid...)
	binary.BigEndian.PutUint32(badSize[:4], 4)
	err = parseError(t, badSize)
	require.ErrorIs(t, err, mp4.ErrInvalid)

	badOffset := h264Segment(sps, pps, [][]sample{{{duration: 40, data: []byte{0, 0, 0, 1, 0x65}, key: true}}})
	// The first trun data offset is the only sample data locator in this fixture.
	idx := bytes.Index(badOffset, []byte("trun"))
	require.NotEqual(t, -1, idx)
	binary.BigEndian.PutUint32(badOffset[idx+12:idx+16], uint32(len(badOffset)))
	err = parseError(t, badOffset)
	require.ErrorIs(t, err, mp4.ErrInvalid)

	badCount := h264Segment(sps, pps, [][]sample{{{duration: 40, data: []byte{0, 0, 0, 1, 0x65}, key: true}}})
	idx = bytes.Index(badCount, []byte("trun"))
	binary.BigEndian.PutUint32(badCount[idx+8:idx+12], 100001)
	err = parseError(t, badCount)
	require.ErrorIs(t, err, mp4.ErrInvalid)

	badConfig := h264Segment(sps, pps, [][]sample{{{duration: 40, data: []byte{0, 0, 0, 1, 0x65}, key: true}}})
	idx = bytes.Index(badConfig, []byte("avcC"))
	binary.BigEndian.PutUint16(badConfig[idx+10:idx+12], 0xffff)
	err = parseError(t, badConfig)
	require.ErrorIs(t, err, mp4.ErrInvalid)
}

type sample struct {
	duration uint32
	data     []byte
	key      bool
}

func writeSegment(t *testing.T, data []byte) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "segment.mp4")
	require.NoError(t, os.WriteFile(path, data, 0o600))
	return path
}

func parseError(t *testing.T, data []byte) error {
	t.Helper()
	path := writeSegment(t, data)
	_, err := mp4.ParseSegment(path)
	return err
}

func h264Segment(sps, pps []byte, fragments [][]sample) []byte {
	return fragmentedFile(avc1Box(avcC(sps, pps)), fragments)
}

func h265Segment(vps, sps, pps []byte, fragments [][]sample) []byte {
	return fragmentedFile(hvc1Box(hvcC(vps, sps, pps)), fragments)
}

func fragmentedFile(codecBox []byte, fragments [][]sample) []byte {
	data := append(box("ftyp", []byte("isom\x00\x00\x02\x00isomiso6")), moov(codecBox)...)
	for i, samples := range fragments {
		moof, offset := moof(i+1, samples)
		mdatData := flattenSamples(samples)
		binary.BigEndian.PutUint32(moof[offset:offset+4], uint32(len(moof)+8))
		data = append(data, moof...)
		data = append(data, box("mdat", mdatData)...)
	}
	return data
}

func moov(codecBox []byte) []byte {
	mvhd := fullBox("mvhd", 0, 0, make([]byte, 16))
	binary.BigEndian.PutUint32(mvhd[20:24], 1000)
	mdhd := fullBox("mdhd", 0, 0, make([]byte, 16))
	binary.BigEndian.PutUint32(mdhd[20:24], 1000)
	hdlr := fullBox("hdlr", 0, 0, append(make([]byte, 4), []byte("vide")...))
	stsdPayload := append(make([]byte, 4), codecBox...)
	binary.BigEndian.PutUint32(stsdPayload, 1)
	stsd := fullBox("stsd", 0, 0, stsdPayload)
	tkhd := fullBox("tkhd", 0, 0, append(make([]byte, 8), 0, 0, 0, 1))
	stbl := box("stbl", stsd)
	minf := box("minf", stbl)
	mdia := box("mdia", append(append(mdhd, hdlr...), minf...))
	trak := box("trak", append(tkhd, mdia...))
	return box("moov", append(mvhd, trak...))
}

func avc1Box(config []byte) []byte {
	payload := append(make([]byte, 78), config...)
	return box("avc1", payload)
}

func hvc1Box(config []byte) []byte {
	payload := append(make([]byte, 78), config...)
	return box("hvc1", payload)
}

func avcC(sps, pps []byte) []byte {
	p := []byte{1, 0x64, 0, 0x1f, 0xff, 0xe1, byte(len(sps) >> 8), byte(len(sps))}
	p = append(p, sps...)
	p = append(p, 1, byte(len(pps)>>8), byte(len(pps)))
	return box("avcC", append(p, pps...))
}

func hvcC(vps, sps, pps []byte) []byte {
	p := make([]byte, 23)
	p[0], p[1], p[21], p[22] = 1, 1, 3, 3
	for typ, nalu := range map[byte][]byte{32: vps, 33: sps, 34: pps} {
		p = append(p, 0x80|typ, 0, 1, byte(len(nalu)>>8), byte(len(nalu)))
		p = append(p, nalu...)
	}
	return box("hvcC", p)
}

func moof(sequence int, samples []sample) ([]byte, int) {
	tfhd := fullBox("tfhd", 0, 0x020000, []byte{0, 0, 0, 1})
	trunPayload := make([]byte, 8+12*len(samples))
	binary.BigEndian.PutUint32(trunPayload[0:4], uint32(len(samples)))
	// data_offset is patched after the moof size is known.
	for i, s := range samples {
		o := 8 + i*12
		binary.BigEndian.PutUint32(trunPayload[o:o+4], s.duration)
		binary.BigEndian.PutUint32(trunPayload[o+4:o+8], uint32(len(s.data)))
		if !s.key {
			binary.BigEndian.PutUint32(trunPayload[o+8:o+12], 0x10000)
		}
	}
	trun := fullBox("trun", 0, 0x701, trunPayload)
	traf := box("traf", append(tfhd, trun...))
	moof := box("moof", append(fullBox("mfhd", 0, 0, []byte{0, 0, 0, byte(sequence)}), traf...))
	// trun's data_offset starts 16 bytes after the box's type field.
	return moof, 8 + 16 + 8 + 16 + 16
}

func defaultMoof(sampleData []byte) ([]byte, int) {
	tfhdPayload := make([]byte, 16)
	binary.BigEndian.PutUint32(tfhdPayload[0:4], 1)
	binary.BigEndian.PutUint32(tfhdPayload[4:8], 40)
	binary.BigEndian.PutUint32(tfhdPayload[8:12], uint32(len(sampleData)))
	tfhd := fullBox("tfhd", 0, 0x020038, tfhdPayload)
	trun := fullBox("trun", 0, 1, make([]byte, 8))
	binary.BigEndian.PutUint32(trun[12:16], 1)
	traf := box("traf", append(tfhd, trun...))
	moof := box("moof", append(fullBox("mfhd", 0, 0, []byte{0, 0, 0, 1}), traf...))
	return moof, bytes.Index(moof, []byte("trun")) + 12
}

func flattenSamples(samples []sample) []byte {
	var out []byte
	for _, s := range samples {
		out = append(out, s.data...)
	}
	return out
}

func fullBox(typ string, version, flags uint32, payload []byte) []byte {
	h := make([]byte, 4)
	binary.BigEndian.PutUint32(h, version<<24|flags)
	return box(typ, append(h, payload...))
}

func box(typ string, payload []byte) []byte {
	out := make([]byte, 8+len(payload))
	binary.BigEndian.PutUint32(out, uint32(len(out)))
	copy(out[4:8], typ)
	copy(out[8:], payload)
	return out
}

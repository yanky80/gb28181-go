package mp4

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mickeyzzc/gb28181-go/platform/cascade"
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

	got, err := ParseSegment(path)
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

	got, err := ParseSegment(path)
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
	data := append(makeBox("ftyp", []byte("isom\x00\x00\x02\x00isomiso6")), moov(avc1Box(avcC([]byte{0x67, 1}, []byte{0x68, 2})))...)
	data = append(data, moof...)
	data = append(data, makeBox("mdat", sampleData)...)

	got, err := ParseSegment(writeSegment(t, data))
	require.NoError(t, err)
	require.Equal(t, uint32(40), got.Samples[0].Duration)
	require.Equal(t, uint32(len(sampleData)), got.Samples[0].Size)
	require.True(t, got.Samples[0].IsKeyFrame)
}

func TestParseSegmentStandardVersion1Tkhd(t *testing.T) {
	sps, pps := []byte{0x67, 1}, []byte{0x68, 2}
	path := writeSegment(t, fragmentedFileVersion(avc1Box(avcC(sps, pps)), [][]sample{{
		{duration: 40, data: []byte{0, 0, 0, 1, 0x65}, key: true},
	}}, 1))

	got, err := ParseSegment(path)
	require.NoError(t, err)
	require.Equal(t, uint32(1000), got.Timescale)
	require.Len(t, got.Samples, 1)
}

func TestParseSegmentStandardVersion1Mdhd(t *testing.T) {
	data := fragmentedFileWithMoov(moovVersions([][]byte{avc1Box(avcC([]byte{0x67, 1}, []byte{0x68, 2}))}, 0, 1, nil))

	got, err := ParseSegment(writeSegment(t, data))
	require.NoError(t, err)
	require.Equal(t, uint32(1000), got.Timescale)
}

func TestParseSegmentAcceptsEmptyAndNamedHdlr(t *testing.T) {
	codec := avc1Box(avcC([]byte{0x67, 1}, []byte{0x68, 2}))
	for _, hdlrLen := range []int{20, 27} {
		data := fragmentedFileWithMoov(moovVersionsWithLengths(
			[][]byte{codec}, 0, 0, nil, 80, 20, hdlrLen))
		_, err := ParseSegment(writeSegment(t, data))
		require.NoError(t, err)
	}
}

func TestParseSegmentRejectsInvalidHdlrFields(t *testing.T) {
	mutations := []struct {
		name string
		edit func([]byte)
	}{
		{"version", func(data []byte) { hdlrField(data, 0)[0] = 1 }},
		{"flags", func(data []byte) { hdlrField(data, 1)[0] = 1 }},
		{"pre-defined", func(data []byte) { hdlrField(data, 4)[0] = 1 }},
		{"reserved", func(data []byte) { hdlrField(data, 12)[0] = 1 }},
		{"invalid-utf8", func(data []byte) { hdlrField(data, 24)[0] = 0xff }},
		{"early-nul", func(data []byte) { hdlrField(data, 25)[0] = 0 }},
		{"missing-nul", func(data []byte) { hdlrField(data, 30)[0] = 'x' }},
	}
	for _, tt := range mutations {
		t.Run(tt.name, func(t *testing.T) {
			data := h264Segment([]byte{0x67, 1}, []byte{0x68, 2}, [][]sample{{
				{duration: 40, data: []byte{0, 0, 0, 1, 0x65}, key: true},
			}})
			tt.edit(data)
			_, err := ParseSegment(writeSegment(t, data))
			require.ErrorIs(t, err, ErrInvalid)
		})
	}
}

func TestParseSegmentRejectsShortStandardFixedBoxes(t *testing.T) {
	codec := avc1Box(avcC([]byte{0x67, 1}, []byte{0x68, 2}))
	tests := []struct {
		name                      string
		tkhdVersion, mdhdVersion  byte
		tkhdLen, mdhdLen, hdlrLen int
	}{
		{"tkhd-v0", 0, 0, 79, 20, 27},
		{"tkhd-v1", 1, 0, 91, 20, 27},
		{"mdhd-v0", 0, 0, 80, 19, 27},
		{"mdhd-v1", 0, 1, 80, 31, 27},
		{"hdlr", 0, 0, 80, 20, 19},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := fragmentedFileWithMoov(moovVersionsWithLengths(
				[][]byte{codec}, tt.tkhdVersion, tt.mdhdVersion, nil,
				tt.tkhdLen, tt.mdhdLen, tt.hdlrLen))
			_, err := ParseSegment(writeSegment(t, data))
			require.Error(t, err)
			require.True(t, errors.Is(err, ErrTruncated) || errors.Is(err, ErrInvalid), "%v", err)
		})
	}
}

func TestParseSegmentValidatesStsdEntriesAfterFirstCodec(t *testing.T) {
	first := avc1Box(avcC([]byte{0x67, 1}, []byte{0x68, 2}))
	second := makeBox("hvc1", make([]byte, 10))
	data := fragmentedFileWithMoov(moovVersions([][]byte{first, second}, 0, 0, nil))

	_, err := ParseSegment(writeSegment(t, data))
	require.ErrorIs(t, err, ErrInvalid)
}

func TestParseSegmentValidatesLaterSupportedCodecConfig(t *testing.T) {
	first := avc1Box(avcC([]byte{0x67, 1}, []byte{0x68, 2}))
	second := hvc1Box(nil)
	data := fragmentedFileWithMoov(moovVersions([][]byte{first, second}, 0, 0, nil))

	_, err := ParseSegment(writeSegment(t, data))
	require.ErrorIs(t, err, ErrInvalid)
}

func TestParseSegmentAcceptsExtendedSizeStsdAndCodecChildren(t *testing.T) {
	config := avcC([]byte{0x67, 1}, []byte{0x68, 2})
	tests := []struct {
		name    string
		entries [][]byte
	}{
		{
			name: "unknown entry and child",
			entries: [][]byte{
				extendedBox("free", []byte{1, 2, 3}),
				avc1BoxWithTail(config, extendedBox("free", []byte{4, 5, 6})),
			},
		},
		{
			name: "supported entry and config",
			entries: [][]byte{
				extendedCodecBox("avc1", config),
			},
		},
		{
			name: "zero-sized terminal entry and child",
			entries: [][]byte{
				avc1BoxWithTail(config, zeroBox("free", []byte{7, 8})),
				zeroBox("free", []byte{9, 10}),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := fragmentedFileWithMoov(moovVersions(tt.entries, 0, 0, nil))
			got, err := ParseSegment(writeSegment(t, data))
			require.NoError(t, err)
			require.Equal(t, "h264", got.Codec)
		})
	}
}

func TestParseSegmentRejectsMalformedExtendedSizeStsdAndCodecBoxes(t *testing.T) {
	config := avcC([]byte{0x67, 1}, []byte{0x68, 2})
	tests := []struct {
		name    string
		entries [][]byte
	}{
		{
			name: "truncated entry largesize",
			entries: [][]byte{
				avc1Box(config),
				truncatedExtendedHeader("free"),
			},
		},
		{
			name: "entry largesize smaller than header",
			entries: [][]byte{
				avc1Box(config),
				extendedHeader("free", 8),
			},
		},
		{
			name: "entry largesize beyond parent",
			entries: [][]byte{
				avc1Box(config),
				extendedHeader("free", 1<<20),
			},
		},
		{
			name: "damaged subsequent codec child",
			entries: [][]byte{
				avc1BoxWithTail(config, truncatedExtendedHeader("free")),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := fragmentedFileWithMoov(moovVersions(tt.entries, 0, 0, nil))
			_, err := ParseSegment(writeSegment(t, data))
			require.Error(t, err)
			require.True(t, errors.Is(err, ErrTruncated) || errors.Is(err, ErrInvalid), "%v", err)
		})
	}
}

func TestParseSegmentRejectsDamagedCodecConfigTail(t *testing.T) {
	tests := []struct {
		name string
		box  []byte
	}{
		{"avcC", avc1BoxWithTail(avcC([]byte{0x67, 1}, []byte{0x68, 2}), []byte{0, 0, 0, 4})},
		{"hvcC", hvc1BoxWithTail(hvcC([]byte{0x40, 1}, []byte{0x42, 1}, []byte{0x44, 1}), []byte{0, 0, 0, 4})},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := fragmentedFileWithMoov(moovVersions([][]byte{tt.box}, 0, 0, nil))
			_, err := ParseSegment(writeSegment(t, data))
			require.ErrorIs(t, err, ErrInvalid)
		})
	}
}

func TestParseSegmentRejectsDuplicateCodecConfig(t *testing.T) {
	config := avcC([]byte{0x67, 1}, []byte{0x68, 2})
	data := fragmentedFileWithMoov(moovVersions([][]byte{avc1BoxWithTail(config, config)}, 0, 0, nil))

	_, err := ParseSegment(writeSegment(t, data))
	require.ErrorIs(t, err, ErrInvalid)
}

func TestParseSegmentRejectsConflictingCodecConfig(t *testing.T) {
	data := fragmentedFileWithMoov(moovVersions([][]byte{avc1BoxWithTail(
		avcC([]byte{0x67, 1}, []byte{0x68, 2}),
		hvcC([]byte{0x40, 1}, []byte{0x42, 1}, []byte{0x44, 1}),
	)}, 0, 0, nil))

	_, err := ParseSegment(writeSegment(t, data))
	require.ErrorIs(t, err, ErrInvalid)
}

func TestParseSegmentRejectsStsdTrailingBytes(t *testing.T) {
	codec := avc1Box(avcC([]byte{0x67, 1}, []byte{0x68, 2}))
	data := fragmentedFileWithMoov(moovVersions([][]byte{codec}, 0, 0, []byte{0}))

	_, err := ParseSegment(writeSegment(t, data))
	require.ErrorIs(t, err, ErrInvalid)
}

func TestParseSegmentRejectsNonFourByteNALLength(t *testing.T) {
	h264 := h264Segment([]byte{0x67, 1}, []byte{0x68, 2}, [][]sample{{
		{duration: 40, data: []byte{0, 0, 0, 1, 0x65}, key: true},
	}})
	idx := bytes.Index(h264, []byte("avcC"))
	require.NotEqual(t, -1, idx)
	h264[idx+8] &^= 3
	_, err := ParseSegment(writeSegment(t, h264))
	require.ErrorIs(t, err, ErrInvalid)

	h265 := h265Segment([]byte{0x40, 1}, []byte{0x42, 1}, []byte{0x44, 1}, [][]sample{{
		{duration: 40, data: []byte{0, 0, 0, 2, 0x26, 9}, key: true},
	}})
	idx = bytes.Index(h265, []byte("hvcC"))
	require.NotEqual(t, -1, idx)
	h265[idx+4+21] &^= 3
	_, err = ParseSegment(writeSegment(t, h265))
	require.ErrorIs(t, err, ErrInvalid)
}

func TestParseSegmentImplicitTfhdBase(t *testing.T) {
	sampleData := []byte{0, 0, 0, 2, 0x26, 9}
	moof, dataOffset := implicitBaseMoof([]sample{{duration: 40, data: sampleData, key: true}})
	binary.BigEndian.PutUint32(moof[dataOffset:dataOffset+4], uint32(len(moof)+8))
	data := append(makeBox("ftyp", []byte("isom\x00\x00\x02\x00isomiso6")), moov(hvc1Box(hvcC([]byte{0x40, 1}, []byte{0x42, 1}, []byte{0x44, 1})))...)
	data = append(data, moof...)
	data = append(data, makeBox("mdat", sampleData)...)

	got, err := ParseSegment(writeSegment(t, data))
	require.NoError(t, err)
	require.Equal(t, int64(len(data)-len(sampleData)), got.Samples[0].Offset)
	require.Equal(t, uint32(len(sampleData)), got.Samples[0].Size)
}

func TestParseSegmentRejectsNestedBoxBudget(t *testing.T) {
	var nested []byte
	for i := 0; i < 100001; i++ {
		nested = append(nested, makeBox("free", nil)...)
	}
	base := moov(avc1Box(avcC([]byte{0x67, 1}, []byte{0x68, 2})))
	largeMoov := makeBox("moov", append(base[8:], makeBox("mvex", nested)...))
	data := append(makeBox("ftyp", []byte("isom\x00\x00\x02\x00isomiso6")), largeMoov...)

	err := parseError(t, data)
	require.ErrorIs(t, err, ErrInvalid)
}

func TestParseSegmentRejectsFileChangesDuringParse(t *testing.T) {
	valid := h264Segment([]byte{0x67, 1}, []byte{0x68, 2}, [][]sample{{
		{duration: 40, data: []byte{0, 0, 0, 1, 0x65}, key: true},
	}})
	for _, tc := range []struct {
		name  string
		write func(string) error
	}{
		{"growth", func(path string) error {
			f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
			if err != nil {
				return err
			}
			defer f.Close()
			_, err = f.Write([]byte{0})
			return err
		}},
		{"truncation", func(path string) error {
			return os.Truncate(path, int64(len(valid)-1))
		}},
		{"mode", func(path string) error {
			return os.Chmod(path, 0o640)
		}},
		{"metadata", func(path string) error {
			st, err := os.Stat(path)
			if err != nil {
				return err
			}
			return os.Chtimes(path, st.ModTime(), st.ModTime().Add(time.Second))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := writeSegment(t, valid)
			var writeErr error
			_, err := parseSegment(path, func() { writeErr = tc.write(path) })
			require.NoError(t, writeErr)
			require.ErrorIs(t, err, ErrChanged)
		})
	}

	path := writeSegment(t, valid)
	var writeErr error
	_, err := parseSegment(path, func() { writeErr = os.Truncate(path, 8) })
	require.NoError(t, writeErr)
	require.ErrorIs(t, err, ErrChanged, "file change must win over the parse error it causes")
}

func TestParseSegmentRejectsTruncatedAndInvalidFiles(t *testing.T) {
	sps, pps := []byte{0x67, 1}, []byte{0x68, 2}
	valid := h264Segment(sps, pps, [][]sample{{{duration: 40, data: []byte{0, 0, 0, 1, 0x65}, key: true}}})

	truncated := append([]byte(nil), valid[:len(valid)-1]...)
	err := parseError(t, truncated)
	require.ErrorIs(t, err, ErrTruncated)

	badSize := append([]byte(nil), valid...)
	binary.BigEndian.PutUint32(badSize[:4], 4)
	err = parseError(t, badSize)
	require.ErrorIs(t, err, ErrInvalid)

	badOffset := h264Segment(sps, pps, [][]sample{{{duration: 40, data: []byte{0, 0, 0, 1, 0x65}, key: true}}})
	// The first trun data offset is the only sample data locator in this fixture.
	idx := bytes.Index(badOffset, []byte("trun"))
	require.NotEqual(t, -1, idx)
	binary.BigEndian.PutUint32(badOffset[idx+12:idx+16], uint32(len(badOffset)))
	err = parseError(t, badOffset)
	require.ErrorIs(t, err, ErrInvalid)

	badCount := h264Segment(sps, pps, [][]sample{{{duration: 40, data: []byte{0, 0, 0, 1, 0x65}, key: true}}})
	idx = bytes.Index(badCount, []byte("trun"))
	binary.BigEndian.PutUint32(badCount[idx+8:idx+12], 100001)
	err = parseError(t, badCount)
	require.ErrorIs(t, err, ErrInvalid)

	badConfig := h264Segment(sps, pps, [][]sample{{{duration: 40, data: []byte{0, 0, 0, 1, 0x65}, key: true}}})
	idx = bytes.Index(badConfig, []byte("avcC"))
	binary.BigEndian.PutUint16(badConfig[idx+10:idx+12], 0xffff)
	err = parseError(t, badConfig)
	require.ErrorIs(t, err, ErrInvalid)
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
	_, err := ParseSegment(path)
	return err
}

func prependParameterSets(info *cascade.SegmentInfo, annexB []byte) []byte {
	var out []byte
	sets := [][]byte{info.SPS, info.PPS}
	if info.Codec == "h265" {
		sets = [][]byte{info.VPS, info.SPS, info.PPS}
	}
	for _, set := range sets {
		out = append(out, 0, 0, 0, 1)
		out = append(out, set...)
	}
	return append(out, annexB...)
}

func lengthPrefixedToAnnexB(data []byte) []byte {
	var out []byte
	for len(data) >= 4 {
		n := int(binary.BigEndian.Uint32(data))
		if n > len(data)-4 {
			return nil
		}
		out = append(out, 0, 0, 0, 1)
		out = append(out, data[4:4+n]...)
		data = data[4+n:]
	}
	return out
}

func h264Segment(sps, pps []byte, fragments [][]sample) []byte {
	return fragmentedFile(avc1Box(avcC(sps, pps)), fragments)
}

func h265Segment(vps, sps, pps []byte, fragments [][]sample) []byte {
	return fragmentedFile(hvc1Box(hvcC(vps, sps, pps)), fragments)
}

func fragmentedFile(codecBox []byte, fragments [][]sample) []byte {
	return fragmentedFileVersion(codecBox, fragments, 0)
}

func fragmentedFileVersion(codecBox []byte, fragments [][]sample, tkhdVersion byte) []byte {
	return fragmentedFileWithMoovSamples(moovVersion(codecBox, tkhdVersion), fragments)
}

func fragmentedFileWithMoov(moovBox []byte) []byte {
	return fragmentedFileWithMoovSamples(moovBox, [][]sample{{
		{duration: 40, data: []byte{0, 0, 0, 1, 0x65}, key: true},
	}})
}

func fragmentedFileWithMoovSamples(moovBox []byte, fragments [][]sample) []byte {
	data := append(makeBox("ftyp", []byte("isom\x00\x00\x02\x00isomiso6")), moovBox...)
	for i, samples := range fragments {
		moof, offset := moof(i+1, samples)
		mdatData := flattenSamples(samples)
		binary.BigEndian.PutUint32(moof[offset:offset+4], uint32(len(moof)+8))
		data = append(data, moof...)
		data = append(data, makeBox("mdat", mdatData)...)
	}
	return data
}

func moov(codecBox []byte) []byte {
	return moovVersion(codecBox, 0)
}

func moovVersion(codecBox []byte, tkhdVersion byte) []byte {
	return moovVersions([][]byte{codecBox}, tkhdVersion, 0, nil)
}

func moovVersions(entries [][]byte, tkhdVersion, mdhdVersion byte, stsdTail []byte) []byte {
	tkhdLen := 80
	if tkhdVersion == 1 {
		tkhdLen = 92
	}
	mdhdLen := 20
	if mdhdVersion == 1 {
		mdhdLen = 32
	}
	return moovVersionsWithLengths(entries, tkhdVersion, mdhdVersion, stsdTail, tkhdLen, mdhdLen, 27)
}

func moovVersionsWithLengths(entries [][]byte, tkhdVersion, mdhdVersion byte, stsdTail []byte, tkhdLen, mdhdLen, hdlrLen int) []byte {
	mvhd := fullBox("mvhd", 0, 0, make([]byte, 16))
	binary.BigEndian.PutUint32(mvhd[20:24], 1000)
	mdhdPayload := make([]byte, mdhdLen)
	mdhd := fullBox("mdhd", uint32(mdhdVersion), 0, mdhdPayload)
	if mdhdVersion == 1 {
		binary.BigEndian.PutUint32(mdhd[28:32], 1000)
	} else {
		binary.BigEndian.PutUint32(mdhd[20:24], 1000)
	}
	hdlrPayload := make([]byte, hdlrLen)
	if len(hdlrPayload) >= 8 {
		copy(hdlrPayload[4:8], "vide")
	}
	if len(hdlrPayload) >= 27 {
		copy(hdlrPayload[20:27], "camera\x00")
	}
	hdlr := fullBox("hdlr", 0, 0, hdlrPayload)
	stsdPayload := make([]byte, 4)
	binary.BigEndian.PutUint32(stsdPayload, uint32(len(entries)))
	for _, entry := range entries {
		stsdPayload = append(stsdPayload, entry...)
	}
	stsdPayload = append(stsdPayload, stsdTail...)
	stsd := fullBox("stsd", 0, 0, stsdPayload)
	tkhdPayload := make([]byte, tkhdLen)
	if tkhdVersion == 1 {
		binary.BigEndian.PutUint32(tkhdPayload[16:20], 1)
	} else if tkhdVersion == 0 {
		binary.BigEndian.PutUint32(tkhdPayload[8:12], 1)
	}
	tkhd := fullBox("tkhd", uint32(tkhdVersion), 0, tkhdPayload)
	stbl := makeBox("stbl", stsd)
	minf := makeBox("minf", stbl)
	mdia := makeBox("mdia", append(append(mdhd, hdlr...), minf...))
	trak := makeBox("trak", append(tkhd, mdia...))
	return makeBox("moov", append(mvhd, trak...))
}

func avc1Box(config []byte) []byte {
	return avc1BoxWithTail(config, nil)
}

func hvc1Box(config []byte) []byte {
	return hvc1BoxWithTail(config, nil)
}

func avc1BoxWithTail(config, tail []byte) []byte {
	payload := append(make([]byte, 78), config...)
	return makeBox("avc1", append(payload, tail...))
}

func hvc1BoxWithTail(config, tail []byte) []byte {
	payload := append(make([]byte, 78), config...)
	return makeBox("hvc1", append(payload, tail...))
}

func hdlrField(data []byte, offset int) []byte {
	idx := bytes.Index(data, []byte("hdlr"))
	if idx < 0 {
		panic("hdlr fixture missing")
	}
	return data[idx+4+offset : idx+4+offset+1]
}

func avcC(sps, pps []byte) []byte {
	p := []byte{1, 0x64, 0, 0x1f, 0xff, 0xe1, byte(len(sps) >> 8), byte(len(sps))}
	p = append(p, sps...)
	p = append(p, 1, byte(len(pps)>>8), byte(len(pps)))
	return makeBox("avcC", append(p, pps...))
}

func hvcC(vps, sps, pps []byte) []byte {
	p := make([]byte, 23)
	p[0], p[1], p[21], p[22] = 1, 1, 3, 3
	for typ, nalu := range map[byte][]byte{32: vps, 33: sps, 34: pps} {
		p = append(p, 0x80|typ, 0, 1, byte(len(nalu)>>8), byte(len(nalu)))
		p = append(p, nalu...)
	}
	return makeBox("hvcC", p)
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
	traf := makeBox("traf", append(tfhd, trun...))
	moof := makeBox("moof", append(fullBox("mfhd", 0, 0, []byte{0, 0, 0, byte(sequence)}), traf...))
	// trun's data_offset starts 16 bytes after the box's type field.
	return moof, 8 + 16 + 8 + 16 + 16
}

func defaultMoof(sampleData []byte) ([]byte, int) {
	tfhdPayload := make([]byte, 20)
	binary.BigEndian.PutUint32(tfhdPayload[0:4], 1)
	binary.BigEndian.PutUint32(tfhdPayload[4:8], 1)
	binary.BigEndian.PutUint32(tfhdPayload[8:12], 40)
	binary.BigEndian.PutUint32(tfhdPayload[12:16], uint32(len(sampleData)))
	tfhd := fullBox("tfhd", 0, 0x02003a, tfhdPayload)
	trun := fullBox("trun", 0, 1, make([]byte, 8))
	binary.BigEndian.PutUint32(trun[12:16], 1)
	traf := makeBox("traf", append(tfhd, trun...))
	moof := makeBox("moof", append(fullBox("mfhd", 0, 0, []byte{0, 0, 0, 1}), traf...))
	return moof, bytes.Index(moof, []byte("trun")) + 12
}

func implicitBaseMoof(samples []sample) ([]byte, int) {
	tfhd := fullBox("tfhd", 0, 0, []byte{0, 0, 0, 1})
	trunPayload := make([]byte, 8+12*len(samples))
	binary.BigEndian.PutUint32(trunPayload[0:4], uint32(len(samples)))
	for i, s := range samples {
		o := 8 + i*12
		binary.BigEndian.PutUint32(trunPayload[o:o+4], s.duration)
		binary.BigEndian.PutUint32(trunPayload[o+4:o+8], uint32(len(s.data)))
		if !s.key {
			binary.BigEndian.PutUint32(trunPayload[o+8:o+12], 0x10000)
		}
	}
	trun := fullBox("trun", 0, 0x701, trunPayload)
	traf := makeBox("traf", append(tfhd, trun...))
	moof := makeBox("moof", append(fullBox("mfhd", 0, 0, []byte{0, 0, 0, 1}), traf...))
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
	return makeBox(typ, append(h, payload...))
}

func makeBox(typ string, payload []byte) []byte {
	out := make([]byte, 8+len(payload))
	binary.BigEndian.PutUint32(out, uint32(len(out)))
	copy(out[4:8], typ)
	copy(out[8:], payload)
	return out
}

func extendedBox(typ string, payload []byte) []byte {
	out := make([]byte, 16+len(payload))
	binary.BigEndian.PutUint32(out, 1)
	copy(out[4:8], typ)
	binary.BigEndian.PutUint64(out[8:16], uint64(len(out)))
	copy(out[16:], payload)
	return out
}

func extendedCodecBox(typ string, config []byte) []byte {
	return extendedBox(typ, append(make([]byte, 78), config...))
}

func extendedHeader(typ string, size uint64) []byte {
	out := make([]byte, 16)
	binary.BigEndian.PutUint32(out, 1)
	copy(out[4:8], typ)
	binary.BigEndian.PutUint64(out[8:16], size)
	return out
}

func truncatedExtendedHeader(typ string) []byte {
	out := make([]byte, 8)
	binary.BigEndian.PutUint32(out, 1)
	copy(out[4:8], typ)
	return out
}

func zeroBox(typ string, payload []byte) []byte {
	out := make([]byte, 8+len(payload))
	copy(out[4:8], typ)
	copy(out[8:], payload)
	return out
}

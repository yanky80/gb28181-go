// Package mp4 parses recorded segment files in the small ISO BMFF subset:
// one video track in a fragmented MP4 file, with avcC/hvcC metadata and trun
// sample tables. It deliberately does not decode media payloads.
package mp4

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/mickeyzzc/gb28181-go/platform/cascade"
)

var (
	// ErrInvalid reports malformed box structure, metadata, offsets, or values.
	ErrInvalid = errors.New("mp4: invalid fragmented MP4")
	// ErrTruncated reports a file that ends before a declared box or table.
	ErrTruncated = errors.New("mp4: truncated fragmented MP4")
	// ErrChanged reports a file whose size or stat metadata changed mid-parse.
	ErrChanged = errors.New("mp4: segment changed during parse")
)

const (
	maxBoxes      = 100_000
	maxMetadata   = 100_000
	maxSamples    = 100_000
	maxConfigSize = 1 << 20
	// ponytail: 16 MiB sample cap bounds playback allocation; raise only with
	// a streaming sample reader.
	maxSampleSize = 16 << 20
)

// ParseSegment parses one complete fragmented MP4 recording made of one or
// more fragments. Returned offsets point into path, and sample payloads retain
// their MP4 length-prefix framing for cascade playback.
func ParseSegment(path string) (*cascade.SegmentInfo, error) {
	return parseSegment(path, nil)
}

func parseSegment(path string, duringParse func()) (*cascade.SegmentInfo, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("mp4: open %s: %w", path, err)
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("mp4: stat %s: %w", path, err)
	}
	if st.Size() < 8 {
		return nil, fmt.Errorf("%w: file is too short", ErrTruncated)
	}
	p := &parser{r: f, size: st.Size(), onReadBox: duringParse}
	info, parseErr := p.parse()
	end, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("mp4: final stat %s: %w", path, err)
	}
	if end.Size() != st.Size() || end.Mode() != st.Mode() || !end.ModTime().Equal(st.ModTime()) {
		return nil, fmt.Errorf("%w: %s", ErrChanged, path)
	}
	if parseErr != nil {
		return nil, parseErr
	}
	return info, nil
}

type parser struct {
	r         io.ReaderAt
	size      int64
	boxCount  int
	metaCount int
	onReadBox func()
}

type box struct {
	start, payload, end int64
	typ                 string
}

func (p *parser) parse() (*cascade.SegmentInfo, error) {
	var top []box
	for off := int64(0); off < p.size; {
		b, err := p.readBox(off, p.size, true)
		if err != nil {
			return nil, err
		}
		if b.typ == "moov" || b.typ == "moof" || b.typ == "mdat" {
			top = append(top, b)
		}
		off = b.end
	}

	var track *trackInfo
	for _, b := range top {
		if b.typ != "moov" {
			continue
		}
		if track != nil {
			return nil, invalid("multiple moov boxes")
		}
		var err error
		track, err = p.parseMoov(b)
		if err != nil {
			return nil, err
		}
	}
	if track == nil {
		return nil, invalid("missing moov")
	}
	if track.timescale == 0 {
		return nil, invalid("video timescale is zero")
	}

	var mdats []box
	var samples []cascade.SegmentSample
	moofCount := 0
	mdatCount := 0
	for _, b := range top {
		switch b.typ {
		case "mdat":
			mdatCount++
			mdats = append(mdats, b)
		case "moof":
			moofCount++
			part, err := p.parseMoof(b, track)
			if err != nil {
				return nil, err
			}
			if len(samples)+len(part) > maxSamples {
				return nil, invalid("sample count exceeds limit")
			}
			samples = append(samples, part...)
		}
	}
	if moofCount == 0 || mdatCount == 0 || len(samples) == 0 {
		return nil, invalid("missing video fragments")
	}
	previousEnd := int64(-1)
	for _, sample := range samples {
		end, ok := addInt64(sample.Offset, int64(sample.Size))
		if !ok || sample.Offset < 0 || sample.Offset < previousEnd || !inMdat(sample.Offset, end, mdats) {
			return nil, invalid("sample offset is outside mdat")
		}
		previousEnd = end
	}

	return &cascade.SegmentInfo{
		Codec: track.codec, SPS: track.sps, PPS: track.pps, VPS: track.vps,
		Timescale: track.timescale, Samples: samples,
	}, nil
}

type trackInfo struct {
	id                      uint32
	codec                   string
	timescale               uint32
	sps, pps                []byte
	vps                     []byte
	defaultDur, defaultSize uint32
	defaultFlags            uint32
}

func (p *parser) parseMoov(moov box) (*trackInfo, error) {
	var track *trackInfo
	var trex map[uint32][3]uint32
	for off := moov.payload; off < moov.end; {
		b, err := p.readBox(off, moov.end, false)
		if err != nil {
			return nil, err
		}
		switch b.typ {
		case "trak":
			candidate, ok, err := p.parseTrak(b)
			if err != nil {
				return nil, err
			} else if ok && track == nil {
				track = candidate
			}
		case "mvex":
			trex = make(map[uint32][3]uint32)
			for x := b.payload; x < b.end; {
				child, err := p.readBox(x, b.end, false)
				if err != nil {
					return nil, err
				}
				if child.typ == "trex" {
					if err := p.addMetadata(1); err != nil {
						return nil, err
					}
					flags, data, err := p.fullBox(child)
					if err != nil {
						return nil, err
					}
					if flags>>24 != 0 {
						return nil, invalid("invalid trex version")
					}
					if len(data) < 20 {
						return nil, truncated("short trex")
					}
					trex[binary.BigEndian.Uint32(data[:4])] = [3]uint32{
						binary.BigEndian.Uint32(data[8:12]),
						binary.BigEndian.Uint32(data[12:16]),
						binary.BigEndian.Uint32(data[16:20]),
					}
				}
				x = child.end
			}
		}
		off = b.end
	}
	if track == nil {
		return nil, invalid("missing video track")
	}
	if defaults, ok := trex[track.id]; ok {
		track.defaultDur, track.defaultSize, track.defaultFlags = defaults[0], defaults[1], defaults[2]
	}
	return track, nil
}

func (p *parser) parseTrak(trak box) (*trackInfo, bool, error) {
	var id uint32
	var mdia box
	for off := trak.payload; off < trak.end; {
		b, err := p.readBox(off, trak.end, false)
		if err != nil {
			return nil, false, err
		}
		switch b.typ {
		case "tkhd":
			v, err := p.readPrefix(b, 4)
			if err != nil {
				return nil, false, err
			}
			if v[0] != 0 && v[0] != 1 {
				return nil, false, invalid("unsupported tkhd version")
			}
			if v[0] == 1 {
				v, err = p.readPrefix(b, 96)
				if err != nil {
					return nil, false, err
				}
				id = binary.BigEndian.Uint32(v[20:24])
			} else {
				v, err = p.readPrefix(b, 84)
				if err != nil {
					return nil, false, err
				}
				id = binary.BigEndian.Uint32(v[12:16])
			}
		case "mdia":
			mdia = b
		}
		off = b.end
	}
	if id == 0 || mdia.end == 0 {
		return nil, false, nil
	}

	var handler string
	var timescale uint32
	var stsd box
	for off := mdia.payload; off < mdia.end; {
		b, err := p.readBox(off, mdia.end, false)
		if err != nil {
			return nil, false, err
		}
		switch b.typ {
		case "mdhd":
			v, err := p.readPrefix(b, 4)
			if err != nil {
				return nil, false, err
			}
			if v[0] != 0 && v[0] != 1 {
				return nil, false, invalid("unsupported mdhd version")
			}
			if v[0] == 1 {
				v, err = p.readPrefix(b, 36)
				if err != nil {
					return nil, false, err
				}
				timescale = binary.BigEndian.Uint32(v[20:24])
			} else {
				v, err = p.readPrefix(b, 24)
				if err != nil {
					return nil, false, err
				}
				timescale = binary.BigEndian.Uint32(v[12:16])
			}
		case "hdlr":
			v, err := p.readPrefix(b, 24)
			if err != nil {
				return nil, false, err
			}
			if b.end-b.payload > 24 {
				var terminator [1]byte
				if _, err := p.r.ReadAt(terminator[:], b.end-1); err != nil {
					if errors.Is(err, io.EOF) {
						return nil, false, truncated("short hdlr name")
					}
					return nil, false, err
				}
				if terminator[0] != 0 {
					return nil, false, invalid("hdlr name is not null-terminated")
				}
			}
			handler = string(v[8:12])
		case "minf":
			for x := b.payload; x < b.end; {
				child, err := p.readBox(x, b.end, false)
				if err != nil {
					return nil, false, err
				}
				if child.typ == "stbl" {
					for y := child.payload; y < child.end; {
						grandchild, err := p.readBox(y, child.end, false)
						if err != nil {
							return nil, false, err
						}
						if grandchild.typ == "stsd" {
							stsd = grandchild
						}
						y = grandchild.end
					}
				}
				x = child.end
			}
		}
		off = b.end
	}
	if handler != "vide" {
		return nil, false, nil
	}
	if stsd.end == 0 {
		return nil, false, invalid("video track missing stsd")
	}
	info, err := p.parseStsd(stsd)
	if err != nil {
		return nil, false, err
	}
	info.id, info.timescale = id, timescale
	return info, true, nil
}

func (p *parser) parseStsd(stsd box) (*trackInfo, error) {
	v, err := p.readSmall(stsd, maxConfigSize)
	if err != nil {
		return nil, err
	}
	if len(v) < 8 {
		return nil, invalid("short stsd")
	}
	count := binary.BigEndian.Uint32(v[4:8])
	if count == 0 || count > 16 {
		return nil, invalid("invalid stsd entry count")
	}
	off := int64(8)
	var first *trackInfo
	for i := uint32(0); i < count; i++ {
		if off+8 > int64(len(v)) {
			return nil, truncated("short stsd entry")
		}
		sz := int64(binary.BigEndian.Uint32(v[off:]))
		if sz < 86 || off+sz > int64(len(v)) {
			return nil, invalid("invalid video sample entry")
		}
		typ := string(v[off+4 : off+8])
		codec := ""
		configType := ""
		switch typ {
		case "avc1", "avc3":
			codec, configType = "h264", "avcC"
		case "hvc1", "hev1":
			codec, configType = "h265", "hvcC"
		default:
			off += sz
			continue
		}
		if first == nil {
			info := &trackInfo{codec: codec}
			if err := p.parseCodecConfig(v[off+86:off+sz], configType, info); err != nil {
				return nil, err
			}
			first = info
		}
		off += sz
	}
	if off != int64(len(v)) {
		return nil, invalid("stsd has trailing data")
	}
	if first != nil {
		return first, nil
	}
	return nil, invalid("unsupported video codec")
}

func (p *parser) parseCodecConfig(data []byte, typ string, info *trackInfo) error {
	off := 0
	for off < len(data) {
		if len(data)-off < 8 {
			return invalid("trailing sample-entry bytes")
		}
		sz := int(binary.BigEndian.Uint32(data[off:]))
		if sz < 8 || sz > len(data)-off {
			return invalid("invalid codec configuration box")
		}
		if string(data[off+4:off+8]) == typ {
			config := data[off+8 : off+sz]
			if len(config) > maxConfigSize {
				return invalid("codec configuration is too large")
			}
			if typ == "avcC" {
				return parseAVCC(config, info)
			}
			return parseHVCC(config, info)
		}
		off += sz
	}
	return invalid("missing codec configuration")
}

func parseAVCC(v []byte, info *trackInfo) error {
	if len(v) < 7 || v[0] != 1 || v[4]&3 != 3 {
		return invalid("unsupported avcC")
	}
	off, spsCount := 6, int(v[5]&31)
	if spsCount == 0 {
		return invalid("avcC has no SPS")
	}
	var err error
	info.sps, off, err = readParameterSets(v, off, spsCount)
	if err != nil {
		return err
	}
	if off >= len(v) {
		return truncated("avcC has no PPS count")
	}
	ppsCount := int(v[off])
	off++
	if ppsCount == 0 {
		return invalid("avcC has no PPS")
	}
	info.pps, off, err = readParameterSets(v, off, ppsCount)
	if err != nil {
		return err
	}
	if off == len(v) {
		return nil
	}
	// High-profile AVC records may carry the optional SPS extension fields.
	switch v[1] {
	case 100, 110, 122, 144:
		if len(v)-off < 4 {
			return truncated("short avcC extension")
		}
		extCount := int(v[off+3])
		off += 4
		for i := 0; i < extCount; i++ {
			if len(v)-off < 2 {
				return truncated("short avcC extension length")
			}
			n := int(binary.BigEndian.Uint16(v[off:]))
			off += 2
			if n == 0 || n > len(v)-off {
				return invalid("invalid avcC extension length")
			}
			off += n
		}
	default:
		return invalid("avcC has trailing data")
	}
	if off != len(v) {
		return invalid("avcC has trailing data")
	}
	return nil
}

func parseHVCC(v []byte, info *trackInfo) error {
	if len(v) < 23 || v[0] != 1 || v[21]&3 != 3 {
		return invalid("unsupported hvcC")
	}
	arrays := int(v[22])
	if arrays == 0 {
		return invalid("hvcC has no parameter arrays")
	}
	off := 23
	for i := 0; i < arrays; i++ {
		if len(v)-off < 3 {
			return truncated("short hvcC array")
		}
		typ := v[off] & 0x3f
		count := int(binary.BigEndian.Uint16(v[off+1:]))
		off += 3
		for j := 0; j < count; j++ {
			if len(v)-off < 2 {
				return truncated("short hvcC NAL length")
			}
			n := int(binary.BigEndian.Uint16(v[off:]))
			off += 2
			if n == 0 || n > len(v)-off {
				return invalid("invalid hvcC NAL length")
			}
			param := append([]byte(nil), v[off:off+n]...)
			switch typ {
			case 32:
				if len(info.vps) == 0 {
					info.vps = param
				}
			case 33:
				if len(info.sps) == 0 {
					info.sps = param
				}
			case 34:
				if len(info.pps) == 0 {
					info.pps = param
				}
			}
			off += n
		}
	}
	if off != len(v) {
		return invalid("hvcC has trailing data")
	}
	if len(info.vps) == 0 || len(info.sps) == 0 || len(info.pps) == 0 {
		return invalid("hvcC is missing VPS, SPS, or PPS")
	}
	return nil
}

func readParameterSets(v []byte, off, count int) ([]byte, int, error) {
	var first []byte
	for i := 0; i < count; i++ {
		if len(v)-off < 2 {
			return nil, off, truncated("short parameter-set length")
		}
		n := int(binary.BigEndian.Uint16(v[off:]))
		off += 2
		if n == 0 || n > len(v)-off {
			return nil, off, invalid("invalid parameter-set length")
		}
		if first == nil {
			first = append([]byte(nil), v[off:off+n]...)
		}
		off += n
	}
	return first, off, nil
}

func (p *parser) parseMoof(moof box, track *trackInfo) ([]cascade.SegmentSample, error) {
	var trafs []box
	for off := moof.payload; off < moof.end; {
		b, err := p.readBox(off, moof.end, false)
		if err != nil {
			return nil, err
		}
		if b.typ == "traf" {
			trafs = append(trafs, b)
		}
		off = b.end
	}

	var out []cascade.SegmentSample
	for _, b := range trafs {
		part, err := p.parseTraf(b, moof, track, len(trafs) == 1)
		if err != nil {
			return nil, err
		}
		if len(out)+len(part) > maxSamples {
			return nil, invalid("sample count exceeds limit")
		}
		out = append(out, part...)
	}
	return out, nil
}

func (p *parser) parseTraf(traf, moof box, track *trackInfo, singleTraf bool) ([]cascade.SegmentSample, error) {
	var tfhd, trun []box
	for off := traf.payload; off < traf.end; {
		b, err := p.readBox(off, traf.end, false)
		if err != nil {
			return nil, err
		}
		switch b.typ {
		case "tfhd":
			tfhd = append(tfhd, b)
		case "trun":
			if len(trun) >= maxSamples {
				return nil, invalid("too many trun boxes")
			}
			trun = append(trun, b)
		}
		off = b.end
	}
	if len(tfhd) != 1 || len(trun) == 0 {
		return nil, invalid("traf missing tfhd or trun")
	}
	flags, data, err := p.fullBox(tfhd[0])
	if err != nil {
		return nil, err
	}
	if len(data) < 4 {
		return nil, invalid("short tfhd")
	}
	id := binary.BigEndian.Uint32(data[:4])
	if id != track.id {
		return nil, nil
	}
	base := moof.start
	off := 4
	defaultDur, defaultSize, defaultFlags := track.defaultDur, track.defaultSize, track.defaultFlags
	if flags&0x000001 != 0 {
		if len(data)-off < 8 {
			return nil, truncated("short tfhd base offset")
		}
		v := binary.BigEndian.Uint64(data[off:])
		if v > uint64(^uint64(0)>>1) {
			return nil, invalid("tfhd base offset overflows")
		}
		base, off = int64(v), off+8
	}
	if flags&0x000002 != 0 {
		if len(data)-off < 4 {
			return nil, truncated("short tfhd sample description index")
		}
		off += 4
	}
	if flags&0x000008 != 0 {
		if len(data)-off < 4 {
			return nil, truncated("short tfhd duration")
		}
		defaultDur = binary.BigEndian.Uint32(data[off:])
		off += 4
	}
	if flags&0x000010 != 0 {
		if len(data)-off < 4 {
			return nil, truncated("short tfhd size")
		}
		defaultSize = binary.BigEndian.Uint32(data[off:])
		off += 4
	}
	if flags&0x000020 != 0 {
		if len(data)-off < 4 {
			return nil, truncated("short tfhd flags")
		}
		defaultFlags = binary.BigEndian.Uint32(data[off:])
		off += 4
	}
	if flags&0x000001 == 0 && flags&0x020000 == 0 {
		if !singleTraf {
			return nil, invalid("implicit tfhd base requires a single traf")
		}
		firstFlags, _, err := p.fullBox(trun[0])
		if err != nil {
			return nil, err
		}
		if firstFlags&1 == 0 {
			return nil, invalid("implicit tfhd base requires first trun data offset")
		}
		base = moof.start
	}

	var out []cascade.SegmentSample
	var cursor int64
	for _, b := range trun {
		flags, data, err := p.fullBox(b)
		if err != nil {
			return nil, err
		}
		if len(data) < 4 {
			return nil, invalid("short trun")
		}
		count := binary.BigEndian.Uint32(data[:4])
		if count == 0 || count > maxSamples {
			return nil, invalid("invalid trun sample count")
		}
		if uint64(len(out))+uint64(count) > maxSamples {
			return nil, invalid("sample count exceeds limit")
		}
		if err := p.addMetadata(int(count)); err != nil {
			return nil, err
		}
		off := 4
		dataOffset := cursor
		if flags&1 != 0 {
			if len(data)-off < 4 {
				return nil, truncated("short trun data offset")
			}
			dataOffset = int64(int32(binary.BigEndian.Uint32(data[off:])))
			off += 4
		}
		var firstFlags *uint32
		if flags&4 != 0 {
			if len(data)-off < 4 {
				return nil, truncated("short trun first flags")
			}
			value := binary.BigEndian.Uint32(data[off:])
			firstFlags = &value
			off += 4
		}
		part, err := parseRunSamples(data[off:], flags, count, base, dataOffset, defaultDur, defaultSize, defaultFlags, firstFlags)
		if err != nil {
			return nil, err
		}
		out = append(out, part...)
		if len(part) > 0 {
			last := part[len(part)-1]
			end, ok := addInt64(last.Offset, int64(last.Size))
			if !ok {
				return nil, invalid("sample offset overflows")
			}
			cursor = end - base
		}
	}
	return out, nil
}

func parseRunSamples(data []byte, flags uint32, count uint32, base, dataOffset int64, defaultDur, defaultSize, defaultFlags uint32, firstFlags *uint32) ([]cascade.SegmentSample, error) {
	entrySize := 0
	if flags&0x100 != 0 {
		entrySize += 4
	}
	if flags&0x200 != 0 {
		entrySize += 4
	}
	if flags&0x400 != 0 {
		entrySize += 4
	}
	if flags&0x800 != 0 {
		entrySize += 4
	}
	if uint64(count)*uint64(entrySize) > uint64(len(data)) {
		return nil, invalid("trun table is short")
	}
	pos, ok := addInt64(base, dataOffset)
	if !ok {
		return nil, invalid("sample offset overflows")
	}
	out := make([]cascade.SegmentSample, 0, count)
	for i := uint32(0); i < count; i++ {
		var dur, size, sampleFlags uint32 = defaultDur, defaultSize, defaultFlags
		if flags&0x100 != 0 {
			dur = binary.BigEndian.Uint32(data)
			data = data[4:]
		}
		if flags&0x200 != 0 {
			size = binary.BigEndian.Uint32(data)
			data = data[4:]
		}
		if flags&0x400 != 0 {
			sampleFlags = binary.BigEndian.Uint32(data)
			data = data[4:]
		} else if i == 0 && firstFlags != nil {
			sampleFlags = *firstFlags
		}
		if flags&0x800 != 0 {
			data = data[4:]
		}
		if dur == 0 || size == 0 {
			return nil, invalid("sample duration or size is zero")
		}
		if size > maxSampleSize {
			return nil, invalid("sample is too large")
		}
		end, ok := addInt64(pos, int64(size))
		if !ok {
			return nil, invalid("sample offset overflows")
		}
		out = append(out, cascade.SegmentSample{Offset: pos, Size: size, Duration: dur, IsKeyFrame: sampleFlags&0x10000 == 0})
		pos = end
	}
	if len(data) != 0 {
		return nil, invalid("trun has trailing sample data")
	}
	return out, nil
}

func (p *parser) fullBox(b box) (uint32, []byte, error) {
	v, err := p.readSmall(b, maxConfigSize)
	if err != nil {
		return 0, nil, err
	}
	if len(v) < 4 {
		return 0, nil, invalid("short full box")
	}
	return binary.BigEndian.Uint32(v), v[4:], nil
}

func (p *parser) readSmall(b box, max int) ([]byte, error) {
	if b.end-b.payload > int64(max) {
		return nil, invalid("box payload is too large")
	}
	v := make([]byte, b.end-b.payload)
	if _, err := p.r.ReadAt(v, b.payload); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, truncated("short box payload")
		}
		return nil, err
	}
	return v, nil
}

func (p *parser) readPrefix(b box, n int) ([]byte, error) {
	if n < 0 || n > 128 {
		return nil, invalid("prefix size exceeds limit")
	}
	if b.end-b.payload < int64(n) {
		return nil, truncated("short box prefix")
	}
	v := make([]byte, n)
	if _, err := p.r.ReadAt(v, b.payload); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, truncated("short box prefix")
		}
		return nil, err
	}
	return v, nil
}

func (p *parser) readBox(off, limit int64, allowZero bool) (box, error) {
	if p.boxCount >= maxBoxes {
		return box{}, invalid("box budget exceeded")
	}
	p.boxCount++
	if off < 0 || limit < off || limit-off < 8 {
		return box{}, truncated("short box header")
	}
	var h [16]byte
	if _, err := p.r.ReadAt(h[:8], off); err != nil {
		return box{}, truncated("short box header")
	}
	size := uint64(binary.BigEndian.Uint32(h[:4]))
	header := int64(8)
	if string(h[4:8]) == "" {
		return box{}, invalid("empty box type")
	}
	if size == 1 {
		if limit-off < 16 {
			return box{}, truncated("short extended box header")
		}
		if _, err := p.r.ReadAt(h[8:16], off+8); err != nil {
			return box{}, truncated("short extended box header")
		}
		size = binary.BigEndian.Uint64(h[8:16])
		header = 16
	} else if size == 0 {
		if !allowZero {
			return box{}, invalid("zero-sized nested box")
		}
		size = uint64(limit - off)
	}
	if size < uint64(header) {
		return box{}, invalid("box size is smaller than its header")
	}
	if size > uint64(limit-off) {
		return box{}, truncated("box exceeds file")
	}
	end := off + int64(size)
	if end <= off {
		return box{}, invalid("box offset overflows")
	}
	if p.onReadBox != nil {
		hook := p.onReadBox
		p.onReadBox = nil
		hook()
	}
	return box{start: off, payload: off + header, end: end, typ: string(h[4:8])}, nil
}

func (p *parser) addMetadata(n int) error {
	if n < 0 || p.metaCount > maxMetadata-n {
		return invalid("metadata budget exceeded")
	}
	p.metaCount += n
	return nil
}

func inMdat(start, end int64, mdats []box) bool {
	for _, b := range mdats {
		if start >= b.payload && end <= b.end {
			return true
		}
	}
	return false
}

func addInt64(a, b int64) (int64, bool) {
	if b > 0 && a > int64(^uint64(0)>>1)-b {
		return 0, false
	}
	if b < 0 && a < -int64(^uint64(0)>>1)-1-b {
		return 0, false
	}
	return a + b, true
}

func invalid(msg string) error   { return fmt.Errorf("%w: %s", ErrInvalid, msg) }
func truncated(msg string) error { return fmt.Errorf("%w: %s", ErrTruncated, msg) }

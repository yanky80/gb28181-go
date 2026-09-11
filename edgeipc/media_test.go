package edgeipc

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
)

func TestMediaFrameRoundTripUsesFrozenHeader(t *testing.T) {
	want := MediaFrame{
		Codec:    CodecH265,
		Flags:    FlagIDR | FlagDiscontinuity,
		CameraID: "camera-7",
		PTS90kHz: 123456789,
		Sequence: 1<<40 + 42,
		Payload:  h265IDRAU(),
	}
	wire, err := MarshalMediaFrame(want)
	if err != nil {
		t.Fatal(err)
	}
	if got := binary.BigEndian.Uint16(wire[8:10]); got != MediaHeaderSize {
		t.Fatalf("header_len = %d, want %d", got, MediaHeaderSize)
	}
	if got := binary.BigEndian.Uint16(wire[10:12]); got != uint16(len(want.CameraID)) {
		t.Fatalf("camera_id_len = %d, want %d", got, len(want.CameraID))
	}
	if got := binary.BigEndian.Uint32(wire[12:16]); got != uint32(len(want.Payload)) {
		t.Fatalf("payload_len = %d, want %d", got, len(want.Payload))
	}
	if got := binary.BigEndian.Uint64(wire[16:24]); got != want.PTS90kHz {
		t.Fatalf("pts = %d, want %d", got, want.PTS90kHz)
	}
	if got := binary.BigEndian.Uint64(wire[24:32]); got != want.Sequence {
		t.Fatalf("sequence = %d, want %d", got, want.Sequence)
	}

	got, err := ParseMediaFrame(wire)
	if err != nil {
		t.Fatal(err)
	}
	if got.Codec != want.Codec || got.Flags != want.Flags || got.CameraID != want.CameraID ||
		got.PTS90kHz != want.PTS90kHz || got.Sequence != want.Sequence || !bytes.Equal(got.Payload, want.Payload) {
		t.Fatalf("round trip = %+v, want %+v", got, want)
	}
}

func TestMediaReaderHandlesShortReadsAndPipelining(t *testing.T) {
	first, err := MarshalMediaFrame(MediaFrame{Codec: CodecH264, CameraID: "cam-a", Sequence: 1, Payload: h264AU(0x41)})
	if err != nil {
		t.Fatal(err)
	}
	second, err := MarshalMediaFrame(MediaFrame{Codec: CodecH264, CameraID: "cam-b", Sequence: 2, Payload: h264AU(0x41)})
	if err != nil {
		t.Fatal(err)
	}
	reader := NewMediaReader(&chunkReader{data: append(first, second...), size: 1})
	gotFirst, err := reader.Read()
	if err != nil {
		t.Fatal(err)
	}
	gotSecond, err := reader.Read()
	if err != nil {
		t.Fatal(err)
	}
	if gotFirst.CameraID != "cam-a" || gotSecond.CameraID != "cam-b" {
		t.Fatalf("frames = %+v, %+v", gotFirst, gotSecond)
	}
}

func TestConfiguredMediaReaderRejectsPeerCodecBeforePayloadRead(t *testing.T) {
	wire, err := MarshalMediaFrame(MediaFrame{Codec: CodecH264, CameraID: "cam-1", Payload: h264AU(0x41)})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := NewMediaReaderForCodec(bytes.NewReader(wire), CodecH265)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reader.Read(); !errors.Is(err, ErrCodecMismatch) {
		t.Fatalf("error = %v, want codec mismatch", err)
	}
}

func TestMediaReaderHonorsHostPayloadLimitBeforeAllocation(t *testing.T) {
	wire, err := MarshalMediaFrame(MediaFrame{Codec: CodecH264, CameraID: "cam-1", Payload: h264AU(0x41)})
	if err != nil {
		t.Fatal(err)
	}
	reader, err := NewMediaReaderWithMaxPayload(bytes.NewReader(wire), len(h264AU(0x41))-1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reader.Read(); !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("error = %v, want payload-too-large", err)
	}
}

func TestMediaFrameRejectsInvalidHeaderBeforePayloadAllocation(t *testing.T) {
	base, err := MarshalMediaFrame(MediaFrame{Codec: CodecH264, CameraID: "cam-1", Payload: h264AU(0x41)})
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name string
		edit func([]byte)
		want error
	}{
		{"magic", func(b []byte) { b[0] = 'x' }, ErrInvalidMagic},
		{"version", func(b []byte) { b[4] = 2 }, ErrUnsupportedVersion},
		{"flags", func(b []byte) { b[5] = 0x04 }, ErrInvalidFlags},
		{"codec", func(b []byte) { b[6] = 3 }, ErrUnsupportedCodec},
		{"reserved", func(b []byte) { b[7] = 1 }, ErrNonZeroReserved},
		{"header length", func(b []byte) { binary.BigEndian.PutUint16(b[8:10], 31) }, ErrInvalidHeader},
		{"camera length zero", func(b []byte) { binary.BigEndian.PutUint16(b[10:12], 0) }, ErrInvalidCameraID},
		{"camera length", func(b []byte) { binary.BigEndian.PutUint16(b[10:12], 65) }, ErrInvalidCameraID},
		{"payload length", func(b []byte) { binary.BigEndian.PutUint32(b[12:16], MaxMediaPayload+1) }, ErrPayloadTooLarge},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			data := append([]byte(nil), base...)
			tt.edit(data)
			_, err := ParseMediaFrame(data)
			if !errors.Is(err, tt.want) {
				t.Fatalf("error = %v, want %v", err, tt.want)
			}
		})
	}

	headerOnly := append([]byte(nil), base[:MediaHeaderSize]...)
	binary.BigEndian.PutUint32(headerOnly[12:16], MaxMediaPayload+1)
	if _, err := NewMediaReader(bytes.NewReader(headerOnly)).Read(); !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("stream error = %v, want %v", err, ErrPayloadTooLarge)
	}
}

func TestMediaFrameRejectsInvalidCameraUTF8AndTruncatedPayload(t *testing.T) {
	wire, err := MarshalMediaFrame(MediaFrame{Codec: CodecH264, CameraID: "cam-1", Payload: h264AU(0x41)})
	if err != nil {
		t.Fatal(err)
	}
	badUTF8 := append([]byte(nil), wire...)
	badUTF8[MediaHeaderSize] = 0xff
	if _, err := ParseMediaFrame(badUTF8); !errors.Is(err, ErrInvalidUTF8) {
		t.Fatalf("UTF-8 error = %v, want invalid UTF-8", err)
	}
	if _, err := NewMediaReader(bytes.NewReader(wire[:len(wire)-1])).Read(); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("truncated error = %v, want unexpected EOF", err)
	}
}

func TestH265IDRRequiresVPSPSPPSAndIDR(t *testing.T) {
	frame := MediaFrame{Codec: CodecH265, Flags: FlagIDR, CameraID: "cam-1", Payload: h265IDRAU()}
	if _, err := MarshalMediaFrame(frame); err != nil {
		t.Fatal(err)
	}
	frame.Payload = []byte{0, 0, 1, 0x40, 1, 0, 0, 1, 0x42, 1, 0, 0, 1, 0x44, 1, 0, 0, 1, 0x02}
	if _, err := MarshalMediaFrame(frame); !errors.Is(err, ErrInvalidAccessUnit) {
		t.Fatalf("missing IDR error = %v", err)
	}
	frame.Payload = []byte{0, 0, 1, 0x40, 1, 0, 0, 1, 0x42, 1, 0, 0, 1, 0x44, 1, 0, 0, 1, 0x26}
	if _, err := MarshalMediaFrame(frame); !errors.Is(err, ErrInvalidAccessUnit) {
		t.Fatalf("truncated H.265 header error = %v", err)
	}
}

func TestH264IDRRejectsForbiddenNAL(t *testing.T) {
	frame := MediaFrame{Codec: CodecH264, Flags: FlagIDR, CameraID: "cam-1", Payload: h264AU(0x65)}
	if _, err := MarshalMediaFrame(frame); !errors.Is(err, ErrInvalidAccessUnit) {
		t.Fatalf("missing SPS/PPS error = %v, want invalid AU", err)
	}
	frame.Payload = h264IDRAU()
	if _, err := MarshalMediaFrame(frame); err != nil {
		t.Fatal(err)
	}
	frame.Payload = h264AU(0xe5)
	if _, err := MarshalMediaFrame(frame); !errors.Is(err, ErrInvalidAccessUnit) {
		t.Fatalf("error = %v, want invalid AU", err)
	}
}

func TestMediaFrameRequiresAnnexBAccessUnit(t *testing.T) {
	for _, codec := range []Codec{CodecH264, CodecH265} {
		if _, err := MarshalMediaFrame(MediaFrame{Codec: codec, CameraID: "cam-1", Payload: []byte{1, 2}}); !errors.Is(err, ErrInvalidAccessUnit) {
			t.Fatalf("codec %d error = %v, want invalid AU", codec, err)
		}
	}
}

func TestMediaFrameRejectsUnknownOrIncompleteAccessUnit(t *testing.T) {
	for _, tc := range []struct {
		name  string
		codec Codec
		au    []byte
	}{
		{name: "h264 unknown nal", codec: CodecH264, au: h264AU(0x1e)},
		{name: "h264 parameter set only", codec: CodecH264, au: h264AU(0x67)},
		{name: "h265 reserved nal", codec: CodecH265, au: []byte{0, 0, 1, 0x60, 1}},
		{name: "h265 parameter set only", codec: CodecH265, au: []byte{0, 0, 1, 0x40, 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := MarshalMediaFrame(MediaFrame{Codec: tc.codec, CameraID: "cam-1", Payload: tc.au})
			if !errors.Is(err, ErrInvalidAccessUnit) {
				t.Fatalf("error = %v, want invalid AU", err)
			}
		})
	}
}

func TestMediaPayloadUpperBoundIsAccepted(t *testing.T) {
	payload := make([]byte, MaxMediaPayload)
	copy(payload, h264AU(0x41))
	if _, err := MarshalMediaFrame(MediaFrame{Codec: CodecH264, CameraID: "cam-1", Payload: payload}); err != nil {
		t.Fatalf("exact payload limit rejected: %v", err)
	}
}

func h264AU(nal byte) []byte { return []byte{0, 0, 1, nal} }

func h264IDRAU() []byte {
	return []byte{0, 0, 1, 0x67, 0, 0, 1, 0x68, 0, 0, 1, 0x65}
}

func h265IDRAU() []byte {
	return []byte{0, 0, 1, 0x40, 1, 0, 0, 1, 0x42, 1, 0, 0, 1, 0x44, 1, 0, 0, 1, 0x26, 1}
}

type chunkReader struct {
	data []byte
	size int
}

func (r *chunkReader) Read(p []byte) (int, error) {
	if len(r.data) == 0 {
		return 0, io.EOF
	}
	n := r.size
	if n > len(p) {
		n = len(p)
	}
	if n > len(r.data) {
		n = len(r.data)
	}
	copy(p, r.data[:n])
	r.data = r.data[n:]
	return n, nil
}

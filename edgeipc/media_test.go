package edgeipc

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
)

func TestMediaFrameRoundTrip(t *testing.T) {
	want := MediaFrame{
		Codec:    CodecH265,
		Flags:    FlagIDR | FlagParameterSets,
		CameraID: 7,
		Epoch:    9,
		Sequence: 42,
		PTS:      123456789,
		Payload:  h265IDRAU(),
	}

	encoded, err := MarshalMediaFrame(want)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded)-MediaHeaderSize != len(want.Payload) {
		t.Fatalf("encoded length = %d, want header + %d", len(encoded), len(want.Payload))
	}
	if got := binary.BigEndian.Uint16(encoded[8:10]); got != want.CameraID {
		t.Fatalf("camera id bytes = %d, want %d", got, want.CameraID)
	}

	got, err := ParseMediaFrame(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if got.Codec != want.Codec || got.Flags != want.Flags || got.CameraID != want.CameraID ||
		got.Epoch != want.Epoch || got.Sequence != want.Sequence || got.PTS != want.PTS ||
		!bytes.Equal(got.Payload, want.Payload) {
		t.Fatalf("round trip = %+v, want %+v", got, want)
	}
}

func TestReadMediaFrameHandlesShortReadsAndKeepsPipelinedFrame(t *testing.T) {
	first := MediaFrame{Codec: CodecH264, CameraID: 1, Sequence: 1, Payload: []byte{0, 0, 1, 0x65}}
	second := MediaFrame{Codec: CodecH264, CameraID: 2, Sequence: 2, Payload: []byte{0, 0, 1, 0x41}}
	one, err := MarshalMediaFrame(first)
	if err != nil {
		t.Fatal(err)
	}
	two, err := MarshalMediaFrame(second)
	if err != nil {
		t.Fatal(err)
	}

	r := NewMediaReader(&chunkReader{data: append(one, two...), size: 1})
	gotOne, err := r.Read()
	if err != nil {
		t.Fatal(err)
	}
	gotTwo, err := r.Read()
	if err != nil {
		t.Fatal(err)
	}
	if gotOne.CameraID != first.CameraID || gotTwo.CameraID != second.CameraID {
		t.Fatalf("read frames = %+v, %+v", gotOne, gotTwo)
	}
}

func TestConfiguredMediaReaderRejectsPeerCodecBeforePayloadRead(t *testing.T) {
	wire, err := MarshalMediaFrame(MediaFrame{Codec: CodecH264, CameraID: 1, Payload: []byte{0, 0, 1, 0x41}})
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

func TestMediaFrameRejectsInvalidHeaderBeforePayloadAllocation(t *testing.T) {
	tests := []struct {
		name string
		edit func([]byte)
		want error
	}{
		{name: "magic", edit: func(b []byte) { b[0] = 'x' }, want: ErrInvalidMagic},
		{name: "version", edit: func(b []byte) { b[4] = 2 }, want: ErrUnsupportedVersion},
		{name: "header length", edit: func(b []byte) { b[5] = 31 }, want: ErrInvalidHeader},
		{name: "codec", edit: func(b []byte) { b[6] = 3 }, want: ErrUnsupportedCodec},
		{name: "flags", edit: func(b []byte) { b[7] = 0x80 }, want: ErrInvalidFlags},
		{name: "camera", edit: func(b []byte) { binary.BigEndian.PutUint16(b[8:10], 65) }, want: ErrInvalidCameraID},
		{name: "reserved", edit: func(b []byte) { b[10] = 1 }, want: ErrNonZeroReserved},
		{name: "payload length", edit: func(b []byte) { binary.BigEndian.PutUint32(b[28:32], MaxMediaPayload+1) }, want: ErrPayloadTooLarge},
	}
	base, err := MarshalMediaFrame(MediaFrame{Codec: CodecH264, CameraID: 1, Payload: []byte{0, 0, 1, 0x41}})
	if err != nil {
		t.Fatal(err)
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

	data := append([]byte(nil), base[:MediaHeaderSize]...)
	binary.BigEndian.PutUint32(data[28:32], MaxMediaPayload+1)
	_, err = NewMediaReader(bytes.NewReader(data)).Read()
	if !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("stream error = %v, want %v", err, ErrPayloadTooLarge)
	}
}

func TestMediaFrameRejectsTruncatedPayload(t *testing.T) {
	encoded, err := MarshalMediaFrame(MediaFrame{Codec: CodecH264, CameraID: 1, Payload: []byte{0, 0, 1, 0x41}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = NewMediaReader(bytes.NewReader(encoded[:len(encoded)-1])).Read()
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("error = %v, want unexpected EOF", err)
	}
}

func TestH265IDRRequiresAnnexBParameterSetsAndIDR(t *testing.T) {
	good := MediaFrame{Codec: CodecH265, Flags: FlagIDR | FlagParameterSets, CameraID: 1, Payload: h265IDRAU()}
	if _, err := MarshalMediaFrame(good); err != nil {
		t.Fatalf("valid H.265 IDR rejected: %v", err)
	}
	good.Payload = []byte{0, 0, 1, 0x40, 0, 0, 1, 0x42, 0, 0, 1, 0x44, 0, 0, 1, 0x02}
	if _, err := MarshalMediaFrame(good); !errors.Is(err, ErrInvalidAccessUnit) {
		t.Fatalf("non-IDR H.265 AU error = %v, want invalid AU", err)
	}
	good.Payload = []byte{0, 0, 1, 0x26, 0}
	if _, err := MarshalMediaFrame(good); !errors.Is(err, ErrInvalidAccessUnit) {
		t.Fatalf("H.265 IDR without parameter sets error = %v, want invalid AU", err)
	}
	good.Payload = []byte{0, 0, 1, 0x40, 1, 0, 0, 1, 0x42, 1, 0, 0, 1, 0x44, 1, 0, 0, 1, 0x26}
	if _, err := MarshalMediaFrame(good); !errors.Is(err, ErrInvalidAccessUnit) {
		t.Fatalf("truncated H.265 NAL header error = %v, want invalid AU", err)
	}
}

func TestH264IDRRequiresAnIDRAnnexBNAL(t *testing.T) {
	frame := MediaFrame{Codec: CodecH264, Flags: FlagIDR, CameraID: 1, Payload: []byte{0, 0, 1, 0x65}}
	if _, err := MarshalMediaFrame(frame); err != nil {
		t.Fatal(err)
	}
	frame.Payload = []byte{0, 0, 1, 0x41}
	if _, err := MarshalMediaFrame(frame); !errors.Is(err, ErrInvalidAccessUnit) {
		t.Fatalf("error = %v, want invalid AU", err)
	}
	frame.Payload = []byte{0, 0, 1, 0xe5}
	if _, err := MarshalMediaFrame(frame); !errors.Is(err, ErrInvalidAccessUnit) {
		t.Fatalf("forbidden H.264 NAL error = %v, want invalid AU", err)
	}
}

func TestMediaFrameRequiresAnnexBAccessUnit(t *testing.T) {
	for _, codec := range []Codec{CodecH264, CodecH265} {
		if _, err := MarshalMediaFrame(MediaFrame{Codec: codec, CameraID: 1, Payload: []byte{1, 2}}); !errors.Is(err, ErrInvalidAccessUnit) {
			t.Fatalf("codec %d error = %v, want invalid AU", codec, err)
		}
	}
}

func h265IDRAU() []byte {
	return []byte{
		0, 0, 1, 0x40, 1,
		0, 0, 1, 0x42, 1,
		0, 0, 1, 0x44, 1,
		0, 0, 1, 0x26, 1,
	}
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

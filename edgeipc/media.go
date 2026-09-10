// Package edgeipc contains the versioned, transport-independent Edge IPC v1
// wire format. It deliberately does not open sockets or manage camera state.
package edgeipc

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const (
	MediaHeaderSize = 32
	MaxMediaPayload = 8 << 20
	MaxControlLine  = 16 << 10

	ProtocolVersion byte = 1
	mediaMagic           = "EGAU"
)

var (
	ErrInvalidMagic       = errors.New("edgeipc: invalid media magic")
	ErrUnsupportedVersion = errors.New("edgeipc: unsupported protocol version")
	ErrInvalidHeader      = errors.New("edgeipc: invalid media header")
	ErrUnsupportedCodec   = errors.New("edgeipc: unsupported codec")
	ErrInvalidFlags       = errors.New("edgeipc: invalid media flags")
	ErrInvalidCameraID    = errors.New("edgeipc: camera id must be 1..64")
	ErrNonZeroReserved    = errors.New("edgeipc: reserved field is non-zero")
	ErrPayloadTooSmall    = errors.New("edgeipc: media payload is empty")
	ErrPayloadTooLarge    = errors.New("edgeipc: media payload exceeds 8 MiB")
	ErrInvalidAccessUnit  = errors.New("edgeipc: invalid codec access unit")
	ErrCodecMismatch      = errors.New("edgeipc: codec does not match the configured codec")
)

// Codec identifies the elementary video codec carried by a media frame.
type Codec byte

const (
	CodecH264    Codec = 1
	CodecH265    Codec = 2
	DefaultCodec       = CodecH265
)

// MediaFlags describe the access unit. Unknown bits are rejected.
type MediaFlags byte

const (
	FlagIDR MediaFlags = 1 << iota
	FlagParameterSets
)

// MediaFrame is one complete encoded access unit. PTS is in nanoseconds.
type MediaFrame struct {
	Codec    Codec
	Flags    MediaFlags
	CameraID uint16
	Epoch    uint32
	Sequence uint32
	PTS      uint64
	Payload  []byte
}

// MarshalMediaFrame encodes one EGAU frame with its fixed 32-byte header.
func MarshalMediaFrame(frame MediaFrame) ([]byte, error) {
	if err := ValidateMediaFrame(frame); err != nil {
		return nil, err
	}
	data := make([]byte, MediaHeaderSize+len(frame.Payload))
	writeMediaHeader(data[:MediaHeaderSize], frame)
	copy(data[MediaHeaderSize:], frame.Payload)
	return data, nil
}

// ValidateMediaFrame checks the fields that are visible on the wire.
func ValidateMediaFrame(frame MediaFrame) error {
	if !validCodec(frame.Codec) {
		return ErrUnsupportedCodec
	}
	if frame.Flags & ^(FlagIDR|FlagParameterSets) != 0 {
		return ErrInvalidFlags
	}
	if frame.CameraID < 1 || frame.CameraID > 64 {
		return ErrInvalidCameraID
	}
	if len(frame.Payload) < 1 {
		return ErrPayloadTooSmall
	}
	if len(frame.Payload) > MaxMediaPayload {
		return ErrPayloadTooLarge
	}
	return validateMediaAccessUnit(frame)
}

// ParseMediaFrame parses exactly one complete frame. The returned payload
// aliases data, so callers that retain it should copy it when needed.
func ParseMediaFrame(data []byte) (MediaFrame, error) {
	if len(data) < MediaHeaderSize {
		return MediaFrame{}, io.ErrUnexpectedEOF
	}
	frame, payloadLen, err := parseMediaHeader(data[:MediaHeaderSize])
	if err != nil {
		return MediaFrame{}, err
	}
	if int64(MediaHeaderSize)+int64(payloadLen) != int64(len(data)) {
		if int64(len(data)) < int64(MediaHeaderSize)+int64(payloadLen) {
			return MediaFrame{}, io.ErrUnexpectedEOF
		}
		return MediaFrame{}, fmt.Errorf("edgeipc: trailing bytes after media frame")
	}
	frame.Payload = data[MediaHeaderSize:]
	if err := validateMediaAccessUnit(frame); err != nil {
		return MediaFrame{}, err
	}
	return frame, nil
}

// WriteMediaFrame writes one frame, handling short writes.
func WriteMediaFrame(w io.Writer, frame MediaFrame) error {
	if err := ValidateMediaFrame(frame); err != nil {
		return err
	}
	var header [MediaHeaderSize]byte
	writeMediaHeader(header[:], frame)
	if err := writeAll(w, header[:]); err != nil {
		return err
	}
	return writeAll(w, frame.Payload)
}

// MediaReader reads complete EGAU frames from a stream without reading past a
// frame. Header validation happens before the payload allocation.
type MediaReader struct {
	r          io.Reader
	expected   Codec
	configured bool
}

func NewMediaReader(r io.Reader) *MediaReader { return &MediaReader{r: r} }

// NewMediaReaderForCodec makes a reader that rejects frames from a peer that
// does not use the codec agreed during the handshake.
func NewMediaReaderForCodec(r io.Reader, codec Codec) (*MediaReader, error) {
	if !validCodec(codec) {
		return nil, ErrUnsupportedCodec
	}
	return &MediaReader{r: r, expected: codec, configured: true}, nil
}

func (r *MediaReader) Read() (MediaFrame, error) {
	var header [MediaHeaderSize]byte
	if _, err := io.ReadFull(r.r, header[:]); err != nil {
		return MediaFrame{}, err
	}
	frame, payloadLen, err := parseMediaHeader(header[:])
	if err != nil {
		return MediaFrame{}, err
	}
	if r.configured && frame.Codec != r.expected {
		return MediaFrame{}, ErrCodecMismatch
	}
	frame.Payload = make([]byte, payloadLen)
	if _, err := io.ReadFull(r.r, frame.Payload); err != nil {
		return MediaFrame{}, err
	}
	if err := validateMediaAccessUnit(frame); err != nil {
		return MediaFrame{}, err
	}
	return frame, nil
}

func parseMediaHeader(header []byte) (MediaFrame, uint32, error) {
	if string(header[:4]) != mediaMagic {
		return MediaFrame{}, 0, ErrInvalidMagic
	}
	if header[4] != ProtocolVersion {
		return MediaFrame{}, 0, ErrUnsupportedVersion
	}
	if header[5] != MediaHeaderSize {
		return MediaFrame{}, 0, ErrInvalidHeader
	}
	codec := Codec(header[6])
	if !validCodec(codec) {
		return MediaFrame{}, 0, ErrUnsupportedCodec
	}
	flags := MediaFlags(header[7])
	if flags&^(FlagIDR|FlagParameterSets) != 0 {
		return MediaFrame{}, 0, ErrInvalidFlags
	}
	cameraID := binary.BigEndian.Uint16(header[8:10])
	if cameraID < 1 || cameraID > 64 {
		return MediaFrame{}, 0, ErrInvalidCameraID
	}
	if binary.BigEndian.Uint16(header[10:12]) != 0 {
		return MediaFrame{}, 0, ErrNonZeroReserved
	}
	payloadLen := binary.BigEndian.Uint32(header[28:32])
	if payloadLen == 0 {
		return MediaFrame{}, 0, ErrPayloadTooSmall
	}
	if payloadLen > MaxMediaPayload {
		return MediaFrame{}, 0, ErrPayloadTooLarge
	}
	return MediaFrame{
		Codec:    codec,
		Flags:    flags,
		CameraID: cameraID,
		Epoch:    binary.BigEndian.Uint32(header[12:16]),
		Sequence: binary.BigEndian.Uint32(header[16:20]),
		PTS:      binary.BigEndian.Uint64(header[20:28]),
	}, payloadLen, nil
}

func writeMediaHeader(header []byte, frame MediaFrame) {
	copy(header[:4], mediaMagic)
	header[4] = ProtocolVersion
	header[5] = MediaHeaderSize
	header[6] = byte(frame.Codec)
	header[7] = byte(frame.Flags)
	binary.BigEndian.PutUint16(header[8:10], frame.CameraID)
	binary.BigEndian.PutUint32(header[12:16], frame.Epoch)
	binary.BigEndian.PutUint32(header[16:20], frame.Sequence)
	binary.BigEndian.PutUint64(header[20:28], frame.PTS)
	binary.BigEndian.PutUint32(header[28:32], uint32(len(frame.Payload)))
}

func validCodec(codec Codec) bool { return codec == CodecH264 || codec == CodecH265 }

func validateMediaAccessUnit(frame MediaFrame) error {
	if !validAccessUnit(frame.Codec, frame.Payload) {
		return ErrInvalidAccessUnit
	}
	if frame.Flags&FlagIDR != 0 && !validKeyframe(frame.Codec, frame.Payload) {
		return ErrInvalidAccessUnit
	}
	return nil
}

func writeAll(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if n > 0 {
			data = data[n:]
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

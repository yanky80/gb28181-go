// Package edgeipc contains the transport-independent Edge IPC v1 wire format.
// It deliberately does not open sockets or manage camera state.
package edgeipc

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"unicode/utf8"
)

const (
	MediaHeaderSize      = 32
	MaxMediaPayload      = 8 << 20
	ProtocolVersion byte = 1
	mediaMagic           = "EGAU"
)

var (
	ErrInvalidMagic       = errors.New("edgeipc: invalid media magic")
	ErrUnsupportedVersion = errors.New("edgeipc: unsupported protocol version")
	ErrInvalidHeader      = errors.New("edgeipc: invalid media header")
	ErrUnsupportedCodec   = errors.New("edgeipc: unsupported codec")
	ErrInvalidFlags       = errors.New("edgeipc: invalid media flags")
	ErrInvalidCameraID    = errors.New("edgeipc: camera id must be 1..64 bytes")
	ErrInvalidUTF8        = errors.New("edgeipc: camera id is not valid UTF-8")
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

// MediaFlags are the two defined EGAU flag bits.
type MediaFlags byte

const (
	FlagIDR MediaFlags = 1 << iota
	FlagDiscontinuity
)

// MediaFrame is one complete encoded access unit. PTS90kHz uses the GB media
// clock, and CameraID is the UTF-8 identifier carried after the header.
type MediaFrame struct {
	Codec    Codec
	Flags    MediaFlags
	CameraID string
	PTS90kHz uint64
	Sequence uint64
	Payload  []byte
}

// MarshalMediaFrame encodes one EGAU frame with its fixed 32-byte header.
func MarshalMediaFrame(frame MediaFrame) ([]byte, error) {
	if err := ValidateMediaFrame(frame); err != nil {
		return nil, err
	}
	cameraID := []byte(frame.CameraID)
	data := make([]byte, MediaHeaderSize+len(cameraID)+len(frame.Payload))
	writeMediaHeader(data[:MediaHeaderSize], frame, uint16(len(cameraID)))
	copy(data[MediaHeaderSize:], cameraID)
	copy(data[MediaHeaderSize+len(cameraID):], frame.Payload)
	return data, nil
}

// ValidateMediaFrame checks all fields that are visible on the wire.
func ValidateMediaFrame(frame MediaFrame) error {
	if !validCodec(frame.Codec) {
		return ErrUnsupportedCodec
	}
	if frame.Flags&^(FlagIDR|FlagDiscontinuity) != 0 {
		return ErrInvalidFlags
	}
	if !validCameraID(frame.CameraID) {
		return ErrInvalidCameraID
	}
	if len(frame.Payload) == 0 {
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
	frame, cameraLen, payloadLen, err := parseMediaHeader(data[:MediaHeaderSize])
	if err != nil {
		return MediaFrame{}, err
	}
	total := int64(MediaHeaderSize) + int64(cameraLen) + int64(payloadLen)
	if int64(len(data)) != total {
		if int64(len(data)) < total {
			return MediaFrame{}, io.ErrUnexpectedEOF
		}
		return MediaFrame{}, fmt.Errorf("edgeipc: trailing bytes after media frame")
	}
	cameraEnd := MediaHeaderSize + int(cameraLen)
	frame.CameraID = string(data[MediaHeaderSize:cameraEnd])
	if !utf8.ValidString(frame.CameraID) {
		return MediaFrame{}, ErrInvalidUTF8
	}
	frame.Payload = data[cameraEnd:]
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
	cameraID := []byte(frame.CameraID)
	writeMediaHeader(header[:], frame, uint16(len(cameraID)))
	if err := writeAll(w, header[:]); err != nil {
		return err
	}
	if err := writeAll(w, cameraID); err != nil {
		return err
	}
	return writeAll(w, frame.Payload)
}

// MediaReader reads complete EGAU frames without reading past a frame. Header
// validation happens before the payload allocation.
type MediaReader struct {
	r          io.Reader
	expected   Codec
	configured bool
	maxPayload uint32
}

func NewMediaReader(r io.Reader) *MediaReader {
	return &MediaReader{r: r, maxPayload: MaxMediaPayload}
}

// NewMediaReaderWithMaxPayload applies a smaller host-side bound without
// changing the Edge IPC v1 wire limit.
func NewMediaReaderWithMaxPayload(r io.Reader, maxPayload int) (*MediaReader, error) {
	if maxPayload < 1 || maxPayload > MaxMediaPayload {
		return nil, ErrPayloadTooLarge
	}
	return &MediaReader{r: r, maxPayload: uint32(maxPayload)}, nil
}

// NewMediaReaderForCodec rejects frames that differ from the handshake codec.
func NewMediaReaderForCodec(r io.Reader, codec Codec) (*MediaReader, error) {
	if !validCodec(codec) {
		return nil, ErrUnsupportedCodec
	}
	return &MediaReader{r: r, expected: codec, configured: true, maxPayload: MaxMediaPayload}, nil
}

func (r *MediaReader) Read() (MediaFrame, error) {
	var header [MediaHeaderSize]byte
	if _, err := io.ReadFull(r.r, header[:]); err != nil {
		return MediaFrame{}, err
	}
	frame, cameraLen, payloadLen, err := parseMediaHeader(header[:])
	if err != nil {
		return MediaFrame{}, err
	}
	if r.configured && frame.Codec != r.expected {
		return MediaFrame{}, ErrCodecMismatch
	}
	if payloadLen > r.maxPayload {
		return MediaFrame{}, ErrPayloadTooLarge
	}
	cameraID := make([]byte, cameraLen)
	if _, err := io.ReadFull(r.r, cameraID); err != nil {
		return MediaFrame{}, err
	}
	if !utf8.Valid(cameraID) {
		return MediaFrame{}, ErrInvalidUTF8
	}
	frame.CameraID = string(cameraID)
	frame.Payload = make([]byte, payloadLen)
	if _, err := io.ReadFull(r.r, frame.Payload); err != nil {
		return MediaFrame{}, err
	}
	if err := validateMediaAccessUnit(frame); err != nil {
		return MediaFrame{}, err
	}
	return frame, nil
}

func parseMediaHeader(header []byte) (MediaFrame, uint16, uint32, error) {
	if string(header[:4]) != mediaMagic {
		return MediaFrame{}, 0, 0, ErrInvalidMagic
	}
	if header[4] != ProtocolVersion {
		return MediaFrame{}, 0, 0, ErrUnsupportedVersion
	}
	flags := MediaFlags(header[5])
	if flags&^(FlagIDR|FlagDiscontinuity) != 0 {
		return MediaFrame{}, 0, 0, ErrInvalidFlags
	}
	codec := Codec(header[6])
	if !validCodec(codec) {
		return MediaFrame{}, 0, 0, ErrUnsupportedCodec
	}
	if header[7] != 0 {
		return MediaFrame{}, 0, 0, ErrNonZeroReserved
	}
	if binary.BigEndian.Uint16(header[8:10]) != MediaHeaderSize {
		return MediaFrame{}, 0, 0, ErrInvalidHeader
	}
	cameraLen := binary.BigEndian.Uint16(header[10:12])
	if cameraLen < 1 || cameraLen > 64 {
		return MediaFrame{}, 0, 0, ErrInvalidCameraID
	}
	payloadLen := binary.BigEndian.Uint32(header[12:16])
	if payloadLen == 0 {
		return MediaFrame{}, 0, 0, ErrPayloadTooSmall
	}
	if payloadLen > MaxMediaPayload {
		return MediaFrame{}, 0, 0, ErrPayloadTooLarge
	}
	return MediaFrame{
		Codec:    codec,
		Flags:    flags,
		PTS90kHz: binary.BigEndian.Uint64(header[16:24]),
		Sequence: binary.BigEndian.Uint64(header[24:32]),
	}, cameraLen, payloadLen, nil
}

func writeMediaHeader(header []byte, frame MediaFrame, cameraLen uint16) {
	copy(header[:4], mediaMagic)
	header[4] = ProtocolVersion
	header[5] = byte(frame.Flags)
	header[6] = byte(frame.Codec)
	binary.BigEndian.PutUint16(header[8:10], MediaHeaderSize)
	binary.BigEndian.PutUint16(header[10:12], cameraLen)
	binary.BigEndian.PutUint32(header[12:16], uint32(len(frame.Payload)))
	binary.BigEndian.PutUint64(header[16:24], frame.PTS90kHz)
	binary.BigEndian.PutUint64(header[24:32], frame.Sequence)
}

func validCodec(codec Codec) bool { return codec == CodecH264 || codec == CodecH265 }

func validCameraID(cameraID string) bool {
	return len(cameraID) >= 1 && len(cameraID) <= 64 && utf8.ValidString(cameraID)
}

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

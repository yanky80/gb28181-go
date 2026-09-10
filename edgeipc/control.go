package edgeipc

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

const (
	MessageHello      = "hello"
	MessageStart      = "start"
	MessageRequestIDR = "request_idr"
	MessageStop       = "stop"
	MessageAck        = "ack"
	MessageReady      = "ready"
	MessageHealth     = "health"
	MessageError      = "error"
)

const (
	ErrorCodeInvalidJSON        = "INVALID_JSON"
	ErrorCodeLineTooLong        = "LINE_TOO_LONG"
	ErrorCodeUnsupportedVersion = "UNSUPPORTED_VERSION"
	ErrorCodeUnknownType        = "UNKNOWN_TYPE"
	ErrorCodeInvalidCodec       = "INVALID_CODEC"
	ErrorCodeCodecMismatch      = "CODEC_MISMATCH"
	ErrorCodeInvalidCameraID    = "INVALID_CAMERA_ID"
	ErrorCodeInvalidMedia       = "INVALID_MEDIA"
)

var (
	ErrInvalidJSON        = errors.New("edgeipc: invalid control JSON")
	ErrControlLineTooLong = errors.New("edgeipc: control line exceeds 16 KiB")
	ErrUnknownMessageType = errors.New("edgeipc: unknown control message type")
)

// ControlMessage is the v1 JSONL envelope. Fields not used by a message type
// are omitted; unknown fields received from a peer are ignored for additive
// compatibility within protocol version 1.
type ControlMessage struct {
	Type      string `json:"type"`
	Version   byte   `json:"version"`
	RequestID string `json:"request_id,omitempty"`
	CameraID  uint16 `json:"camera_id,omitempty"`
	Codec     Codec  `json:"codec,omitempty"`
	Status    string `json:"status,omitempty"`
	Code      string `json:"code,omitempty"`
	Message   string `json:"message,omitempty"`
}

// MarshalJSON encodes codecs by their stable wire names rather than enum
// numbers, keeping the control stream readable by the C++ peer.
func (c Codec) MarshalJSON() ([]byte, error) {
	name, err := codecName(c)
	if err != nil {
		return nil, err
	}
	return json.Marshal(name)
}

func (c *Codec) UnmarshalJSON(data []byte) error {
	var name string
	if err := json.Unmarshal(data, &name); err != nil {
		return ErrUnsupportedCodec
	}
	switch name {
	case "h264":
		*c = CodecH264
	case "h265":
		*c = CodecH265
	default:
		return ErrUnsupportedCodec
	}
	return nil
}

// ValidateControlMessage validates the common envelope and the required
// fields for each v1 message type.
func ValidateControlMessage(message ControlMessage) error {
	if message.Version != ProtocolVersion {
		return ErrUnsupportedVersion
	}
	switch message.Type {
	case MessageHello, MessageReady:
		if !validCodec(message.Codec) {
			return ErrUnsupportedCodec
		}
	case MessageStart:
		if !validCodec(message.Codec) {
			return ErrUnsupportedCodec
		}
		if !validCameraID(message.CameraID) {
			return ErrInvalidCameraID
		}
	case MessageRequestIDR, MessageStop:
		if !validCameraID(message.CameraID) {
			return ErrInvalidCameraID
		}
	case MessageAck:
		if message.RequestID == "" {
			return errors.New("edgeipc: ack requires request_id")
		}
	case MessageHealth:
		if message.Status == "" {
			return errors.New("edgeipc: health requires status")
		}
	case MessageError:
		if message.Code == "" || message.Message == "" {
			return errors.New("edgeipc: error requires code and message")
		}
	default:
		return ErrUnknownMessageType
	}
	return nil
}

// ValidateCodecAgreement rejects a peer's attempt to change the configured
// codec through a hello/ready message or a media frame.
func ValidateCodecAgreement(configured, peer Codec) error {
	if !validCodec(configured) || !validCodec(peer) {
		return ErrUnsupportedCodec
	}
	if configured != peer {
		return ErrCodecMismatch
	}
	return nil
}

// ValidateControlCodec validates the codec-bearing control messages against
// the locally configured codec. Other message types carry no codec choice.
func ValidateControlCodec(configured Codec, message ControlMessage) error {
	if err := ValidateControlMessage(message); err != nil {
		return err
	}
	if message.Type == MessageHello || message.Type == MessageReady || message.Type == MessageStart {
		return ValidateCodecAgreement(configured, message.Codec)
	}
	return nil
}

// WriteControlMessage writes one validated JSON object followed by LF.
func WriteControlMessage(w io.Writer, message ControlMessage) error {
	if err := ValidateControlMessage(message); err != nil {
		return err
	}
	data, err := json.Marshal(message)
	if err != nil {
		return fmt.Errorf("edgeipc: marshal control message: %w", err)
	}
	if len(data)+1 > MaxControlLine {
		return ErrControlLineTooLong
	}
	data = append(data, '\n')
	return writeAll(w, data)
}

// ControlReader reads one JSONL message at a time. It reads directly from the
// supplied stream so a call never consumes bytes belonging to the next line.
type ControlReader struct{ r *bufio.Reader }

func NewControlReader(r io.Reader) *ControlReader { return &ControlReader{r: bufio.NewReader(r)} }

func (r *ControlReader) Read() (ControlMessage, error) {
	line := make([]byte, 0, MaxControlLine)
	for {
		fragment, err := r.r.ReadSlice('\n')
		if len(line)+len(fragment) > MaxControlLine {
			return ControlMessage{}, ErrControlLineTooLong
		}
		line = append(line, fragment...)
		if len(fragment) > 0 && fragment[len(fragment)-1] == '\n' {
			return decodeControlLine(line[:len(line)-1])
		}
		if err != nil {
			if errors.Is(err, bufio.ErrBufferFull) {
				continue
			}
			if errors.Is(err, io.EOF) && len(line) > 0 {
				return decodeControlLine(line)
			}
			return ControlMessage{}, err
		}
	}
}

func decodeControlLine(line []byte) (ControlMessage, error) {
	line = bytes.TrimSuffix(line, []byte{'\r'})
	if len(line) == 0 {
		return ControlMessage{}, ErrInvalidJSON
	}
	var message ControlMessage
	if err := json.Unmarshal(line, &message); err != nil {
		return ControlMessage{}, fmt.Errorf("%w: %v", ErrInvalidJSON, err)
	}
	if err := ValidateControlMessage(message); err != nil {
		return ControlMessage{}, err
	}
	return message, nil
}

func codecName(codec Codec) (string, error) {
	switch codec {
	case CodecH264:
		return "h264", nil
	case CodecH265:
		return "h265", nil
	default:
		return "", ErrUnsupportedCodec
	}
}

func validCameraID(cameraID uint16) bool { return cameraID >= 1 && cameraID <= 64 }

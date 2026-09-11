package edgeipc

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"unicode/utf8"
)

const MaxControlLine = 16 << 10

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

var (
	ErrInvalidJSON        = errors.New("edgeipc: invalid control JSON")
	ErrControlLineTooLong = errors.New("edgeipc: control line exceeds 16 KiB")
	ErrUnknownMessageType = errors.New("edgeipc: unknown control message type")
)

// ControlMessage is the v1 JSONL envelope. RequestID is numeric on the wire;
// CameraID is always a UTF-8 string.
type ControlMessage struct {
	Type        string  `json:"type"`
	Version     byte    `json:"version"`
	CameraID    string  `json:"camera_id,omitempty"`
	PID         int64   `json:"pid,omitempty"`
	Codecs      []Codec `json:"codecs,omitempty"`
	StreamEpoch uint64  `json:"stream_epoch,omitempty"`
	RequestID   uint64  `json:"request_id,omitempty"`
	GraceMS     uint32  `json:"grace_ms,omitempty"`
	State       string  `json:"state,omitempty"`
	Codec       Codec   `json:"codec,omitempty"`
	Width       uint32  `json:"width,omitempty"`
	Height      uint32  `json:"height,omitempty"`
	RTSP        string  `json:"rtsp,omitempty"`
	InferFPS    float64 `json:"infer_fps,omitempty"`
	Encode      string  `json:"encode,omitempty"`
	Code        string  `json:"code,omitempty"`
	Retryable   bool    `json:"retryable,omitempty"`
}

func (m ControlMessage) MarshalJSON() ([]byte, error) {
	type plain ControlMessage
	data, err := json.Marshal(plain(m))
	if err != nil {
		return nil, err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return nil, err
	}
	if m.Type == MessageStop {
		fields["grace_ms"], err = json.Marshal(m.GraceMS)
		if err != nil {
			return nil, err
		}
	}
	if m.Type == MessageHealth {
		fields["infer_fps"], err = json.Marshal(m.InferFPS)
		if err != nil {
			return nil, err
		}
	}
	if m.Type == MessageError {
		fields["retryable"], err = json.Marshal(m.Retryable)
		if err != nil {
			return nil, err
		}
	}
	return json.Marshal(fields)
}

func (m *ControlMessage) UnmarshalJSON(data []byte) error {
	type plain ControlMessage
	var decoded plain
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	for _, field := range requiredZeroValuedFields(decoded.Type) {
		raw, ok := fields[field]
		if !ok || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return fmt.Errorf("edgeipc: %s requires %s", decoded.Type, field)
		}
	}
	*m = ControlMessage(decoded)
	return nil
}

func requiredZeroValuedFields(messageType string) []string {
	switch messageType {
	case MessageStop:
		return []string{"grace_ms"}
	case MessageHealth:
		return []string{"infer_fps"}
	case MessageError:
		return []string{"retryable"}
	default:
		return nil
	}
}

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

// ValidateControlMessage validates all required fields for the v1 messages.
func ValidateControlMessage(message ControlMessage) error {
	if message.Version != ProtocolVersion {
		return ErrUnsupportedVersion
	}
	switch message.Type {
	case MessageHello:
		if !validCameraID(message.CameraID) || message.PID <= 0 || len(message.Codecs) == 0 || message.StreamEpoch == 0 {
			return errors.New("edgeipc: invalid hello fields")
		}
		for _, codec := range message.Codecs {
			if !validCodec(codec) {
				return ErrUnsupportedCodec
			}
		}
	case MessageStart, MessageRequestIDR:
		if !validCameraID(message.CameraID) || message.RequestID == 0 {
			return errors.New("edgeipc: invalid request fields")
		}
	case MessageStop:
		if !validCameraID(message.CameraID) || message.RequestID == 0 {
			return errors.New("edgeipc: invalid stop fields")
		}
	case MessageAck:
		if message.RequestID == 0 || message.State == "" {
			return errors.New("edgeipc: invalid ack fields")
		}
	case MessageReady:
		if !validCodec(message.Codec) || message.Width == 0 || message.Height == 0 {
			return errors.New("edgeipc: invalid ready fields")
		}
	case MessageHealth:
		if message.RTSP == "" || message.Encode == "" || math.IsNaN(message.InferFPS) || math.IsInf(message.InferFPS, 0) || message.InferFPS < 0 {
			return errors.New("edgeipc: invalid health fields")
		}
	case MessageError:
		if message.Code == "" {
			return errors.New("edgeipc: error requires code")
		}
	default:
		return ErrUnknownMessageType
	}
	return nil
}

// ValidateCodecAgreement rejects a peer codec different from configuration.
func ValidateCodecAgreement(configured, peer Codec) error {
	if !validCodec(configured) || !validCodec(peer) {
		return ErrUnsupportedCodec
	}
	if configured != peer {
		return ErrCodecMismatch
	}
	return nil
}

// ValidateControlCodec validates the codec-bearing hello and ready messages.
func ValidateControlCodec(configured Codec, message ControlMessage) error {
	if err := ValidateControlMessage(message); err != nil {
		return err
	}
	if message.Type == MessageHello {
		for _, peer := range message.Codecs {
			if err := ValidateCodecAgreement(configured, peer); err == nil {
				return nil
			}
		}
		return ErrCodecMismatch
	}
	if message.Type == MessageReady {
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
	return writeAll(w, append(data, '\n'))
}

// ControlReader reads one bounded JSONL message at a time.
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
	if !utf8.Valid(line) {
		return ControlMessage{}, fmt.Errorf("%w: invalid UTF-8", ErrInvalidJSON)
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

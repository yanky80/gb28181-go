package edgeipc

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
)

func TestControlJSONLRoundTripAndPipelining(t *testing.T) {
	want := ControlMessage{
		Type:      MessageHello,
		Version:   ProtocolVersion,
		Codec:     CodecH265,
		RequestID: "hello-1",
	}
	var wire bytes.Buffer
	if err := WriteControlMessage(&wire, want); err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(wire.String(), "\n") || !strings.Contains(wire.String(), `"codec":"h265"`) {
		t.Fatalf("wire = %q", wire.String())
	}

	reader := NewControlReader(&chunkReader{data: append(append([]byte(nil), wire.Bytes()...), wire.Bytes()...), size: 1})
	got, err := reader.Read()
	if err != nil {
		t.Fatal(err)
	}
	gotAgain, err := reader.Read()
	if err != nil {
		t.Fatal(err)
	}
	if got != want || gotAgain != want {
		t.Fatalf("messages = %+v and %+v, want %+v", got, gotAgain, want)
	}
}

func TestControlMessagesValidateTypesAndFields(t *testing.T) {
	tests := []struct {
		name string
		msg  ControlMessage
		want error
	}{
		{name: "unknown type", msg: ControlMessage{Type: "reconfigure", Version: ProtocolVersion}, want: ErrUnknownMessageType},
		{name: "bad version", msg: ControlMessage{Type: MessageHello, Version: 2, Codec: CodecH265}, want: ErrUnsupportedVersion},
		{name: "hello needs codec", msg: ControlMessage{Type: MessageHello, Version: ProtocolVersion}, want: ErrUnsupportedCodec},
		{name: "start needs camera", msg: ControlMessage{Type: MessageStart, Version: ProtocolVersion, Codec: CodecH265}, want: ErrInvalidCameraID},
		{name: "bad codec", msg: ControlMessage{Type: MessageReady, Version: ProtocolVersion, Codec: Codec(3)}, want: ErrUnsupportedCodec},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := ValidateControlMessage(tt.msg); !errors.Is(err, tt.want) {
				t.Fatalf("error = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestControlMessagesRejectMissingRequiredFields(t *testing.T) {
	tests := []ControlMessage{
		{Type: MessageAck, Version: ProtocolVersion},
		{Type: MessageHealth, Version: ProtocolVersion},
		{Type: MessageError, Version: ProtocolVersion, Code: "E"},
	}
	for _, message := range tests {
		if err := ValidateControlMessage(message); err == nil {
			t.Fatalf("message %+v was accepted without required fields", message)
		}
	}
}

func TestControlReaderRejectsInvalidJSONAndOverlongLine(t *testing.T) {
	if _, err := NewControlReader(strings.NewReader("{\"type\":\"hello\"\n")).Read(); !errors.Is(err, ErrInvalidJSON) {
		t.Fatalf("invalid JSON error = %v", err)
	}
	line := strings.Repeat("x", MaxControlLine+1)
	if _, err := NewControlReader(strings.NewReader(line)).Read(); !errors.Is(err, ErrControlLineTooLong) {
		t.Fatalf("long line error = %v", err)
	}
	if _, err := NewControlReader(strings.NewReader("\n")).Read(); !errors.Is(err, ErrInvalidJSON) {
		t.Fatalf("empty line error = %v", err)
	}
}

func TestControlWriterRejectsOversizedMessageBeforeWriting(t *testing.T) {
	var wire bytes.Buffer
	err := WriteControlMessage(&wire, ControlMessage{
		Type:    MessageError,
		Version: ProtocolVersion,
		Code:    ErrorCodeInvalidMedia,
		Message: strings.Repeat("x", MaxControlLine),
	})
	if !errors.Is(err, ErrControlLineTooLong) {
		t.Fatalf("error = %v, want line-too-long", err)
	}
	if wire.Len() != 0 {
		t.Fatalf("writer received %d bytes after rejection", wire.Len())
	}
}

func TestControlErrorRejectsUnknownCode(t *testing.T) {
	message := ControlMessage{Type: MessageError, Version: ProtocolVersion, Code: "BOGUS", Message: "bad"}
	if err := ValidateControlMessage(message); err == nil {
		t.Fatal("unknown protocol error code was accepted")
	}
}

func TestCodecAgreementRejectsPeerSwitch(t *testing.T) {
	if err := ValidateCodecAgreement(CodecH265, CodecH265); err != nil {
		t.Fatal(err)
	}
	if err := ValidateCodecAgreement(CodecH265, CodecH264); !errors.Is(err, ErrCodecMismatch) {
		t.Fatalf("mismatch error = %v, want codec mismatch", err)
	}
}

func TestControlCodecValidationCoversHandshakeAndStart(t *testing.T) {
	for _, message := range []ControlMessage{
		{Type: MessageHello, Version: ProtocolVersion, Codec: CodecH264},
		{Type: MessageReady, Version: ProtocolVersion, Codec: CodecH264},
		{Type: MessageStart, Version: ProtocolVersion, CameraID: 1, Codec: CodecH264},
	} {
		if err := ValidateControlCodec(CodecH265, message); !errors.Is(err, ErrCodecMismatch) {
			t.Fatalf("%s error = %v, want codec mismatch", message.Type, err)
		}
	}
}

func TestControlLineLimitAcceptsExactLFAndEOFBoundaries(t *testing.T) {
	message := ControlMessage{Type: MessageError, Version: ProtocolVersion, Code: ErrorCodeInvalidMedia, Message: "x"}
	for len(mustJSON(message))+1 < MaxControlLine {
		message.Message += "x"
	}
	var wire bytes.Buffer
	if err := WriteControlMessage(&wire, message); err != nil {
		t.Fatalf("exact LF line rejected: %v", err)
	}
	if wire.Len() != MaxControlLine {
		t.Fatalf("LF line length = %d, want %d", wire.Len(), MaxControlLine)
	}
	if _, err := NewControlReader(bytes.NewReader(wire.Bytes())).Read(); err != nil {
		t.Fatalf("exact LF line read error: %v", err)
	}

	eofMessage := ControlMessage{Type: MessageHealth, Version: ProtocolVersion, Status: "x"}
	for len(mustJSON(eofMessage)) < MaxControlLine {
		eofMessage.Status += "x"
	}
	eofJSON := mustJSON(eofMessage)
	if len(eofJSON) != MaxControlLine {
		t.Fatalf("EOF line length = %d, want %d", len(eofJSON), MaxControlLine)
	}
	if _, err := NewControlReader(bytes.NewReader(eofJSON)).Read(); err != nil {
		t.Fatalf("exact EOF line read error: %v", err)
	}

	crlfMessage := ControlMessage{Type: MessageHealth, Version: ProtocolVersion, Status: "x"}
	for len(mustJSON(crlfMessage))+2 < MaxControlLine {
		crlfMessage.Status += "x"
	}
	crlf := append(mustJSON(crlfMessage), '\r', '\n')
	if len(crlf) != MaxControlLine {
		t.Fatalf("CRLF line length = %d, want %d", len(crlf), MaxControlLine)
	}
	if _, err := NewControlReader(bytes.NewReader(crlf)).Read(); err != nil {
		t.Fatalf("exact CRLF line read error: %v", err)
	}
}

func mustJSON(message ControlMessage) []byte {
	data, err := json.Marshal(message)
	if err != nil {
		panic(err)
	}
	return data
}

func TestControlReaderAcceptsEOFDelimitedFinalLine(t *testing.T) {
	msg := `{"type":"health","version":1,"status":"ok"}`
	reader := NewControlReader(strings.NewReader(msg))
	got, err := reader.Read()
	if err != nil {
		t.Fatal(err)
	}
	if got.Type != MessageHealth || got.Status != "ok" {
		t.Fatalf("message = %+v", got)
	}
	if _, err := reader.Read(); !errors.Is(err, io.EOF) {
		t.Fatalf("second read error = %v, want EOF", err)
	}
}

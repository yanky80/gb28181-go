package edgeipc

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestControlJSONLRoundTripAndPipelining(t *testing.T) {
	want := ControlMessage{Type: MessageHello, Version: ProtocolVersion, CameraID: "camera-7", PID: 42, Codecs: []Codec{CodecH265, CodecH264}, StreamEpoch: 9}
	var wire bytes.Buffer
	if err := WriteControlMessage(&wire, want); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(wire.String(), `"camera_id":"camera-7"`) || !strings.Contains(wire.String(), `"codecs":["h265","h264"]`) {
		t.Fatalf("wire = %q", wire.String())
	}
	reader := NewControlReader(bytes.NewReader(append(wire.Bytes(), wire.Bytes()...)))
	got, err := reader.Read()
	if err != nil {
		t.Fatal(err)
	}
	gotAgain, err := reader.Read()
	if err != nil {
		t.Fatal(err)
	}
	if got.CameraID != want.CameraID || got.PID != want.PID || got.StreamEpoch != want.StreamEpoch || len(got.Codecs) != 2 || gotAgain.CameraID != want.CameraID {
		t.Fatalf("messages = %+v and %+v", got, gotAgain)
	}
}

func TestControlMessagesValidateAllDefinedTypes(t *testing.T) {
	valid := []ControlMessage{
		{Type: MessageHello, Version: ProtocolVersion, CameraID: "cam-1", PID: 1, Codecs: []Codec{CodecH265}, StreamEpoch: 1},
		{Type: MessageStart, Version: ProtocolVersion, CameraID: "cam-1", RequestID: 1},
		{Type: MessageRequestIDR, Version: ProtocolVersion, CameraID: "cam-1", RequestID: 2},
		{Type: MessageStop, Version: ProtocolVersion, CameraID: "cam-1", RequestID: 3, GraceMS: 100},
		{Type: MessageAck, Version: ProtocolVersion, RequestID: 3, State: "stopped"},
		{Type: MessageReady, Version: ProtocolVersion, Codec: CodecH265, Width: 1920, Height: 1080},
		{Type: MessageHealth, Version: ProtocolVersion, RTSP: "ok", InferFPS: 25.5, Encode: "ok"},
		{Type: MessageError, Version: ProtocolVersion, Code: "ENCODER_FAILED", Retryable: true},
	}
	for _, message := range valid {
		if err := ValidateControlMessage(message); err != nil {
			t.Errorf("%s rejected: %v", message.Type, err)
		}
	}
}

func TestControlRequiredZeroValuesStayOnWire(t *testing.T) {
	for _, message := range []ControlMessage{
		{Type: MessageStop, Version: ProtocolVersion, CameraID: "cam-1", RequestID: 1},
		{Type: MessageError, Version: ProtocolVersion, Code: "failed", Retryable: false},
	} {
		var wire bytes.Buffer
		if err := WriteControlMessage(&wire, message); err != nil {
			t.Fatal(err)
		}
		if message.Type == MessageStop && !strings.Contains(wire.String(), `"grace_ms":0`) {
			t.Fatalf("stop omitted grace_ms: %s", wire.String())
		}
		if message.Type == MessageError && !strings.Contains(wire.String(), `"retryable":false`) {
			t.Fatalf("error omitted retryable: %s", wire.String())
		}
	}
}

func TestControlHealthZeroInferFPSRoundTrips(t *testing.T) {
	message := ControlMessage{Type: MessageHealth, Version: ProtocolVersion, RTSP: "ok", InferFPS: 0, Encode: "ok"}
	var wire bytes.Buffer
	if err := WriteControlMessage(&wire, message); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(wire.String(), `"infer_fps":0`) {
		t.Fatalf("health omitted zero infer_fps: %s", wire.String())
	}
	got, err := NewControlReader(bytes.NewReader(wire.Bytes())).Read()
	if err != nil {
		t.Fatal(err)
	}
	if got.Type != MessageHealth || got.InferFPS != 0 {
		t.Fatalf("round trip = %+v", got)
	}
}

func TestControlRequestIDIsNumeric(t *testing.T) {
	var wire bytes.Buffer
	message := ControlMessage{Type: MessageStart, Version: ProtocolVersion, CameraID: "cam-1", RequestID: 7}
	if err := WriteControlMessage(&wire, message); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(wire.String(), `"request_id":7`) || strings.Contains(wire.String(), `"request_id":"7"`) {
		t.Fatalf("wire = %s", wire.String())
	}
}

func TestControlValidationRejectsInvalidFields(t *testing.T) {
	tests := []ControlMessage{
		{Type: MessageHello, Version: 2, CameraID: "cam-1", PID: 1, Codecs: []Codec{CodecH265}, StreamEpoch: 1},
		{Type: "unknown", Version: ProtocolVersion},
		{Type: MessageHello, Version: ProtocolVersion, CameraID: "", PID: 1, Codecs: []Codec{CodecH265}, StreamEpoch: 1},
		{Type: MessageHello, Version: ProtocolVersion, CameraID: "cam-1", PID: 1, Codecs: []Codec{Codec(3)}, StreamEpoch: 1},
		{Type: MessageStart, Version: ProtocolVersion, CameraID: "cam-1"},
		{Type: MessageAck, Version: ProtocolVersion, RequestID: 1},
		{Type: MessageReady, Version: ProtocolVersion, Codec: CodecH265, Width: 0, Height: 1080},
	}
	for _, message := range tests {
		if err := ValidateControlMessage(message); err == nil {
			t.Errorf("invalid message accepted: %+v", message)
		}
	}
}

func TestControlReaderRejectsMissingRequiredZeroValuedFields(t *testing.T) {
	for _, wire := range []string{
		`{"type":"stop","version":1,"camera_id":"cam-1","request_id":1}`,
		`{"type":"health","version":1,"rtsp":"ok","encode":"ok"}`,
		`{"type":"error","version":1,"code":"failed"}`,
	} {
		if _, err := NewControlReader(strings.NewReader(wire + "\n")).Read(); err == nil {
			t.Fatalf("accepted message with missing required field: %s", wire)
		}
	}
}

func TestControlReaderRejectsInvalidJSONAndOversizedLine(t *testing.T) {
	if _, err := NewControlReader(strings.NewReader("{\"type\":\"hello\"\n")).Read(); !errors.Is(err, ErrInvalidJSON) {
		t.Fatalf("invalid JSON error = %v", err)
	}
	if _, err := NewControlReader(strings.NewReader(strings.Repeat("x", MaxControlLine+1))).Read(); !errors.Is(err, ErrControlLineTooLong) {
		t.Fatalf("long line error = %v", err)
	}
}

func TestControlLineLimitAcceptsExactLFCRLFAndEOF(t *testing.T) {
	lf := ControlMessage{Type: MessageError, Version: ProtocolVersion, Code: "x"}
	for len(mustJSON(lf))+1 < MaxControlLine {
		lf.Code += "x"
	}
	var wire bytes.Buffer
	if err := WriteControlMessage(&wire, lf); err != nil || wire.Len() != MaxControlLine {
		t.Fatalf("exact LF line: len=%d err=%v", wire.Len(), err)
	}
	if _, err := NewControlReader(bytes.NewReader(wire.Bytes())).Read(); err != nil {
		t.Fatal(err)
	}

	crlf := ControlMessage{Type: MessageHealth, Version: ProtocolVersion, RTSP: "ok", InferFPS: 1, Encode: "ok"}
	for len(mustJSON(crlf))+2 < MaxControlLine {
		crlf.RTSP += "x"
	}
	crlfWire := append(mustJSON(crlf), '\r', '\n')
	if len(crlfWire) != MaxControlLine {
		t.Fatalf("exact CRLF length = %d", len(crlfWire))
	}
	if _, err := NewControlReader(bytes.NewReader(crlfWire)).Read(); err != nil {
		t.Fatal(err)
	}

	eof := ControlMessage{Type: MessageHealth, Version: ProtocolVersion, RTSP: "ok", InferFPS: 1, Encode: "ok"}
	for len(mustJSON(eof)) < MaxControlLine {
		eof.RTSP += "x"
	}
	if _, err := NewControlReader(bytes.NewReader(mustJSON(eof))).Read(); err != nil {
		t.Fatal(err)
	}
}

func TestCodecAgreementCoversHelloAndReady(t *testing.T) {
	for _, message := range []ControlMessage{
		{Type: MessageHello, Version: ProtocolVersion, CameraID: "cam-1", PID: 1, Codecs: []Codec{CodecH264}, StreamEpoch: 1},
		{Type: MessageReady, Version: ProtocolVersion, Codec: CodecH264, Width: 1, Height: 1},
	} {
		if err := ValidateControlCodec(CodecH265, message); !errors.Is(err, ErrCodecMismatch) {
			t.Fatalf("%s error = %v, want codec mismatch", message.Type, err)
		}
	}
}

func mustJSON(message ControlMessage) []byte {
	data, err := json.Marshal(message)
	if err != nil {
		panic(err)
	}
	return data
}

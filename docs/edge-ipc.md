# Edge IPC v1

Edge IPC v1 is the transport-independent seam between the C++ inference
process and the Go GB gateway. The C++ side owns the UDS client and the Go
side may provide a UDS server; this package only encodes and validates the
wire data. Socket lifecycle, reconnect policy, `CameraRegistry`, and business
state machines are outside this protocol.

## Compatibility

Both streams use protocol version `1`.

- `control.sock` is UTF-8 JSON Lines (JSONL); one JSON object per line.
- `media.sock` is a sequence of binary EGAU frames.
- A peer rejects any other version before acting on the message or allocating
  a media payload.
- New JSON fields may be ignored by a v1 peer. Unknown message types, invalid
  values, and malformed JSON are errors.
- The configured codec and the peer's `hello`/`ready` codec must match. A
  mismatch is rejected; `ready` and a media frame never cause a runtime codec
  switch or a second encoder to start.

The zero-configuration profile is H.265. H.264 remains supported when the
configuration and handshake explicitly select it.

## `control.sock`

The maximum line length is 16 KiB, including the terminating LF. A final line
without LF is accepted if it is within the limit. CRLF is accepted. A writer
must reject an oversized message before writing any bytes.

Every message has a `type` and `version`:

| Type | Required fields | Meaning |
| --- | --- | --- |
| `hello` | `codec` | Peer capability/initial handshake |
| `start` | `camera_id`, `codec` | Request the configured encoded output |
| `request_idr` | `camera_id` | Request an IDR access unit |
| `stop` | `camera_id` | Stop the encoded output request |
| `ack` | `request_id` | Acknowledge a request |
| `ready` | `codec` | Peer is ready with the agreed codec |
| `health` | `status` | Report process/output health |
| `error` | `code`, `message` | Report a protocol or processing error |

Example H.265 handshake and start:

```json
{"type":"hello","version":1,"request_id":"h-1","codec":"h265"}
{"type":"ready","version":1,"codec":"h265"}
{"type":"start","version":1,"camera_id":1,"codec":"h265"}
```

`codec` is the string `h264` or `h265`; numeric codec values are invalid.
`camera_id` is an integer from 1 through 64. `request_idr` is spelled with an
underscore and is not an alias for another message type.

For a protocol error, the peer may send an `error` message with one of these
stable `code` values. The receiver must reject the offending input and must
not dispatch it; whether the host then closes or reconnects the socket is
outside this package.

| Code | Meaning |
| --- | --- |
| `INVALID_JSON` | Control line is not one JSON object |
| `LINE_TOO_LONG` | Control line exceeds 16 KiB |
| `UNSUPPORTED_VERSION` | Protocol version is not 1 |
| `UNKNOWN_TYPE` | Control message type is not in the v1 set |
| `INVALID_CODEC` | Codec is not `h264` or `h265` |
| `CODEC_MISMATCH` | Peer codec differs from configured codec |
| `INVALID_CAMERA_ID` | Camera ID is outside 1..64 |
| `INVALID_MEDIA` | EGAU header, payload, flags, or access unit is invalid |

## `media.sock`

The stream contains one complete frame after another. Each frame starts with
the following fixed 32-byte header. All integers are unsigned and encoded in
network byte order (big-endian).

| Offset | Size | Field | Value/validation |
| ---: | ---: | --- | --- |
| 0 | 4 | magic | ASCII `EGAU` |
| 4 | 1 | version | `1` |
| 5 | 1 | header_len | `32` |
| 6 | 1 | codec | `1` = H.264, `2` = H.265 |
| 7 | 1 | flags | bit 0 = IDR, bit 1 = parameter sets; other bits zero |
| 8 | 2 | camera_id | 1..64 |
| 10 | 2 | reserved | zero |
| 12 | 4 | stream_epoch | encoder lifetime, starts a new sequence at one |
| 16 | 4 | sequence | access-unit sequence within the epoch |
| 20 | 8 | pts_ns | presentation timestamp in nanoseconds |
| 28 | 4 | payload_len | 1..8 MiB |

The payload follows immediately and contains one encoded Annex-B access unit.
The reader validates the complete header, including magic, version,
`header_len`, codec, flags, camera range, reserved bytes, and payload length
before allocating the payload. It then uses exact-read semantics, so short
reads and concatenated frames are handled without changing frame boundaries.

For an H.265 frame marked IDR, the access unit must contain Annex-B VPS, SPS,
PPS, and an IDR NAL unit. The H.265 NAL unit types are VPS `32`, SPS `33`, PPS
`34`, and IDR `19` or `20`; the two-byte NAL header is interpreted according to
the H.265 specification. An H.264 frame marked IDR must contain an IDR NAL
unit (type `5`). Unmarked access units are not treated as keyframes.

## Go interface

The `edgeipc` package exposes the protocol without adding dependencies:

```go
frame := edgeipc.MediaFrame{
    Codec: edgeipc.CodecH265, CameraID: 1, Sequence: 1,
    Payload: annexBAccessUnit,
}
wire, err := edgeipc.MarshalMediaFrame(frame)
got, err := edgeipc.NewMediaReader(r).Read()

message := edgeipc.ControlMessage{
    Type: edgeipc.MessageHello, Version: edgeipc.ProtocolVersion,
    Codec: edgeipc.CodecH265,
}
err := edgeipc.WriteControlMessage(w, message)
got, err := edgeipc.NewControlReader(r).Read()
```

Use `ValidateCodecAgreement(configured, peer)` after accepting the handshake
and before starting output. Use `ValidateMediaFrame` or the media reader's
validation at the media seam.

Errors are returned to the caller; the package does not send an `error`
message, close a socket, retry, or alter process state.

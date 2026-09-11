# Edge IPC v1

Edge IPC v1 is the versioned wire seam between the C++ inference process and
the Go GB gateway. This package encodes and validates bytes only; it does not
open sockets, reconnect, own a `CameraRegistry`, or implement a business state
machine.

## Common rules

Both streams use protocol version `1`. The default codec is H.265. H.264 is
valid only when selected by configuration and accepted by the handshake. A
codec mismatch is rejected; `hello`, `ready`, and media frames never trigger
a silent codec switch or a second encoder.

## `media.sock`

The stream is a sequence of EGAU frames. Integers are unsigned big-endian.
The fixed header is exactly 32 bytes:

| Offset | Size | Field | Rule |
| ---: | ---: | --- | --- |
| 0 | 4 | `magic` | ASCII `EGAU` |
| 4 | 1 | `version` | `1` |
| 5 | 1 | `flags` | bit 0 `IDR`; bit 1 `DISCONTINUITY`; other bits zero |
| 6 | 1 | `codec` | `1` H.264; `2` H.265 |
| 7 | 1 | `reserved` | zero |
| 8 | 2 | `header_len` | `32` |
| 10 | 2 | `camera_id_len` | `1..64` bytes |
| 12 | 4 | `payload_len` | `1..8 MiB` |
| 16 | 8 | `pts_90khz` | 90 kHz presentation timestamp |
| 24 | 8 | `sequence` | access-unit sequence number |

The header is followed by exactly `camera_id_len` bytes of UTF-8 `camera_id`,
then exactly `payload_len` bytes of encoded media. `camera_id` is a string; it
is not a numeric range. The reader rejects invalid header fields, camera length
or UTF-8, and payload length before allocating the payload. Short reads and
concatenated frames are handled with exact-read semantics.

Every payload is one Annex-B access unit. H.264 NAL units must have a clear
`forbidden_zero_bit`; an IDR-marked H.264 AU must contain NAL type 5. H.265
NAL units must have a valid two-byte header (`forbidden_zero_bit` clear and
`nuh_temporal_id_plus1` nonzero). An IDR-marked H.265 AU must contain VPS,
SPS, PPS, and IDR NAL types 32, 33, 34, and 19 or 20. `DISCONTINUITY` is only
a flag; it does not change sequence or codec rules.

## `control.sock`

Control is UTF-8 JSONL: one JSON object per line, at most 16 KiB including LF.
CRLF and a final EOF-delimited line are accepted within the limit. Unknown
JSON fields are ignored for additive v1 compatibility; unknown message types,
invalid fields, malformed JSON, and oversized lines are rejected.

Every message has `type` and `version`. The frozen message fields are:

| Type | Required fields |
| --- | --- |
| `hello` | `camera_id` string, numeric `pid`, `codecs` array, `stream_epoch` |
| `start` | `camera_id` string, numeric `request_id` |
| `request_idr` | `camera_id` string, numeric `request_id` |
| `stop` | `camera_id` string, numeric `request_id`, `grace_ms` |
| `ack` | numeric `request_id`, `state` |
| `ready` | `codec`, `width`, `height` |
| `health` | `rtsp`, `infer_fps`, `encode` |
| `error` | `code`, `retryable` |

`codec` is the string `h264` or `h265`. `request_id` is a JSON number and is
never a string. `hello.codecs` is an array of codec strings. `ValidateControlCodec`
checks that the configured codec is advertised by `hello` and equals `ready`;
`NewMediaReaderForCodec` performs the same check before reading a media
payload.

A health timeout retires the current stream epoch; health from that epoch cannot
revive it. The peer must reconnect and send `hello` with a new `stream_epoch`.

Example default H.265 exchange:

```json
{"type":"hello","version":1,"camera_id":"camera-1","pid":420,"codecs":["h265"],"stream_epoch":1}
{"type":"start","version":1,"camera_id":"camera-1","request_id":1}
{"type":"ready","version":1,"codec":"h265","width":1920,"height":1080}
{"type":"ack","version":1,"request_id":1,"state":"started"}
```

Errors are returned to the caller. The package does not send an `error`
message, close a socket, retry, or alter process state.

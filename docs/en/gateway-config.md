# Gateway configuration

`cmd/gb-gateway` reads a small `key=value` file with `LoadConfig`. Parsing
does not write the file; callers can atomically replace it and load it again
on restart. Unknown or duplicate keys include their source line.

The default profile is GB/T 28181-2022 with H.265, UDP signaling, a 60-second
heartbeat, a 3,600-second registration lease, 5-second stop grace, a 3-second
IDR deadline, and an 8 MiB access-unit limit. The only accepted profiles are
`2022+h265`, `2022+h264`, and `2016+h264`; all cameras in one gateway use the
same codec profile.

Supported keys are:

- `gb.enabled`, `gb.protocol_version`, `gb.server_addr`,
  `gb.server_domain`, `gb.local_device_id`, `gb.realm`, `gb.sip_listen`,
  `gb.heartbeat_interval`, `gb.register_expires`, `gb.transport`,
  `gb.media_transport`, `gb.stop_grace_ms`, `gb.idr_timeout_ms`, and
  `gb.record_playback`;
- `ipc.control_socket`, `ipc.media_socket`, `ipc.max_au_bytes`, and
  `ipc.status_dir`;
- `camN.camera_id`, `camN.gb_expose`, `camN.gb_name`, `camN.gb_codec`, and
  `camN.ptz_mode`.

`ptz_mode` accepts `none`, `local-gb28181`, `onvif`, or `vendor`. PTZ is only
advertised after a matching adapter is wired; selecting a mode alone does not
claim camera capability.

Here `camera_id` is the design's external key for a local camera; it is not
the stable GB channel identity assigned by the gateway.

`gb.password` and other secret-like keys are rejected. Load secrets
separately with `LoadCredentials`; the file must be regular and mode `0600`
and uses the same `key=value` syntax, for example:

```text
sip.password=replace-me
```

Credential values are not included in parser or permission errors, and the
ordinary configuration has no secret fields.

## Recording index

The gateway recording store keeps its own in-memory index and append-only
`recordings.jsonl` journal. Create it and call `Scan(ctx)` from the gateway's
periodic recorder check:

```go
store, err := NewRecordingStore(
	"/var/lib/edge-gateway/recordings.jsonl",
	"/home/admin/edge/nvr",
)
```

Only `root/{camera_id}/{YYYYMMDD}/*.mp4` for the current day is scanned. A
file must keep the same size for two scans before `ffprobe` metadata is
recorded. `ListRecordings` returns overlap-filtered, `started_at`-ordered
results; `Remove` appends a tombstone and `Compact` atomically rewrites the
journal. The gateway owns the journal while the recorder owns media files.

## Run

Build and run the standalone gateway with a validated config and a separate
0600 credentials file:

```sh
mkdir -p tmp
go build -o tmp/gb-gateway ./cmd/gb-gateway
./tmp/gb-gateway -config /etc/edge-gateway/gateway.conf \
  -credentials /etc/edge-gateway/credentials
```

The default `ipc.status_dir` is `/var/lib/edge-gateway`, a durable directory
that preserves `channels.json` and stable channel mappings across reboot.
Override it only with another durable path. The UDS listeners remain under
`/run/edge-gateway` by default and stop on SIGINT or SIGTERM. A local lifecycle smoke test
is:

```sh
go test ./cmd/gb-gateway -run TestGatewayStartsSocketsBeforeCascadeAndStopsCleanly -count=1
```

## Gateway status consumer contract

`gateway-status.json` is replaced atomically with mode `0640`. Consumers may
read it at any time and must tolerate a missing file during startup or
shutdown. `channels[].last_health` is omitted until the first health message,
and `metrics.queue_depth` is the aggregate number of pending control commands
across all connected peers. The file is change-driven with a bounded
one-second publication cadence; consumers should not interpret its timestamp
as a heartbeat.

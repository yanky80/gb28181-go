# Cascade client: registering to an upper platform

`platform/cascade` makes *your* platform a LOWER platform in a GB/T
28181 cascade: it registers upward, uploads an aggregated catalog,
forwards INVITEs as streams, and serves playback from your recordings.

## Config

```go
cfg := cascade.Config{
    Enabled:       true,
    ProtocolVersion: "",                 // empty = GB/T 28181-2022
    ServerDomain:  "34020000002000000001", // upper platform's GB ID
    ServerAddr:    "192.0.2.20:5060",      // upper platform's SIP address
    LocalDeviceID: "34020000002000000002", // THIS platform's ID upward
    Realm:         "3402000000",
    Password:      "secret",
    SIPListen:     ":5061",                // cascade client's own SIP port
    UserAgent:     "",                     // "" = "gb28181-go/cascade"
}
```

The supported profiles are `2022+h265` (the default), `2022+h264`, and
`2016+h264`. An empty `CameraInfo.Encoding` keeps source compatibility and
selects H.265; set it explicitly to `h264` for an H.264 camera. REGISTER
advertises the selected version with `X-GB-Ver` and the cascade refuses
2022/H.265 media when the upper platform reports `2.0` or omits the marker.

Catalog identity defaults are neutral (`Unknown` manufacturer/model)
and overridable via `CatalogDefaultManufacturer` /
`CatalogDefaultModel` — your cascade advertises your brand, not the
library's.

## Host seams — all injected

```go
type CameraSource interface {
    Cameras() []CameraInfo          // your local camera list
    Hub(cameraID string) *platform.FrameHub // live frames per camera
}

// Optional: expose current local-camera state without changing CameraSource.
type CameraStatusSource interface {
    CameraStatus(cameraID string) string // "ON" or "OFF"
}

type MainStreamAcquirer interface {
    AcquireMainHub(ctx context.Context, cameraID string) (hub *platform.FrameHub, release func(), err error)
}

type Store interface {
    UpsertCascadeChannel(ctx, CascadeChannel) error
    ListCascadeChannels(ctx) ([]CascadeChannel, error)
    ListRecordings(ctx, RecordingFilter) ([]Recording, error)
}
```

When implemented, `CameraStatusSource` drives Catalog and DeviceStatus. Empty
or non-`ON` values report `OFF`; older sources retain the legacy `ON` behavior.
DeviceStatus accepts the local device ID and allocated GB channel IDs, reports
unknown or hidden channels as `OFF`, and formats time in the zone set by
`SetGBTimezone`.

Wire and start:

```go
svc := cascade.New(cfg, cameraSource, store)
// Optional: acquire/release the encoder's main live output per dialog
svc.SetMainStreamAcquirer(mainAcquirer)
// Optional: on-demand sub-stream tier
svc.SetSubStreamAcquirer(subAcquirer)
// Playback from recorded segments:
svc.SetSegmentParser(segmentParser)
svc.Start(ctx)
```

`SegmentParser` feeds playback from your recording pipeline: it
yields `SegmentInfo` (codec + parameter sets + timestamped samples)
per segment file; hosts with an fMP4 pipeline adapt their parser with
a thin wrapper (field names already match).

Playback and Download INVITEs are rejected with `503` before `200 OK` when
either `Store` or `SegmentParser` is not configured. `RecordInfo` remains
diagnosable: without a `Store` it returns an empty result. Playback windows
are half-open, empty windows return `404`, invalid ranges return `400`, and
recording results are paged in batches of 40 with `SumNum` carrying the total.

For recorded segments in the supported fragmented MP4 format,
`platform/mp4.ParseSegment` can be passed directly to `SetSegmentParser`.
Its H.264/H.265 samples use 4-byte big-endian NAL length prefixes;
other `lengthSizeMinusOne` values are rejected.

## What the upper platform sees

- **Catalog** — your cameras as channels, with **stable first-seen
  channel IDs** (upward identity survives restarts).
- **Live** — INVITEs from above subscribe your `FrameHub`, flow
  through `psmux`, and push RTP/PS upward (UDP or TCP).
- **Playback** — RecordInfo from your `Store`; configured playback INVITEs
  pump recorded segments through the same media path.
- **Signaling** — BYE/SUBSCRIBE/MESSAGE/INFO/OPTIONS all answered.

Multiple upper platforms are supported (`upstreams` config); each is
an independent registration session over the shared listener.

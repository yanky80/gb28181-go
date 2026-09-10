# Edge GB28181 Gateway

This context covers the box-side GB28181 lower-platform gateway and its reusable protocol components.

## Language

**GB gateway**:
The standalone lower-platform identity that exposes a box's local cameras to an upper GB28181 platform.
_Avoid_: GB server, media server

**Cascade service**:
The reusable protocol component that registers the GB gateway with an upper platform and handles signaling and media sessions.
_Avoid_: GB gateway

**Local camera**:
A camera known to the box before it is projected into the upper platform.
_Avoid_: Device, channel

**GB channel**:
The stable upper-platform identity assigned to one local camera and retained across rename, restart, disablement, and temporary outage.
_Avoid_: Camera ID

**Live output lease**:
The gateway's demand for a local camera's encoded live stream while a GB live session needs it.
_Avoid_: Stream process

**Stream epoch**:
One continuous lifetime of an encoder instance whose access-unit sequence starts from one.
_Avoid_: Session

**Recording segment**:
One indexed time interval of locally recorded media that can contribute samples to a GB playback session.
_Avoid_: Event clip

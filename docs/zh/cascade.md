# 级联客户端：注册到上级平台

`platform/cascade` 让**你的**平台成为 GB/T 28181 级联中的下级平台：
向上注册、上传聚合目录、把 INVITE 转发为流、从你的录像提供回放。

## Config

```go
cfg := cascade.Config{
    Enabled:       true,
    ProtocolVersion: "",                 // 空值 = GB/T 28181-2022
    ServerDomain:  "34020000002000000001", // 上级平台的国标 ID
    ServerAddr:    "192.0.2.20:5060",      // 上级平台 SIP 地址
    LocalDeviceID: "34020000002000000002", // 本平台对上的国标 ID
    Realm:         "3402000000",
    Password:      "secret",
    SIPListen:     ":5061",                // 级联客户端自己的 SIP 端口
    UserAgent:     "",                     // "" = "gb28181-go/cascade"
}
```

支持的 profile 为 `2022+h265`（默认）、`2022+h264` 和 `2016+h264`。空的
`CameraInfo.Encoding` 为保持源码兼容仍默认 H.265；H.264 相机请显式设为
`h264`。REGISTER 会用 `X-GB-Ver` 宣告所选版本；上级返回 `2.0` 或缺少该标识
时，级联会拒绝 2022/H.265 媒体，不会静默降级。

目录身份默认中性（厂商/型号 `Unknown`），经
`CatalogDefaultManufacturer` / `CatalogDefaultModel` 覆盖——你的级联
播报的是你的品牌，不是库的。

## 宿主接缝——全部注入

```go
type CameraSource interface {
    Cameras() []CameraInfo               // 本地相机清单
    Hub(cameraID string) *platform.FrameHub // 每台相机的实时帧
}

// 可选：不改变 CameraSource，提供本地相机当前状态。
type CameraStatusSource interface {
    CameraStatus(cameraID string) string // "ON" 或 "OFF"
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

实现 `CameraStatusSource` 后，Catalog 和 DeviceStatus 使用动态状态；空值或
非 `ON` 值均上报 `OFF`。未实现该接缝的旧来源保持目录原有的 `ON` 行为。
DeviceStatus 同时接受级联本地设备 ID 和已分配的 GB 通道 ID；未知或隐藏通道
上报 `OFF`，时间使用 `SetGBTimezone` 配置的时区。

接线与启动：

```go
svc := cascade.New(cfg, cameraSource, store)
// 可选：按每个直播会话 acquire/release 主直播输出
svc.SetMainStreamAcquirer(mainAcquirer)
// 可选:按需子码流层
svc.SetSubStreamAcquirer(subAcquirer)
// 从录像段回放:
svc.SetSegmentParser(segmentParser)
svc.Start(ctx)
```

`SegmentParser` 从你的录像管线供给回放：每个段文件产出
`SegmentInfo`（编码 + 参数集 + 带时间戳的 sample）；fMP4 管线的宿主
用一个薄包装适配自己的解析器即可（字段名已对齐）。

## 上级平台看到什么

- **目录**——你的相机作为通道上报，**通道 ID 按首次出现保持稳定**
  （向上身份跨重启不变）。
- **直播**——上级的 INVITE 订阅你的 `FrameHub`，经 `psmux` 推
  RTP/PS 上行（UDP 或 TCP）。
- **回放**——RecordInfo 来自你的 `Store`；回放 INVITE 把录像段灌进
  同一条媒体路径。
- **信令**——BYE/SUBSCRIBE/MESSAGE/INFO/OPTIONS 全部应答。

支持多个上级平台（`upstreams` 配置）；每个是共享 listener 上的独立
注册会话。

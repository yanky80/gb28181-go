# `gb-gateway` 使用手册

`cmd/gb-gateway` 是盒端的 GB/T 28181 下级平台进程。它通过两个本地
Unix domain socket（UDS）接收推理/编码进程的控制和视频访问单元，再向
上级平台注册、发布目录并按需转发直播、回放和下载。

默认运行 profile 为 GB/T 28181-2022 + H.265 + SIP/UDP。只支持
`2022+h265`、`2022+h264` 和 `2016+h264`；一个进程内的所有摄像机必须
使用同一种编码。

本页是运维快速手册；[Edge IPC v1 规范](../edge-ipc.md) 是控制和媒体
线格式的权威定义。

## 1. 部署

### 前提

- Linux，且运行账户能绑定 `gb.sip_listen` 指定的 UDP 端口；
- Go 1.26（从源码构建时）；
- 推理/编码进程与网关能够访问相同的 UDS 路径；
- 启用录像索引时，PATH 中必须有 `ffprobe`，录像文件放在
  `${ipc.status_dir}/recordings/{camera_id}/{YYYYMMDD}/`；
- 控制 socket 的权限固定为 `0600`，因此 C++ peer 必须与网关使用同一
  Unix 用户。媒体 socket 为 `0660`，但这不能替代控制 socket 的同用户
  要求。

### 构建与安装

在构建机执行。交叉构建不需要 cgo：

```sh
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 \
  go build -trimpath -o gb-gateway ./cmd/gb-gateway

install -D -m 0755 gb-gateway /usr/local/bin/gb-gateway
install -d -m 0750 -o edgegw -g edgegw /etc/edge-gateway
install -d -m 0750 -o edgegw -g edgegw /var/lib/edge-gateway
```

下面的 systemd unit 是最小部署示例。`RuntimeDirectory` 创建
`/run/edge-gateway`，`StateDirectory` 创建持久的
`/var/lib/edge-gateway`；两者都归 `edgegw` 所有。

```ini
# /etc/systemd/system/gb-gateway.service
[Unit]
Description=GB28181 edge gateway
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=edgegw
Group=edgegw
RuntimeDirectory=edge-gateway
RuntimeDirectoryMode=0750
StateDirectory=edge-gateway
StateDirectoryMode=0750
UMask=0077
ExecStart=/usr/local/bin/gb-gateway \
  -config /etc/edge-gateway/gateway.conf \
  -credentials /etc/edge-gateway/credentials
Restart=on-failure
RestartSec=2

[Install]
WantedBy=multi-user.target
```

```sh
systemctl daemon-reload
systemctl enable --now gb-gateway
systemctl status gb-gateway
journalctl -u gb-gateway -f
```

`SIGTERM` 和 `SIGINT` 会停止新会话、释放级联会话、关闭两个 socket，并
等待本地连接处理完成。不要用 `kill -9` 代替正常停机。

## 2. 启动参数与配置

二进制只有两个启动参数：

| 参数 | 默认值 | 说明 |
| --- | --- | --- |
| `-config` | `/etc/edge-gateway/gateway.conf` | 非敏感 `key=value` 配置文件 |
| `-credentials` | `/etc/edge-gateway/credentials` | 仅在 `gb.enabled=true` 时读取的 SIP 凭据文件 |

配置文件忽略空行和 `#` 后的注释；未知键、重复键和带有 `password` 或
`secret` 的键都会导致启动失败。凭据必须放在单独的普通文件中，权限必须
**恰为** `0600`。

### 最小可运行配置

```ini
# /etc/edge-gateway/gateway.conf
gb.enabled=true
gb.protocol_version=2022
gb.server_addr=192.0.2.10:5060
gb.server_domain=34020000002000000001
gb.local_device_id=34020000001320000001
gb.realm=3402000000
gb.sip_listen=:5061
gb.heartbeat_interval=60s
gb.register_expires=3600
gb.transport=udp
gb.media_transport=auto
gb.stop_grace_ms=5000
gb.idr_timeout_ms=3000
gb.record_playback=false

ipc.control_socket=/run/edge-gateway/control.sock
ipc.media_socket=/run/edge-gateway/media.sock
ipc.max_au_bytes=8388608
ipc.status_dir=/var/lib/edge-gateway

cam1.camera_id=camera-1
cam1.gb_expose=true
cam1.gb_name=Camera 1
cam1.gb_codec=h265
cam1.ptz_mode=none
```

```ini
# /etc/edge-gateway/credentials (chmod 0600)
sip.password=replace-with-platform-password
```

多摄像机使用连续的 `camN` 编号（从 `cam1` 开始，不可跳号），并且每个
`camera_id` 必须唯一。摄像机会在编码端连接并发送有效 `health` 前保持
OFF；OFF 摄像机仍可在目录中出现，但直播 INVITE 会被拒绝。

### 配置键

| 键 | 必填/默认 | 约束与含义 |
| --- | --- | --- |
| `gb.enabled` | 默认 `false` | 启用后才向上级注册，也才要求上级身份和凭据。关闭时两个 UDS 仍可启动。 |
| `gb.protocol_version` | `2022` | `2022` 或 `2016`。`2016+h265` 被拒绝。 |
| `gb.server_addr` | 启用时必填 | 上级 SIP `host:port`。 |
| `gb.server_domain`、`gb.local_device_id` | 启用时必填 | 各为 20 位数字 GB ID。 |
| `gb.realm` | 启用时必填 | SIP 摘要认证 realm。 |
| `gb.sip_listen` | `:5061` | 本地 UDP SIP 监听地址。 |
| `gb.heartbeat_interval` | `60s` | `1s` 到 `1h`。也作为录像扫描周期。 |
| `gb.register_expires` | `3600` | `1` 到 `86400` 秒。 |
| `gb.transport` | `udp` | 目前仅支持 `udp`。 |
| `gb.media_transport` | `auto` | 可填 `auto`、`udp`、`tcp-active`；当前用于配置校验，实际会话仍按上级 SDP 协商，不能用它强制覆盖单次协商。 |
| `gb.stop_grace_ms` | `5000` | 释放直播后发送 STOP 前的延迟，`0` 到 `60000`。 |
| `gb.idr_timeout_ms` | `3000` | WAIT_IDR 失败期限，`1` 到 `60000`。超时会使当前 epoch OFF 并释放相关会话。 |
| `gb.record_playback` | `false` | 启用录像索引、RecordInfo、Playback/Download；需要可用的录像目录和 `ffprobe`。 |
| `ipc.control_socket` | `/run/edge-gateway/control.sock` | 绝对路径，启动后权限 `0600`。 |
| `ipc.media_socket` | `/run/edge-gateway/media.sock` | 绝对路径，启动后权限 `0660`。 |
| `ipc.max_au_bytes` | `8388608` | 单访问单元上限，`1` 到 8 MiB。 |
| `ipc.status_dir` | `/var/lib/edge-gateway` | 绝对且持久的目录；保存 `channels.json`、`gateway-status.json` 和可选录像索引。 |
| `camN.camera_id` | 每个摄像机必填 | 1–64 字节 UTF-8，本地 IPC 身份；不是网关分配的 GB 通道号。 |
| `camN.gb_expose` | `true` | 是否在上级目录中发布。 |
| `camN.gb_name` | 空 | Catalog 显示名称。 |
| `camN.gb_codec` | `h265` | `h264` 或 `h265`；同一进程所有摄像机必须一致。 |
| `camN.ptz_mode` | `none` | `none`、`local-gb28181`、`onvif`、`vendor`（兼容 `gb28181`）。未接入 Adapter 时不会宣称 PTZ 能力。 |

`gateway-status.json` 位于 `ipc.status_dir`，以原子替换方式写入，权限为
`0640`。它是变更驱动快照，不是每秒心跳；消费者必须容忍启动/停止期间文件
暂不存在。

## 3. UDS 接入

两个 socket 都是 Unix `SOCK_STREAM`：

| Socket | 方向 | 格式 | 连接约束 |
| --- | --- | --- | --- |
| `control.sock` | 双向 | UTF-8 JSONL | 连接后 5 秒内第一条必须是 `hello`；最多 64 个握手中/已连接 peer；一个连接绑定一个摄像机。 |
| `media.sock` | C++ → Gateway | EGAU 二进制帧 | 第一帧绑定 `camera_id`；同一摄像机的新连接替换旧连接。 |

控制连接的 `hello` 通过后，编码端应持续发送 `health`。网关每 5 秒检查，
连续 15 秒未收到有效 health 即将该 epoch 标为 OFF；此后必须重连并用**新的**
`stream_epoch` 发送 `hello`，旧 epoch 的 health 或媒体不会复活它。

### 3.1 控制协议：JSONL

每行一个 UTF-8 JSON 对象，行尾 LF，整行（包括 LF）最大 16 KiB。CRLF 和
EOF 结尾的最后一行兼容；未知字段可忽略，未知类型、坏 JSON 或非法字段会被
拒绝。所有消息都有 `type` 和 `version: 1`。

| `type` | 方向 | 必填字段 |
| --- | --- | --- |
| `hello` | C++ → Gateway | `camera_id`、正数 `pid`、`codecs`（`h264`/`h265` 数组）、非零 `stream_epoch` |
| `start` | Gateway → C++ | `camera_id`、非零数字 `request_id` |
| `request_idr` | Gateway → C++ | `camera_id`、非零数字 `request_id` |
| `stop` | Gateway → C++ | `camera_id`、非零数字 `request_id`、`grace_ms` |
| `ack` | C++ → Gateway | 非零数字 `request_id`、非空 `state` |
| `ready` | C++ → Gateway | `codec`、非零 `width`、非零 `height` |
| `health` | C++ → Gateway | 非空 `rtsp`、有限且非负的 `infer_fps`、非空 `encode` |
| `error` | C++ → Gateway | 非空 `code`、布尔 `retryable` |

`request_id` 是 JSON 数字而非字符串；网关在进程生命周期内单调递增。`hello`
声明的 codec 必须包含配置 codec，`ready.codec` 也必须与之相同。每个
`start` 应答一次 `ready` 与 `ack`；每个 `request_idr`/`stop` 应答对应
`ack`。重复或乱序 ACK 只作观测，不会回滚网关的期望状态。

典型 H.265 会话：

```json
{"type":"hello","version":1,"camera_id":"camera-1","pid":420,"codecs":["h265"],"stream_epoch":1}
{"type":"start","version":1,"camera_id":"camera-1","request_id":1}
{"type":"ready","version":1,"codec":"h265","width":1920,"height":1080}
{"type":"ack","version":1,"request_id":1,"state":"started"}
{"type":"health","version":1,"rtsp":"rtsp://127.0.0.1/live/1","infer_fps":25,"encode":"h265"}
```

### 3.2 媒体协议：EGAU

媒体流是连续帧；每帧为 32 字节固定大端序头、UTF-8 `camera_id` 和 Annex-B
访问单元：

| 偏移 | 长度 | 字段 | 规则 |
| ---: | ---: | --- | --- |
| 0 | 4 | magic | ASCII `EGAU` |
| 4 | 1 | version | `1` |
| 5 | 1 | flags | bit 0=`IDR`，bit 1=`DISCONTINUITY`，其余位为 0 |
| 6 | 1 | codec | `1`=H.264，`2`=H.265 |
| 7 | 1 | reserved | `0` |
| 8 | 2 | header_len | 大端 `32` |
| 10 | 2 | camera_id_len | 大端，`1..64` |
| 12 | 4 | payload_len | 大端，`1..8 MiB`，并受 `ipc.max_au_bytes` 限制 |
| 16 | 8 | pts_90khz | 大端，90 kHz PTS |
| 24 | 8 | sequence | 大端，访问单元序号 |

每个 payload 必须是一个完整 Annex-B AU，且 `camera_id`、codec 和当前控制
epoch 的配置匹配。首帧要求 `sequence=1`；之后 sequence 必须加一且 PTS 严格
递增。出现断号、PTS 倒退、`DISCONTINUITY`、坏 AU、连接替换都会进入
`WAIT_IDR`：网关只发一次 `request_idr`，丢弃非 IDR AU，直到收到有效 IDR 或
达到 `gb.idr_timeout_ms`。

- H.264 的 IDR 帧必须包含 SPS、PPS 和 NAL type 5；
- H.265 的 IDR 帧必须包含 VPS、SPS、PPS 和 NAL type 19 或 20；
- `WAIT_IDR` 时被丢弃的帧不会推进 sequence/PTS 基线。

不要在任一 socket 上混入日志、长度前缀或其它协议字节。C++ 端应处理短写，
但网关可处理任意短读和粘包。

## 4. 验收与排障

```sh
# 配置与凭据权限
stat -c '%a %n' /etc/edge-gateway/credentials
sudo -u edgegw /usr/local/bin/gb-gateway \
  -config /etc/edge-gateway/gateway.conf \
  -credentials /etc/edge-gateway/credentials

# 启动后检查
ss -xl | grep edge-gateway
ls -l /run/edge-gateway/{control,media}.sock
cat /var/lib/edge-gateway/gateway-status.json
```

常见故障：

| 现象 | 优先检查 |
| --- | --- |
| 启动即失败 | 配置键是否重复/未知；GB ID 是否为 20 位数字；凭据是否普通文件且 `0600`。 |
| 控制端立即断开 | 第一行是否在 5 秒内发送 `hello`；`camera_id`、codec、`stream_epoch` 是否有效。 |
| Catalog 中是 OFF 或 INVITE 返回不可用 | 是否持续发送有效 `health`；epoch 是否已经因超时退休；`gb_expose` 是否为 true。 |
| 一直没有视频 | EGAU 的 `camera_id`/codec 是否匹配；序号与 PTS 是否连续；IDR 是否带齐 H.264 SPS/PPS 或 H.265 VPS/SPS/PPS。 |
| 回放不可用 | `gb.record_playback=true`、`ffprobe` 可执行、录像路径及片段稳定性是否满足要求。 |

协议字段、错误边界和二进制示例以 [Edge IPC v1](../edge-ipc.md) 为准；变更
该协议必须先提升 schema 版本，不能在 v1 上静默改变含义。

# 盒端 GB28181 设计—代码差距

> 修订日期：2026-09-09  
> 最新设计：`cki-cv-project` 提交 `eee31d21dd8d347a617ce0db751347d39839be7d` 的 `docs/盒端系统详细设计.md`。本地 `master` 停在 `0b4c0a5`，但 `origin/master`、`github/master` 与 `docs/box-system-design-gb28181-rk3588` 均指向 `eee31d2`；下文“设计”行号均指该 blob。  
> 当前代码：`gb28181-go` HEAD `43f84652195d55dd03b785435b0e17ce18c8b2bb`（`main`、`origin/main`、`upstream/main` 一致）。  
> 对比基线：上一版报告为设计 `0b4c0a5` / 代码 `5af7002`。  
> 验证：`go test ./...` 与 `go test -race ./...` 均通过（14 个包、575 项）。这不替代真实上级、摄像机及 4G/CGNAT 验收。

## 结论

最新设计已冻结为 **GB/T 28181-2022 + H.265 默认主路径**；**GB/T 28181-2016 + H.264** 和 **2022 + H.264** 只是显式配置的兼容路径，不允许静默降级或并行启动第二个编码器（设计 `:6,62-79,81-106`）。

当前仓库仍是可复用的下级平台协议内核，不是完整盒端 `gb-gateway`：

- H.265 PS/RTP 已有，PSM stream type 为 `0x24`（`psmux/mux.go:13-18,52-63`）。
- `cascade.Config` 没有 `ProtocolVersion`，REGISTER 构造/发送没有 `X-GB-Ver`，也没有上级版本解析或 `VERSION_MISMATCH` 状态（`platform/cascade/seam.go:8-71`; `platform/cascade/service.go:434-472,504-545`）。
- `cmd/gb-gateway`、UDS 服务端、CameraRegistry、GatewayStore 实现、PTZAdapter、fMP4 parser/index 和 `docs/edge-ipc.md` 均不存在；这些现在明确属于本仓（设计 `:256-294,769-807`）。
- DeviceStatus、动态 ON/OFF、主直播流 acquire/release 和完整源可用性仍缺失；回放能力门控已由 issue #14 补齐：未配置 Store/SegmentParser 时 Playback/Download 在 200 OK 前拒绝（设计 `:283-292`；代码 `platform/cascade/service.go`, `platform/cascade/playback.go`）。

因此 Wayfinder 地图必须以 2022/H.265 为主干，并覆盖本仓的协议 profile、网关宿主、PTZ 和回放；旧地图中“2016/H.264 主路径”“H.265 条件启用”以及“网关宿主在盒端仓库”的决定全部作废。

## 新版变更

### 设计：`0b4c0a5` → `eee31d2`

1. 默认协议从 GB/T 28181-2016 改为 2022，默认直播编码从 H.264 改为 H.265（设计 `:6,24,62-75,81-106`）。
2. 新增唯一合法组合：`2022+h265`、`2022+h264`、`2016+h264`；`2016+h265`、2011 和未知版本必须拒绝启动（设计 `:96-106,271-294,585-591`）。
3. REGISTER 及响应必须使用 `X-GB-Ver`：2022=`3.0`，2016=`2.0`；2022/H.265 遇到响应 `2.0` 或缺少标识进入 `VERSION_MISMATCH`，禁止启动媒体，不得运行时静默切换（设计 `:96-106`）。
4. 默认 IPC `hello`/`ready` codec 改为 H.265；媒体 framing 仍同时支持 H.264/H.265（设计 `:323-350,361-396`）。
5. 配置新增 `gb.protocol_version=2022`，`camN.gb_codec` 默认 `h265`（设计 `:557-590`）。
6. 验收新增版本矩阵、`X-GB-Ver: 3.0`、PSM `0x24`、VPS/SPS/PPS、真上级解码及版本不匹配拒流；以 2016 标识发送 H.265 或无版本标识却宣称 2022 成为一票否决项（设计 `:727-767`）。
7. 阶段 1 改为先打通 2022/H.265；2016/H.264 与 2022/H.264 被移入条件性能力（设计 `:809-834`）。

PTZ、回放、网关归属和两仓 seam 未再次改变：PTZ 仍是阶段 2，回放仍是阶段 3；Go 网关源码仍归本仓（设计 `:488-555,769-830`）。

### 代码：`5af7002` → `43f8465`

- REGISTER 的固定 15 秒重试已替换为可配置指数退避，成功后重置（`platform/cascade/seam.go:40-47`; `platform/cascade/service.go:306-350`）。旧报告的“固定间隔”缺口已关闭；当前退避没有 jitter，网络/NAT 变化后的旧 Dialog 清理也仍未关闭（`backoff/backoff.go:1-32`）。
- 新增通用 metrics hooks，但 `cascade` 仍只暴露注册状态、注册时长和活动转发数，网关级 IPC/录像/PTZ 指标仍待实现（`metrics/metrics.go:1-46`; `platform/cascade/service.go:371-405`）。
- 已有 tag 驱动的测试、多平台工具构建、校验和与 GitHub Release 工作流，且矩阵包含 linux/arm64；发布任务只需把未来的 `cmd/gb-gateway` 和 schema 元数据接入现有流程，不再新建框架（`.github/workflows/release.yml:1-109`）。
- 增加了库级安全、校验、基准和快照能力，但未新增 `cmd/gb-gateway`，也未实现本设计新要求的协议版本 profile。
- H.265 mux 能力没有变化，能生成 PSM `0x24` 不等于已经实现 2022 profile；版本标识、组合校验和真平台互通仍是交付条件。

## 本仓范围与差距

| 领域 | 最新设计 | `43f8465` 证据 | 结论 |
| --- | --- | --- | --- |
| 2022 profile | 2022/H.265 默认；REGISTER 双向 `X-GB-Ver: 3.0`；不匹配拒流（设计 `:96-106,283-292`） | Config 无版本字段；REGISTER 仅追加 Expires/Digest（`platform/cascade/seam.go:8-71`; `platform/cascade/service.go:504-545`） | **新增最高优先级缺口** |
| 编码组合 | 仅允许 2022/H.265、2022/H.264、2016/H.264（设计 `:273,564,590`） | `SetVideoCodec` 对未知值默认 H.264，无版本约束（`psmux/mux.go:52-63`） | **需 fail-fast 校验** |
| 网关宿主 | Config、Registry、IPCServer、Store、PTZ、Health（设计 `:256-281`） | 无 `cmd/`；只有注入 seam | **本仓缺口** |
| DeviceStatus | 网关与通道状态应答（设计 `:287`） | `onMessage` 无 `CmdDeviceStatus` 分支（`platform/cascade/service.go:642-673`） | **缺失** |
| 动态目录 | health 驱动 ON/OFF，并触发 Catalog NOTIFY（设计 `:275-281,288`） | `CameraInfo` 无在线状态；Catalog 固定 ON（`platform/cascade/service.go:53-58`; `platform/cascade/catalog.go:62-74`） | **缺失** |
| 生命周期 | 主流首租约 acquire，BYE/错误 release；源失联 teardown（设计 `:289-290,439-473`） | 仅 `SubStreamAcquirer`；主流直接 `Hub()`（`platform/cascade/service.go:60-65`; `platform/cascade/media.go:201-219,268-280`） | **缺失** |
| SDP/来源 | TCP-active 仅接受 `setup:passive`；只接受配置上级（设计 `:298-305,650-656`） | 任意 TCP/RTP/AVP 均设 TCP；未知多上级回退首项（`platform/cascade/media.go:98-138`; `platform/cascade/service.go:412-432`） | **边界缺口** |
| UDS/WAIT_IDR | 严格 framing、sequence/epoch、完整参数集和 3 秒 IDR 超时（设计 `:307-396`） | FrameHub 仅有界丢帧，无 UDS/epoch/WAIT_IDR（`platform/framehub.go:14-35,61-113`） | **网关缺口** |
| PTZ | Adapter、幂等/超时 Stop、拒绝并审计无等价命令（设计 `:488-505`） | 基础方向/对角/zoom/stop 与拒绝路径已有；无 Adapter、租约超时或审计 sink（`platform/cascade/records.go:101-169,189-233`） | **内核已有，宿主缺口** |
| 回放 | index、fMP4 parser、H.264 AVCC/H.265 HVCC、跨片段控制；未就绪时关闭（设计 `:519-555`） | Store/SegmentParser seam 和控制泵已有；issue #14 增加 nil capability 门控、空窗口/半开窗口、分页时区和片段元数据预检 | **能力门控已关闭；索引/parser 仍是交付缺口** |
| 发布 | 同提交构建 `cmd/gb-gateway`，arm64 无 cgo 独立附件（设计 `:801-807`） | Go 1.26；release workflow 已存在，但无 gateway target（`go.mod:1-10`; `.github/workflows/release.yml`） | **目标缺失** |

## Wayfinder 地图/任务影响

现有或待建任务应按以下顺序修正：

1. **先新增“冻结协议版本 profile”决策/任务**：API 兼容方式、`ProtocolVersion` 默认值、`X-GB-Ver` 注入/解析、`VERSION_MISMATCH` 状态及合法组合表。它阻塞 REGISTER、配置、INVITE 和 H.265 验收。
2. **把默认 codec 全部改为 H.265**：IPC 示例、Gateway Registry、START/ready、SDP/PSM/参数集检查；H.264 不再是默认实现主线。
3. **把版本矩阵并入现有 conformance 任务**：三种合法组合成功，`2016+h265`/2011/未知值失败，上级 `2.0`/缺标识时 2022/H.265 禁止 INVITE；验证 PSM `0x24` 与 VPS/SPS/PPS。无需再建纯测试任务。
4. **保留阶段 1 网关任务**：`cmd/gb-gateway`、Registry、UDS、Store、动态状态、DeviceStatus、生命周期、安全和 arm64 发布，均属本仓。
5. **保留阶段 2 PTZ 任务**：基础 decode 可复用；只拆 Adapter、超时 Stop、审计与跨仓电子围栏暂停 seam。
6. **保留阶段 3 回放任务**：录像索引、fragmented MP4 parser、能力门控和跨片段/清理并发；默认录像/直播通常均为 H.265，但仍分别是原始主码流与 AI 结果流（设计 `:507-555`）。
7. **撤销旧任务中的过时表述**：“2016/H.264 首期主路径”“H.265 后置可选”“把 `gb-gateway` 源码放入 `rk3588-test`”都不再成立。现有 issue 没有一项因此整体完成或整体越界，所以不关闭任务，只收缩已由上游完成的部分。

## 本仓之外

`rk3588-test` 仍独占 C++ 视频/推理/叠框/编码、UDS 客户端以及 systemd/安装配置；本仓独占 Go 网关、UDS 服务端、状态格式和测试。两仓仅通过版本化 UDS seam 协作（设计 `:769-807`）。真实双摄像机、4G/CGNAT、跨进程、真上级及独立平台互通仍是整体验收门槛（设计 `:739-767`），但不应伪装成本仓可独立完成的实现任务。

# 运行时使用

Transport × Codec 按名注册与 `ServiceOptions` 见 [codec-and-wiring.md](codec-and-wiring.md)。本文：组合层行为、Options、错误与调用约定。

## 客户端 / 服务端路径

```
client.New(argos.WithServiceName(name), ...)
  → ClientOptions（WithClientOptions 或进程默认）
  → 按注册名装配 transport + codec
Open(ctx, Method)
  → 准入 → CallMetadata → OpenFilter
      → Resolver → 池借 Session（未命中则 Dial + NewClientSession）
      → OpenCall → stream.Wrap
调用结束 → Call.Close → 池按 Reusable() / 引用计数归还或关闭

server: Transport.Serve → Conn → NewServerSession
  → loop AcceptCall → 准入 → 两级路由 → Accept → Filter → handler
  → Finish → Close；EOF 后 Session.Close
```

- 会话池维护**在途调用引用计数**；`Sequential` / `OneCallPerConn` 承载力为 1，`Concurrent` 无上限（忙则 `ErrSessionBusy`）。池归**轴**所有：每端点会话数、空闲保留、空闲回收与寿命都在轴构造期定死（`grpc.WithPool(...)`、`example/resp.WithPool(...)`），`argos.Options` 上不再有这些字段。
- **`MaxBufferedBytes`**：组合层按 `Options.PerCall()`（`MaxFrameSize + (ReadAheadMessages+1) × MaxMessageSize + MaxMessageSize`，默认 16 MiB）为一次调用预留，放进调用 ctx 的 `budget.Budget`；`grpc`、`httpunary` 等在读写路径 `TryAcquire`。服务端在 `admit` 后通过 `transport.BudgetSetter` 晚绑定到 Call。`MaxConcurrentCalls × perCall ≤ MaxBufferedBytes` 在 `ClientOptions` / `ServerOptions` 校验。
- **生命周期**：`transport.Transport` 接口上没有 `Close`——要显式释放的实现自带（如 `transport/grpc`），由**构造方**调用；组合层不持有可释放资源。资源在 `Conn` / `Session` / `Call`。

## 调用收尾约定

- 客户端读完业务消息后须读到**终态**（STATUS / trailers）再 `Close`；未读终态的会话不得回池。
- `client.Client` **没有 `Close`**：连接与会话池都在轴里，Client 不持有它们。丢弃未 `Close` 的 `CallStream` 由 `runtime.AddCleanup` 兜底——经 `WithClientCallErrorObserver` 上报并回收准入额度，连接不回池；无进程级连接池。
- `HalfClose` 结束发送，不关闭连接；`Call.Close` 结束本次调用。
- 跨交换的连接级状态（事务等）建模为**一次长期双向流**；内核无会话亲和 API。

## Options

`argos.Options` 为普通结构体：字段写关心的，零值即内置默认。进程默认 `argos.DefaultOptions()`；`client.New` / `server.New` 未指定 `With*Options` 时从其快照。

```go
argos.DefaultOptions().MaxMessageSize = 8 << 20

opts := &argos.Options{MaxMessageSize: 1 << 20, Filters: []filter.Filter{auth}}
srv := server.New(argos.WithServerOptions(opts), argos.WithListenAddress(":7001"))

cli, err := client.New(
    argos.WithServiceName("echo.v1.EchoService"),
    argos.WithTransport("grpc"),
    argos.WithCodec("protobuf"),
    argos.WithTarget("ip://127.0.0.1:7001"),
    argos.WithMaxMessageSize(4<<20),
)
```

- 构造时 **clone 并校验**；改原 `*Options` 不影响已建实例。改 `DefaultOptions()` 须在建任何实例之前。
- **零值 = 默认**；会话数、空闲与寿命等可选上限在轴上，按其选项语义解释（如 `WithPool` 的 maxIdle 传 0 表示不留空闲连接）。
- `MaxMessageSize` / `MaxFrameSize` / `ReadAheadMessages` **只喂 `Options.PerCall()`**，是准入预算输入，不设 wire 上限；wire 上限由轴定死，见下「默认值」。
- **Option 分端**：`ClientOption` → `client.New`；`ServerOption` → `server.New`。
- 客户端：`WithTransport` / `WithCodec` / `WithTarget` 覆盖 `Services[name]`；服务名只来自 `WithServiceName`。
- `server.New` 不返回 error；非法组合由 `Run` 报错。TLS 在 `transport/http2` 与 `transport/http1`（`WithServerTLS` / `WithClientTLS`）；gRPC 压缩在 `grpc`。

### 默认值

`argos.Options` 的字段：

| 选项 | 默认 | 说明 |
|---|---|---|
| `MaxFrameSize` / `MaxMessageSize` | 4 MiB | 准入预算的输入，**不是**线上限额 |
| `ReadAheadMessages` | 1 | 同上 |
| `MaxHeaderBytes` | 1 MiB | HTTP 头块 |
| `MaxConcurrentCalls` | 64 | 在途调用 |
| `MaxBufferedBytes` | 1 GiB | 与 perCall 交叉校验 |
| `HandshakeTimeout` | 10s | 连接级握手（握手子 ctx） |
| `ConnReadBufferSize` | 64 KiB | 连接读缓冲 |
| `MaxInboundConns` | 1024 | 服务端入站连接 |
| `MaxInboundConnIdle` | 50s | 服务端空闲 |
| `MaxInboundConnAge` | 30m | 服务端寿命 |
| `HTTPReadHeaderTimeout` | 10s | HTTP 头 / upgrade |
| `HTTPIdleTimeout` | 50s | keep-alive 空闲 |

默认 `perCall ≈ 16 MiB`，`64 × 16 MiB = MaxBufferedBytes`。

`MaxFrameSize` / `MaxMessageSize` / `ReadAheadMessages` 只喂 `Options.PerCall()`：组合层拿它们估一次调用要预留多少字节（见上「客户端 / 服务端路径」），**不**拿来限线；一次调用实际能缓冲多少由轴决定。会话与池的限额不在 `argos.Options` 上，而是在轴构造期定死：

| 轴选项 | 定死的限额 |
|---|---|
| `WithLimits(transport.Limits{...})` | `MaxMessageSize` / `MaxFrameSize` / `MaxMetadataSize` / `MaxInboundMetadataSize` / `ReadAheadMessages` / `OpenTimeout` / `MaxDrainBytes` |
| `WithPool(maxPerEndpoint, maxIdle, idleTimeout, maxLifetime)` | `MaxSessionsPerEndpoint` / `MaxIdleSessions` / `SessionIdleTimeout` / `MaxSessionLifetime` |
| `WithHandshakeTimeout(d)` | 池的握手超时：一次拨号 + `NewClientSession`（与 `Options.HandshakeTimeout` 的服务端握手子 ctx 是两件事） |

两处同名的 `MaxFrameSize` / `MaxMessageSize` 不是同一个东西：轴上的那两个是 wire 上限，`Options` 上的只是预算输入。`grpc` / `httpunary` 提供全套选项，不传即 `internal/session` / `internal/sessionpool` 的 `DefaultOptions()`（帧/消息 4 MiB、metadata 256 KiB（入站 4 MiB）、ReadAhead 1、OpenTimeout 10s、drain 1 MiB；池 64 / 8 / 50s / 30m、握手 10s）。`example/synth` 同名选项只吃它检查得动的几项——`WithLimits` 里 metadata 字段被忽略（协议没有 metadata），帧/消息 4 MiB、OpenTimeout 10s、drain 1 MiB 是它自己的默认。`example/resp` 只提供 `example/resp.WithOpenTimeout` + `WithPool`：协议不限的维度既不接受配置也不报告（`Limits()` 里是零值，如 RESP2 没有 metadata 可限）。

## 错误与状态

`status.Code` 采用 gRPC 17 个数值（0–16）；`status` 不依赖 protobuf。

```go
status.Error(code, msg)
status.CodeOf(err)           // nil → OK
status.WithDetails(err, ...) // 中性 Detail；grpc 翻译为 Any
```

| 哨兵 | 含义 |
|---|---|
| `status.ErrCardinality` | 响应条数与形态不符 |
| `status.ErrSessionsExhausted` | 会话池耗尽 |
| `status.ErrCallsExhausted` | 调用准入耗尽 |

连接级错误经 `WithClientConnErrorObserver` / `WithServerConnErrorObserver` 上报，不使 `Transport.Serve` 返回。

## gRPC 生态可选包

Health、Reflection、客户端重试与同端口挂载方式见 [grpc-ecosystem.md](grpc-ecosystem.md)。可运行示例：`example/echo/grpc_ecosystem_test.go`。

## 本地验证

```bash
make verify    # test、race、lint、accept、generate、integration、deps
```

gvm 环境：`export GOROOT=/data/root/.gvm/1.27.1/go` 且 `PATH` 含 `$GOROOT/bin`。编码代理约定见 [AGENTS.md](../AGENTS.md)。

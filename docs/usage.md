# 运行时使用

三轴装配与 `ServiceConfig` 见 [codec-and-wiring.md](codec-and-wiring.md)。本文：组合层行为、配置、错误与调用约定。

## 客户端 / 服务端路径

```
client.New(argos.WithServiceName(name), ...)
  → 解析 Config（WithConfig 或进程默认）
  → 工厂建三元组 + 按 Reuse() 建空池
Open(ctx, Method)
  → 准入 → CallMetadata → OpenFilter
      → Resolver → 池借 Session（未命中则 Dial + NewClientSession）
      → OpenCall → stream.Wrap
调用结束 → Call.Close → 池按 Reusable() / 引用计数归还或关闭

server: Transport.Serve → Conn → NewServerSession
  → loop AcceptCall → 准入 → 两级路由 → Accept → Filter → handler
  → Finish → Close；EOF 后 Session.Close
```

- 会话池维护**在途调用引用计数**；`Sequential` / `OneCallPerConn` 承载力为 1，`Concurrent` 无上限（忙则 `ErrSessionBusy`）。空闲受 `MaxIdleSessions` / `SessionIdleTimeout` / `MaxSessionLifetime` 约束。
- **`MaxBufferedBytes`**：`perCall = MaxBufferedBytes / MaxConcurrentCalls` 放入调用 ctx 的 `budget.Budget`；`framing/grpc`、`framing/wholebody` 等在读写路径 `TryAcquire`。服务端在 `admit` 后通过 `framing.BudgetSetter` 晚绑定到 Call。
- **生命周期**：只有 `Transport` 在工厂接口上声明 `Close()`；资源在 `Conn` / `Session` / `Call`。`Framing` / `Codec` 工厂不得持有需释放的资源。

## 调用收尾约定

- 客户端读完业务消息后须读到**终态**（STATUS / trailers）再 `Close`；未读终态的会话不得回池。
- `Client.Close` 是契约：持有 Session、Transport 与会话池回收 goroutine。丢弃未 `Close` 的 Client 由 GC 兜底并经 `WithConnErrorObserver` 上报；无进程级连接池。
- `HalfClose` 结束发送，不关闭连接；`Call.Close` 结束本次调用。
- 跨交换的连接级状态（事务等）建模为**一次长期双向流**；内核无会话亲和 API。

## 配置

`argos.Config` 为普通结构体：字段写关心的，零值即内置默认。进程默认 `argos.DefaultConfig()`；`client.New` / `server.New` 未指定时从其快照。

```go
argos.DefaultConfig().MaxMessageSize = 8 << 20

cfg := &argos.Config{MaxMessageSize: 1 << 20, Filters: []filter.Filter{auth}}
srv := server.New(argos.WithConfig(cfg), argos.WithListenAddress(":7001"))

cli, err := client.New(
    argos.WithServiceName("echo.v1.EchoService"),
    argos.WithTarget("ip://127.0.0.1:7001"),
    argos.WithMaxMessageSize(4<<20),
)
```

- 构造时 **clone 并校验**；改原 `*Config` 不影响已建实例。改 `DefaultConfig()` 须在建任何实例之前。
- **零值 = 默认**；显式关闭可选限额：`argos.Disabled`（仅 `MaxIdleSessions` / `SessionIdleTimeout` / `MaxSessionLifetime`）。
- **Option 分端**：`Option` 通用；`ClientOption` → `client.New`；`ServerOption` → `server.New`。
- 客户端：三轴覆盖 / `WithTarget` > `Services[name]`；服务名只来自 `WithServiceName`。
- `server.New` 不返回 error；非法组合由 `Run` 报错。TLS 在 `transport/http2` 与 `transport/http1`（`WithServerTLS` / `WithClientTLS`）；gRPC 压缩在 `framing/grpc`。

### 默认值

| 选项 | 默认 | 说明 |
|---|---|---|
| `MaxFrameSize` / `MaxMessageSize` | 4 MiB | 线上帧 / 未压缩消息 |
| `MaxMetadataSize` | 256 KiB | |
| `MaxHeaderBytes` | 1 MiB | HTTP 头块 |
| `ReadAheadMessages` | 1 | 接收预读条数 |
| `MaxConcurrentCalls` | 64 | 在途调用 |
| `MaxBufferedBytes` | 1 GiB | 与 perCall 交叉校验 |
| `OpenTimeout` | 10s | 服务端 OPEN/headers |
| `HandshakeTimeout` | 10s | 连接级握手（握手子 ctx） |
| `MaxDrainBytes` | 1 MiB | 服务端残余帧丢弃上限 |
| `ConnReadBufferSize` | 64 KiB | 连接读缓冲 |
| `MaxSessionsPerEndpoint` | 64 | 客户端会话数 |
| `MaxIdleSessions` | 8 | 空闲保留 |
| `SessionIdleTimeout` | 50s | 客户端空闲回收 |
| `MaxSessionLifetime` | 30m | 客户端会话寿命 |
| `MaxInboundConns` | 1024 | 服务端入站连接 |
| `MaxInboundConnIdle` | 50s | 服务端空闲 |
| `MaxInboundConnAge` | 30m | 服务端寿命 |
| `HTTPReadHeaderTimeout` | 10s | HTTP 头 / upgrade |
| `HTTPIdleTimeout` | 50s | keep-alive 空闲 |

默认 `perCall ≈ 16 MiB`，`64 × 16 MiB = MaxBufferedBytes`。

## 错误与状态

`status.Code` 采用 gRPC 17 个数值（0–16）；`status` 不依赖 protobuf。

```go
status.Error(code, msg)
status.CodeOf(err)           // nil → OK
status.WithDetails(err, ...) // 中性 Detail；framing/grpc 翻译为 Any
```

| 哨兵 | 含义 |
|---|---|
| `status.ErrCardinality` | 响应条数与形态不符 |
| `status.ErrSessionsExhausted` | 会话池耗尽 |
| `status.ErrCallsExhausted` | 调用准入耗尽 |

连接级错误经 `WithConnErrorObserver` 上报，不使 `Transport.Serve` 返回。

## gRPC 生态可选包

Health、Reflection、客户端重试与同端口挂载方式见 [grpc-ecosystem.md](grpc-ecosystem.md)。可运行示例：`example/echo/grpc_ecosystem_test.go`。

## 本地验证

```bash
make verify    # test、race、lint、accept、generate、integration、deps
```

gvm 环境：`export GOROOT=/data/root/.gvm/1.27.1/go` 且 `PATH` 含 `$GOROOT/bin`。编码代理约定见 [AGENTS.md](../AGENTS.md)。

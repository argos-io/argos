# Session 与 Call（Transport 轴内）

对外只选配 **Transport × Codec**。握手、复用、消息边界、池化都在 **`transport.Transport` 实现**里完成（`transport/grpc`、`transport/httpunary`、`example/resp`、`example/synth` 等）；组合层只 `Dial` / `Serve`，**不**再选第三维。

实现侧用 `internal/session` 的类型与接口（`session.Framing`、`ClientSession`、`ServerSession`、`Call`）。下文写 **「分帧实现」** 时指的就是轴内这段逻辑，不是用户可见的独立工厂。

可依赖：`transport`, `descriptor`, `metadata`, `status`。  
**不可**依赖：`codec`（仅 `SessionSpec.CodecName` 字符串）、`compressor`（**仅** `grpc` 例外）、`client` / `server`。

## `session.Framing`（轴内，非选配维）

```go
type Framing interface {
    Reuse() ReuseModel                    // 常量，客户端池据此借还
    NewClientSession(ctx, Conn, SessionSpec) (ClientSession, error)
    NewServerSession(ctx, Conn, SessionSpec) (ServerSession, error)
}
```

完整 Transport 轴通常内嵌一个 `Framing` 实例，在 `OpenCall` / `Serve` 路径上调用 `New*Session`。

| `ReuseModel` | 含义 | 示例 |
|--------------|------|------|
| `OneCallPerConn` | 一条连接一次调用后关闭 | 自定义 × udp |
| `Sequential` | 可复用，同时最多 1 个 in-flight | example/resp × tcp |
| `Concurrent` | 多路并发调用 | grpc × http2, httpunary × http1 |

`New*Session`：

- 对 `Conn` 做窄接口断言；不匹配 → 返回 error（**不要** Dial）。
- 有连接级握手（HELLO/AUTH 等）在此完成；无握手则零 I/O 断言即可。
- 成功则 **Session 拥有 Conn**；失败则由拨号的轴（axis）关闭 Conn，**不是**组合层——组合层看不到 `Conn`。
- 握手应使用**握手子 ctx**（带 `HandshakeTimeout`），不要挂在无 deadline 的连接 ctx 上。

## `ClientSession`

```go
OpenCall(ctx, descriptor.Method, CallSpec) (Call, error)
Reusable() bool   // 无 I/O；见 internal/session/session.go 注释五种 false 情况
Close() error     // 关 Session + Conn
```

- `OpenCall`：为 HTTP 类协议构造 `RequestPreface` 并 `StreamConn.OpenStream`。
- 返回 `ErrSessionBusy` / `ErrSessionSpent` **仅**给 sessionpool 消费，不得泄漏到 `client.Open`。
- `OpenStream` 失败时：是否 `markBad` 视错误类型（如 `context.Canceled` 通常不应毒化 keep-alive）。

## `ServerSession` 与 `AcceptCall`

组合层对每条连接**只跑一个** `AcceptCall` 循环；分帧实现不得再开第二个 accept 循环。

| 返回 | 含义 |
|------|------|
| `(call, nil)` | 合法调用 |
| `(call, ErrCallRejected)` | 非法但连接仍可用；**必须**返回可 `Finish` 的 `ServerCall`，由 `server.handleRejected` 写状态 |
| `(nil, io.EOF)` | 正常结束（对端关闭、OneCall 已消费等） |
| 其他 error | 连接级；组合层结束循环并上报 |

实现义务（顺序/复用协议必读 `internal/session/session.go` 注释）：

1. **残余 drain**：上一 call 未读尽的帧须在解析下一 OPEN 前有限丢弃（`MaxDrainBytes`）。
2. **OpenTimeout**：首字节前只受 accept ctx；首字节后启动 Open 解析超时。
3. **Reject 可写回**：wrap `transport.ErrCallRejected` + `status.Error`（会话层自己的哨兵是 `session.ErrCallRejected`，由轴包成前者）；参考 httpunary/grpc 非法 path、example/resp 非法命令。

`MaxDrainBytes` 与 `OpenTimeout` 都由轴在构造期定死（`WithLimits(transport.Limits{...})` 的对应字段），分帧实现从 `SessionSpec.Options` 读到的就是这份快照；`argos.Options` 上没有这两个字段，不用去组合层找。

## `Call` / `ServerCall`

```go
type Call interface {
    Method() string
    Deadline() (time.Time, bool)
    SendHeaders() error          // 响应方；不支持则稳定 Unimplemented
    Recv() ([]byte, release, error)
    Send([]byte) error
    HalfClose() error            // 发起方
    Finish(error) error          // 响应方，恰好一次
    Close() error                // 不关闭 Conn；须同步 join 本 call 的读 goroutine
}
type ServerCall interface {
    Call
    Accept(descriptor.Method) error   // 仅 Shape/能力检查，无 I/O
}
```

- `Recv` 成功时 `release` 非 nil且须调用；`Close` 不回收仍被持有的 payload。
- 发送方向结束但接收仍开放：返回 `transport.SendError` 且 `ReceiveOpen()==true`。
- **Carrier 卫生**：未读到协议终态（STATUS / EOF）就 `Close` → `Session.Reusable()` 须变 false（Sequential 尤其重要）。

## 与 Pipe / Conn 的配对（断言清单）

在轴内实现 `NewClientSession` / `NewServerSession` 时，在代码里明确 assert：

| Transport 注册名 | 客户端 Conn | 服务端 Conn / Carrier | 常用 Carrier 接口 |
|------------------|-------------|------------------------|-------------------|
| `grpc` | `StreamConn` | 每 stream：`ByteStreamCarrier`, `ResponseWriter` 等 | 头/trailers |
| `httpunary` | `StreamConn` | 每请求 `CarrierConn`：`ByteStreamCarrier`, `UnaryResponseWriter`, `RequestHeaderReader` | unary only |
| `resp` / `synth`（example） | `CarrierConn` → `ByteStreamCarrier` | 同左 | + `SendCloser` |

不匹配组合在 `New*Session` 报错即可，无需组合层特判。

## 新 Transport 轴分帧检查单

- [ ] `Reuse()` 恒定；与真实并发行为一致
- [ ] `New*Session` 断言与握手文档化
- [ ] `AcceptCall` 三类错误语义 + reject 时非 nil `ServerCall`
- [ ] Sequential：`MaxDrainBytes`、OpenTimeout、idle/peer close → `Reusable`
- [ ] `Call.Close` 同步 join + 未读终态 → 不复用
- [ ] `SendError` / `ErrSendClosed` 与 grpc/httpunary 测试对齐
- [ ] 包依赖不违反 [architecture.md](architecture.md#分层与依赖)
- [ ] 测试：`internal/fake` 或真实 transport 环回

## `httpunary`

HTTP/1 unary 整包分帧（服务端需 `UnaryResponseWriter`，里程碑矩阵为 **http1**）。客户端 `OpenStream` 亦适用于 http2，但服务端 assert 以 http1 为准，**不以 http2 服务端为已验证组合**。

| 配置 | 路由 |
|------|------|
| `httpunary.NewTransport()`（默认） | RPC 式 `POST /{Service}/{Method}` |
| `httpunary.NewTransport(httpunary.WithREST(RESTConfig))` | `Binding{Method, Verb, Pattern}`；path 变量经 outgoing metadata `x-argos-path-<name>` |

`RequestPreface.Method` 与 `RequestHeaderReader.RequestMethod()` 由 `transport/http1`、`transport/http2` 实现；空 Method 表示 POST。

## 参考与反例

- **内置轴**：`grpc`、`httpunary`
- **连接级状态 / 非 gRPC 命令式 C/S**：`example/resp`、`example/synth`（轴内仍用 `session.Framing`，可自定义握手与 `Accept` 语义；证明组合层无需为具体协议特判）

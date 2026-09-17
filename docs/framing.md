# Framing 接入

包：`github.com/argos-io/argos/framing`（接口）+ 子包 `envelope` / `grpc` / `wholebody` 或自建 `framing/yourproto`。

可依赖：`transport`, `descriptor`, `metadata`, `budget`, `status`。  
**不可**依赖：`codec`（仅 `SessionSpec.CodecName` 字符串）、`compressor`（**仅** `framing/grpc` 例外）、`client` / `server`。

## `Framing` 工厂

```go
type Framing interface {
    Reuse() ReuseModel                    // 常量，客户端池据此借还
    NewClientSession(ctx, Conn, SessionSpec) (ClientSession, error)
    NewServerSession(ctx, Conn, SessionSpec) (ServerSession, error)
}
```

| `ReuseModel` | 含义 | 示例 |
|--------------|------|------|
| `OneCallPerConn` | 一条连接一次调用后关闭 | envelope × udp |
| `Sequential` | 可复用，同时最多 1 个 in-flight | envelope × tcp/ws |
| `Concurrent` | 多路并发调用 | grpc × http2, wholebody × http1 |

`New*Session`：

- 对 `Conn` 做窄接口断言；不匹配 → 返回 error（**不要** Dial）。
- 有连接级握手（HELLO/AUTH 等）在此完成；无握手则零 I/O 断言即可。
- 成功则 **Session 拥有 Conn**；失败则组合层关闭 Conn。
- 握手应使用**握手子 ctx**（带 `HandshakeTimeout`），不要挂在无 deadline 的连接 ctx 上。

## `ClientSession`

```go
OpenCall(ctx, descriptor.Method, CallSpec) (Call, error)
Reusable() bool   // 无 I/O；见 framing.go 注释五种 false 情况
Close() error     // 关 Session + Conn
```

- `OpenCall`：为 HTTP 类协议构造 `RequestPreface` 并 `StreamConn.OpenStream`。
- 返回 `ErrSessionBusy` / `ErrSessionSpent` **仅**给 sessionpool 消费，不得泄漏到 `client.Open`。
- `OpenStream` 失败时：是否 `markBad` 视错误类型（如 `context.Canceled` 通常不应毒化 keep-alive）。

## `ServerSession` 与 `AcceptCall`

组合层对每条连接**只跑一个** `AcceptCall` 循环；Framing 不得再开第二个 accept 循环。

| 返回 | 含义 |
|------|------|
| `(call, nil)` | 合法调用 |
| `(call, ErrCallRejected)` | 非法但连接仍可用；**必须**返回可 `Finish` 的 `ServerCall`，由 `server.handleRejected` 写状态 |
| `(nil, io.EOF)` | 正常结束（对端关闭、OneCall 已消费等） |
| 其他 error | 连接级；组合层结束循环并上报 |

实现义务（顺序/复用协议必读 `framing.go` 注释）：

1. **残余 drain**：上一 call 未读尽的帧须在解析下一 OPEN 前有限丢弃（`MaxDrainBytes`）。
2. **OpenTimeout**：首字节前只受 accept ctx；首字节后启动 Open 解析超时。
3. **Reject 可写回**：wrap `framing.ErrCallRejected` + `status.Error`；参考 envelope 空 method、wholebody/grpc 非法 path。

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
- **budget**：可从 `context` 读 `budget.FromContext`（当前 **envelope** 使用）；其他实现可选。

## 与 Transport 的配对（断言清单）

实现 `NewClientSession` / `NewServerSession` 时，在代码里明确 assert：

| Framing | 客户端 Conn | 服务端 Conn / Carrier | 常用 Carrier 接口 |
|---------|-------------|------------------------|-------------------|
| envelope + tcp/ws | `CarrierConn` → `ByteStreamCarrier` 或 `MessageCarrier` | 同左 | + `SendCloser` |
| envelope + udp | `CarrierConn` → `DatagramCarrier` | 同左 | |
| grpc | `StreamConn` | 每 stream：`ByteStreamCarrier`, `ResponseWriter` 等 | 头/trailers |
| wholebody | `StreamConn` | 每请求 `CarrierConn`：`ByteStreamCarrier`, `UnaryResponseWriter`, `RequestHeaderReader` | unary only |

不匹配组合在 `New*Session` 报错即可，无需组合层特判。

## 新 Framing 检查单

- [ ] `Reuse()` 恒定；与真实并发行为一致
- [ ] `New*Session` 断言与握手文档化
- [ ] `AcceptCall` 三类错误语义 + reject 时非 nil `ServerCall`
- [ ] Sequential：`MaxDrainBytes`、OpenTimeout、idle/peer close → `Reusable`
- [ ] `Call.Close` 同步 join + 未读终态 → 不复用
- [ ] `SendError` / `ErrSendClosed` 与 grpc/envelope 测试对齐
- [ ] 包依赖不违反 [architecture.md](architecture.md#分层与依赖)
- [ ] 测试：`internal/fake` 或真实 transport 环回

## 参考与反例

- **内置分帧（RPC / HTTP unary 等）**：`framing/envelope`、`framing/grpc`、`framing/wholebody`
- **连接级状态 / 非 gRPC 命令式 C/S**：`example/resp`、`example/synth`（仍实现同一套 `Framing` 接口，但可自定义握手与 `Accept` 语义；证明组合层无需为具体协议特判）

# Transport 接入

包：`github.com/argos-io/argos/transport`。包里是**两个**接口，别混：

| 接口 | 层面 | 实现 |
|------|------|------|
| **`Transport`** | 产品面：一次调用的字节、分帧、握手、复用与连接维护 | `transport/grpc`、`transport/httpunary`、`example/resp`、`example/synth` |
| **`Pipe`** | 字节面：拨号 / 监听，只产出 `Conn` | `transport/{tcp,ws,udp,http1,http2}`；完整线栈内部也用 |

接口真源：`transport/transport_impl.go`（`Transport` / `Call` / `ServerConn`）、`transport/transport.go`（`Pipe` / `Conn` / `Carrier`）。

依赖约定：根包可 import `descriptor` / `metadata`（`Transport` 签名需要），**不** import `codec`；字节管道实现连 `descriptor` / `metadata` / `internal/session` 也不得 import；完整线栈可 import `internal/session`、`internal/sessionpool`。由 `invariants_test.go` 强制。

## `Transport`（一等 axis）

```go
type Transport interface {
    OpenCall(ctx context.Context, endpoint string, m descriptor.Method, spec CallSpec) (Call, error)
    Serve(ctx context.Context, onConn func(context.Context, ServerConn), opts ...ServerOption) error
    CallConcurrency() Concurrency
    CodecName() string
}
```

| 方法 | 义务 |
|------|------|
| `OpenCall` | 按协议的复用模型建连或复用连接；`Call.Close` 归还借来的连接 |
| `Serve` | 每个入站连接调用一次 `onConn`；ctx 结束即停。连接**握手前**交来：握手超时与失败上报在组合层，组合层自己调 `ServerConn.Handshake` |
| `CallConcurrency` | 必须是常量：组合层只在 listen surface 起步时读一次，用于决定 handler 是否另起 goroutine |
| `CodecName` | 线上声明的 codec 名（如 gRPC content subtype）。与 Codec 自身的名字不符即装配报错，而不是让对端用错 codec 解 body。空表示协议不在线上声明 codec |

**限额与池不在接口里**：它们在轴构造期用轴自己的选项定死——`WithLimits(transport.Limits{...})`（会话级的帧/消息/metadata 上限、`ReadAheadMessages`、`OpenTimeout`、`MaxDrainBytes`）、`WithPool(maxPerEndpoint, maxIdle, idleTimeout, maxLifetime)`、`WithHandshakeTimeout(d)`，参数类型就是 `transport.Limits` / `transport.PoolLimits`。组合层既不安装也不核对它们：一个轴可能被多个 `Client` 共享，最后绑定的会替所有人做主，所以那些数字只有一个来源（构造处），没有第二份可对照的拷贝。`transport.Limits` 的尺寸字段零值表示该协议不约束这一维（如 RESP2 没有 metadata 可限）。内置轴另提供 `Limits()` / `PoolLimits()` 报告自己实际执行的限额，供自检与测试用——它们不在接口里，组合层不依赖。

**生命周期**：`Transport` 没有 `Close` / `Shutdown`——生命周期就是构造它的 ctx，`Serve` 随该 ctx 结束。limits / 池 / codec 名只在构造期用 `WithLimits` / `WithPool` / `WithCodecName` 定死，组合层既不 `Close` 也不改配置，装配期只核对一处：codec 名（`internal/transportbind.CheckCodecName`，轴声明的名字与 Codec 自己报的名字不一致即报错）。要显式释放的实现自带 `Close`（如 `transport/grpc`），由**构造方**调用。

**一实例一监听面**：一个实例可被多个 `Client` 共享，但只服务**一个** listen surface——轴拥有自己的 listener、会话限额与 codec 名，同一个实例绑两个地址 `Server.Run` 会直接报错（同一地址上的多个服务仍可共用一个实例）。

由此推出一条容易误判的语义：`Server` 停止（`Run` 的 ctx 结束，或 `Shutdown(ctx)` 返回）**只表示不再接受新调用**，不表示在途调用已结束。`Run` 只等各 listen surface 的 `Serve` 返回，而连接处理器跑在 transport 自己的协程上，server 从不 join 它们——所以组合层不调 `Pipe.Shutdown`，**排空连接是轴的所有者的事**：需要等连接结束就自己调轴（或其 `Pipe`）的 `Shutdown(ctx)`。

`Call` / `ServerCall` / `AcceptCall` 的义务（`ErrCallRejected` 须返回可 `Finish` 的 `ServerCall`、残余 drain、首字节后才起 OpenTimeout）见 [session.md](session.md)（Transport 轴内分帧，非第三选配维）。

## `Pipe`（字节面）

```go
type Pipe interface {
    Serve(ctx context.Context, onConn func(context.Context, Conn), opts ...ServerOption) error
    Dial(ctx context.Context, spec DialSpec, opts ...ClientOption) (Conn, error)
    Shutdown(ctx context.Context) error
    Close() error
}
```

| 方法 | 义务 |
|------|------|
| `Serve` | 监听（若适用）；每个入站连接调用一次 `onConn`；`ctx` 取消或 `Close` 应结束 `Serve` |
| `Dial` | `DialSpec.Endpoint` 由 resolver 解析后填入；只建连，不解析 RPC |
| `Shutdown` | 停止接受新连接，等待已有 `onConn` 结束；超时则打断未完成的连接 |
| `Close` | 幂等；释放 listener、连接与后台 goroutine |

服务端地址：`transport.WithListenAddress`；HTTP 系还可 `WithHTTPReadHeaderTimeout` / `WithHTTPIdleTimeout`（在 `onConn` 之前约束「连上不发请求」的 peer）。

## Conn 形态（二选一或兼有）

Transport 轴在 `New*Session` 里对 `Conn` **type assert**，失败则返回轴装配错误，由 axis 关掉这条 `Conn`（组合层看不到 `Conn`：连接是 axis 内部的事）。

| 接口 | 语义 | 典型实现 |
|------|------|----------|
| **`CarrierConn`** | 一条连接同一时刻承载**一次**交换；`Carrier()` 即该连接上的 Carrier | `tcp`, `ws`, `udp`；服务端 `http1` 每请求一个 `Conn` |
| **`StreamConn`** | 连接是端点句柄；每次调用 `OpenStream(ctx, RequestPreface)` 得独立 **Carrier** | `http2`；客户端 `http1` |

`RequestPreface` 由 **轴内分帧**（`OpenCall` / `AcceptCall` 路径）填写（`:path`、headers 等）；`Pipe` 不透明转发。

## Carrier 窄接口（按能力组合）

所有 Carrier 必须有 **`Abort()`**（幂等，只打断本次交换）。Carrier **没有** `Close`；连接级关闭在 `Conn` / `Session`。

| 窄接口 | 用途 |
|--------|------|
| `ByteStreamCarrier` | `io.Reader` + `io.Writer` 字节流（TCP body、H2 stream body） |
| `MessageCarrier` | `RecvMessage` / `SendMessage` 独立消息（WebSocket 帧） |
| `DatagramCarrier` | 一发一收整包（UDP） |
| `SendCloser` | `CloseSend()` 结束发送方向（FIN / END_STREAM） |
| `RequestHeaderReader` | 服务端：`:path` + 请求头 |
| `ResponseHeaderReader` | 客户端：状态码 + 响应头 |
| `ResponseTrailerReader` | 客户端：HTTP/2 trailers |
| `ResponseWriter` | 服务端 H2：`WriteHeaders` + `Finish`（可仅 trailers） |
| `UnaryResponseWriter` | 服务端 H1：一次 `WriteResponse(status, headers, body)` |

同一 concrete 类型可实现多个窄接口；轴内分帧只 assert 自己需要的那几个。

## 发送失败：`SendError`

发送路径失败时返回 `transport.WrapSendError(err, receiveOpen)`：

- `ReceiveOpen() == true`：对端仍可能返回响应（如 HTTP 200 + gRPC status）；轴内 `Call` 映射为 `stream.ErrSendClosed`，**Recv 继续**。
- `ReceiveOpen() == false`：本次交换接收方向也结束（如 UDP 发不出去、连接已死）。

参考：`transport/udp/senderror_test.go`、`transport/grpc/senderror_test.go`。

## HTTP 实现注意点

- **`OpenStream` 不必等响应头**即可返回可写 Carrier（§4.2）；响应头在 body 读写过程中就绪。
- 服务端：`Abort` / 超时写响应头时，**不得**在 net/http handler 已返回后再 `WriteHeader`（http1 用 `handlerDone` 守卫；见 `transport/http1/server.go` 与 `abort_after_handler_test.go`）。
- 客户端：`http.Client.Do` 若 `(resp, err)` 双非 nil，须关闭 `resp.Body`（见 `transport/http1/client.go`、`http2/client.go`）。

## 检查单

`Pipe`（字节面）：

- [ ] `Pipe` 四方法 + 测试覆盖 Serve/Dial/Shutdown/Close 路径
- [ ] 文档化返回的 `Conn` 是 `CarrierConn` 还是 `StreamConn`
- [ ] 每种 Carrier 实现的窄接口列表与 `Abort` 语义
- [ ] 发送失败是否正确使用 `SendError`
- [ ] HTTP：handler 生命周期与 body/leak 行为
- [ ] 不 import `codec` / `descriptor` / `metadata` / `internal/session`

`Transport`（线栈 axis）：

- [ ] 四方法齐全；`CallConcurrency` / `CodecName` 是构造期定死的常量报告
- [ ] 限额与池同样构造期定死（`WithLimits` / `WithPool` / `WithHandshakeTimeout`），组合层不安装、不核对；要对外报告就另加 `Limits()` / `PoolLimits()`（不在接口里）
- [ ] 新增的包在 `invariants_test.go` 的 `classifiedTransportPkgs` 里登记（管道 or 线栈），否则依赖门禁会失败
- [ ] 生命周期是构造它的 ctx：无 `Close` 时靠 `Serve` 的 ctx 结束；有 `Close` 时由构造方调用

## 参考实现

| 包 | Conn | 主要 Carrier 能力 |
|----|------|-------------------|
| `transport/tcp` | `CarrierConn` | `ByteStreamCarrier`, `SendCloser` |
| `transport/ws` | `CarrierConn` | `MessageCarrier`, `SendCloser` |
| `transport/udp` | `CarrierConn` | `DatagramCarrier` |
| `transport/http1` | 客户端 `StreamConn`；服务端每请求 `CarrierConn` | `ByteStreamCarrier`, `SendCloser`, 头/Unary 写响应 |
| `transport/http2` | `StreamConn` | `ByteStreamCarrier`, `SendCloser`, `ResponseWriter`, 头/trailers |

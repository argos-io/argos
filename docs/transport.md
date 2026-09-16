# Transport 接入

包：`github.com/argos-io/argos/transport`（接口）+ `transport/{tcp,ws,udp,http1,http2}`（实现）。**不得** import `descriptor` / `framing` / `codec`。

## 必须实现的接口

```go
type Transport interface {
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

服务端地址：`transport.WithListenAddress`；HTTP 系还可 `WithHTTPTimeouts`（在 `onConn` 之前约束「连上不发请求」的 peer）。

## Conn 形态（二选一或兼有）

Framing 在 `New*Session` 里 **type assert**，失败则返回配置错误（组合层会 `Close` Conn）。

| 接口 | 语义 | 典型实现 |
|------|------|----------|
| **`CarrierConn`** | 一条连接同一时刻承载**一次**交换；`Carrier()` 即该连接上的 Carrier | `tcp`, `ws`, `udp`；服务端 `http1` 每请求一个 `Conn` |
| **`StreamConn`** | 连接是端点句柄；每次调用 `OpenStream(ctx, RequestPreface)` 得独立 **Carrier** | `http2`；客户端 `http1` |

`RequestPreface` 由 **Framing** 填写（`:path`、headers 等）；Transport 不透明转发。

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

同一 concrete 类型可实现多个窄接口；Framing 只 assert 自己需要的那几个。

## 发送失败：`SendError`

发送路径失败时返回 `transport.WrapSendError(err, receiveOpen)`：

- `ReceiveOpen() == true`：对端仍可能返回响应（如 HTTP 200 + gRPC status）；Framing 映射为 `stream.ErrSendClosed`，**Recv 继续**。
- `ReceiveOpen() == false`：本次交换接收方向也结束（如 UDP 发不出去、连接已死）。

参考：`transport/udp/senderror_test.go`、`framing/grpc/senderror_test.go`。

## HTTP 实现注意点

- **`OpenStream` 不必等响应头**即可返回可写 Carrier（§4.2）；响应头在 body 读写过程中就绪。
- 服务端：`Abort` / 超时写响应头时，**不得**在 net/http handler 已返回后再 `WriteHeader`（http1 用 `handlerDone` 守卫；见 `transport/http1/server.go` 与 `abort_after_handler_test.go`）。
- 客户端：`http.Client.Do` 若 `(resp, err)` 双非 nil，须关闭 `resp.Body`（见 `transport/http1/client.go`、`http2/client.go`）。

## 新 Transport 检查单

- [ ] `Transport` 四方法 + 测试覆盖 Serve/Dial/Close 路径
- [ ] 文档化返回的 `Conn` 是 `CarrierConn` 还是 `StreamConn`
- [ ] 每种 Carrier 实现的窄接口列表与 `Abort` 语义
- [ ] 发送失败是否正确使用 `SendError`
- [ ] HTTP：handler 生命周期与 body/leak 行为
- [ ] 不 import `framing` / `descriptor`

## 参考实现

| 包 | Conn | 主要 Carrier 能力 |
|----|------|-------------------|
| `transport/tcp` | `CarrierConn` | `ByteStreamCarrier`, `SendCloser` |
| `transport/ws` | `CarrierConn` | `MessageCarrier`, `SendCloser` |
| `transport/udp` | `CarrierConn` | `DatagramCarrier` |
| `transport/http1` | 客户端 `StreamConn`；服务端每请求 `CarrierConn` | `ByteStreamCarrier`, `SendCloser`, 头/Unary 写响应 |
| `transport/http2` | `StreamConn` | `ByteStreamCarrier`, `SendCloser`, `ResponseWriter`, 头/trailers |

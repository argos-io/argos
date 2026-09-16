# argos v2

可组装的 RPC 运行时内核：**Transport × Framing × Codec** 三轴组成协议；**连接（`Conn` / `Session`）是一等事实**。定位是验证可组装模型，不是生产级通用 RPC 框架。

本文件是整体设计与用法说明。代理约定见 `AGENTS.md`；实现以代码与测试为准。

---

## 1. 目标与非目标

### 目标

1. **一套抽象同时覆盖流式与应答式**——unary 就是单元素流，内核不另开路径。
2. **协议 = 传输 × 分帧 × 编码**；不兼容组合在建连或开承载后明确失败，不静默降级。
3. **连接是一等事实**：`Transport` 产出 `Conn`，`Framing` 在其上建 `Session`，再切成 0..N 次调用。握手、复用、连接级状态都有落点。
4. **可组装性可测**：下列形状只靠三轴组装，组合层不为任一协议特判。

   | 形状 | 证明什么 |
   |---|---|
   | grpc × http2 | 多路复用、HTTP 控制面、trailers 状态 |
   | envelope × tcp/ws | 自有线格式、顺序复用 |
   | wholebody × http1 | 整次调用一次提交、无流式 |
   | envelope × udp | 无连接、单数据报往返 |
   | resp × tcp（`example/resp`） | 连接级 HELLO/AUTH、无 metadata、服务端长流 |
   | synth × tcp（`example/synth`） | 合成 Framing：服务端先发 greeting 等补齐性质 |

5. **原生 gRPC**：与 grpc-go 在 h2c / TLS·ALPN 下互通（形态、状态码、metadata、详情、deadline、压缩）。
6. **服务名是一等概念**：服务端按名登记，客户端按名打开调用。

### 非目标

- 配置文件 / 热加载 / 插件生态 / 进程级可变配置槽位
- 通用非 gRPC 的 HTTP/2 协议栈；http1 流式；udp 上的可靠传输或多路复用
- 完整 RESP / 数据库协议实现（example 仅为收门资产）
- 与 v1（`master`）线格式或状态码数值兼容

---

## 2. 概念模型

传输与分帧各有连接级与调用级，构成 2×2：

| | 连接级 | 调用级 |
|---|---|---|
| **transport** | `Conn` | `Carrier` |
| **framing** | `Session` | `Call` |

| 概念 | 职责 | 不做什么 |
|---|---|---|
| **Transport** | 监听/拨号、TLS/ALPN、产出 `Conn` | 不产出 Carrier；不解析 method；不决定复用 |
| **Conn** | 一条连接的生命周期与能力（窄接口） | 不认调用；不解析协议字节 |
| **Carrier** | 一次交换的承载面 | 不拥有连接（只能 `Abort` 本次） |
| **Framing** | 握手、复用模型、消息边界、状态落点 | 不建连；不做序列化 |
| **Session** | 某 `Conn` 上的协议实例：握手 + 切分调用 | 不路由业务方法；不生产调用 ctx |
| **Call** | 一次调用的分帧实例 | 只搬字节；不关闭 Conn |
| **Codec** | 字节 ↔ 消息 | 不做 I/O |
| **Stream** | `Call` × `Codec` 的解码流 | unary = 单元素流 |
| **Filter / OpenFilter** | 服务端包调用 / 客户端包「打开调用」 | 只见 ctx、method、Stream |
| **CallMetadata** | 方向化 headers/trailers 权威状态 | 不编码线格式 |
| **Service / Server / Client / Resolver** | 具名服务、宿主、调用方、寻址 | Server 只做两级查表；Client 不做解析 |

要点：

- **没有 `Protocol` 登记表**——组合就是 `argos.BindingFunc`（返回全新三元组的工厂）。
- **Compressor 不是核心概念**——仅 gRPC 路径使用（`framing/grpc`、`binding/grpc`）。
- **复用不是第四轴**——`Framing.Reuse()` 声明承载力；借还由客户端会话池执行。

```go
type ReuseModel uint8

const (
    OneCallPerConn ReuseModel = iota // 如 envelope×udp
    Sequential                       // 如 envelope×tcp/ws
    Concurrent                       // 如 grpc×http2
)
```

### 三种 context

| ctx | 父 | 覆盖范围 |
|---|---|---|
| 连接 ctx | 组合层持有 | 整条 Conn/Session；**无 deadline** |
| 握手子 ctx | 连接 ctx | 仅 `New*Session` 一次；挂 `HandshakeTimeout` |
| 调用 ctx | 连接 ctx（服务端）或调用方传入（客户端） | 单次调用；可有 deadline |

`HandshakeTimeout` 不得挂在连接 ctx 上，否则复用连接会在建立后约 10s 集体死亡。

---

## 3. 分层与依赖

组合只发生在 `client` / `server`（及 `stream`）。依赖是 DAG，由 `invariants_test.go` 强制。

| 包 | 允许依赖的本仓库包 | 说明 |
|---|---|---|
| `descriptor` / `status` / `metadata` / `codec` | —— | `status` 不得依赖 protobuf/genproto |
| `budget` | `status` | |
| `compressor` | —— | **仅** `framing/grpc`、`binding/grpc` 可依赖 |
| `transport` | —— | 不 import `descriptor` / `framing` |
| `transport/{tcp,ws,udp,http1,http2}` | `transport`、`status` | |
| `framing` | `transport`、`descriptor`、`metadata`、`budget`、`status` | 不 import `compressor` |
| `framing/envelope` | 同 `framing` | |
| `framing/grpc` | 同 `framing` + `compressor` + `internal/httpstatus` + genproto | 唯一可 import genproto |
| `framing/wholebody` | 同 `framing` + `internal/httpstatus` | |
| `stream` / `filter` / `resolver` | 见表意 | |
| `argos`（根） | `transport`、`framing`、`codec`、`filter` | Config / Binding；不 import `compressor` |
| `binding/grpc` | `argos`、http2、grpc framing、codec、compressor | TLS / 压缩 Option |
| `binding/envelope` | `argos`、tcp/ws/udp、envelope framing、codec | `NewTCP` / `NewWS` / `NewUDP` |
| `binding/wholebody` | `argos`、http1、wholebody framing、codec | 默认 JSON |
| `client` / `server` | 除 `internal/*` 外上述；client 另加 resolver、sessionpool | 唯一组合层 |

关键不变量（摘要）：

1. 全仓一个通用路由器（`server`：Service → Method）。
2. 不用 gRPC 的程序不得传递依赖 `framing/grpc` / `binding/grpc` / `compressor` / genproto。
3. 复用策略只在 `internal/sessionpool`（仅 `client` import）。
4. 一条连接一个 `AcceptCall` 循环；组合层不为 `example/*` 特判。

---

## 4. 组合与用法

### 4.1 三轴

- **Transport**：怎么连、Conn/Carrier 有哪些 I/O 能力  
- **Framing**：握手、一条连接几个调用、边界与状态写在哪  
- **Codec**：消息 ↔ 字节  

```go
type Binding struct {
    Transport transport.Transport
    Framing   framing.Framing
    Codec     codec.Codec
}

type BindingFunc func() (Binding, error) // 每次返回全新、互不共享的三元组
```

便利工厂：

```go
grpcbinding.New(opts...)           // http2 × grpc；WithTLS / WithCompressor
envelopebinding.NewTCP() / NewWS() / NewUDP()
wholebodybinding.New()             // http1 × wholebody，默认 JSON
```

### 4.2 客户端 / 服务端装配

```
client.New(cfg, serviceName)
  → 工厂建一次三元组 + 按 Reuse() 建空池
Open(ctx, Method)
  → 准入 → CallMetadata → OpenFilter
      → Resolver → 池借 Session（未命中则 Dial + NewClientSession）
      → OpenCall → stream.Wrap
调用结束 → Call.Close → 池按 Reusable() / 引用计数归还或关闭

server: Transport.Serve → Conn → NewServerSession
  → loop AcceptCall → 准入 → 两级路由 → Accept → Filter → handler
  → Finish → Close；EOF 退出后 Session.Close
```

会话池对每个会话维护**在途调用引用计数**；`Sequential`/`OneCallPerConn` 承载力为 1，`Concurrent` 无上限（忙则 `ErrSessionBusy`）。空闲会话受 `MaxIdleSessions` / `SessionIdleTimeout` / `MaxSessionLifetime` 约束。

### 4.3 最小用法

```go
cfg, err := argos.New(
    argos.WithService("echo.v1.EchoService",
        argos.ServiceBinding(grpcbinding.New()),
        argos.ServiceTarget("ip://127.0.0.1:7001"),
        argos.ServiceListenAddress(":7001")),
)
if err != nil { ... }

srv := server.New(cfg)
_ = srv.AddBinding(grpcbinding.New(), ":7001")
// RegisterEchoService(srv, impl) 由生成桩提供
go srv.Serve(ctx)

cli, err := client.New(cfg, "echo.v1.EchoService")
ec := echov1.NewEchoServiceClient(cli)
resp, err := ec.Echo(ctx, &echov1.EchoRequest{Msg: "hi"})
```

多传输示例见 `example/echo`（grpc / envelope×tcp|ws|udp / wholebody×http1）。收门资产：`example/resp`、`example/synth`。

自定义组合：实现 `BindingFunc`，与 `binding/grpc.New` 同型——不必改 `client`/`server`。

### 4.4 调用收尾（用法约定）

- 客户端读完业务消息后须读到**终态**（STATUS / trailers）再 `Close`，未读终态的会话不得回池。
- `HalfClose` 结束发送，不关闭连接；`Call.Close` 结束本次调用。
- 跨交换的连接级状态（事务等）建模为**一次长期双向流**，内核不提供会话亲和 API。

---

## 5. 配置

配置是显式不可变的 `*argos.Config`，无进程级槽位。

```go
cfg, err := argos.New(
    argos.WithFilter(auth),
    argos.WithOpenFilter(clientAuth),
    argos.WithMaxMessageSize(4<<20),
    argos.WithService("echo.v1.EchoService",
        argos.ServiceBinding(grpcbinding.New()),
        argos.ServiceTarget("ip://127.0.0.1:7001")),
)
```

- `argos.New` 一次构造并校验；可用 `cfg.With(opts...)` 派生新值。
- 解析优先级：`client.New` / `AddBinding` Option > 服务级 > Config 顶层 > 内置默认。
- **端不匹配的 Option 静默忽略**（同一份 Config 可同时喂给 Client 与 Server）。
- TLS / 压缩在 `binding/grpc.New` 的 Option 里，不在根包。

### 默认值

| 选项 | 默认 | 说明 |
|---|---|---|
| `MaxFrameSize` / `MaxMessageSize` | 4 MiB | 线上帧 / 未压缩消息 |
| `MaxMetadataSize` | 256 KiB | |
| `MaxHeaderBytes` | 1 MiB | HTTP 头块 |
| `ReadAheadMessages` | 1 | 接收方向预读条数 |
| `MaxConcurrentCalls` | 64 | 在途调用 |
| `MaxBufferedBytes` | 1 GiB | 缓冲池；与 perCall 交叉校验 |
| `OpenTimeout` | 10s | 服务端：首字节 → OPEN/headers 完成 |
| `HandshakeTimeout` | 10s | 连接级握手（握手子 ctx） |
| `MaxDrainBytes` | 1 MiB | 服务端丢弃残余帧上限 |
| `ConnReadBufferSize` | 64 KiB | 连接级读缓冲 |
| `MaxSessionsPerEndpoint` | 64 | 客户端会话数 |
| `MaxIdleSessions` | 8 | 空闲会话保留数 |
| `SessionIdleTimeout` | 50s | 客户端空闲回收 |
| `MaxSessionLifetime` | 30m | 客户端会话寿命 |
| `MaxInboundConns` | 1024 | 服务端入站连接 |
| `MaxInboundConnIdle` | 50s | 服务端空闲（必填正数） |
| `MaxInboundConnAge` | 30m | 服务端寿命（必填正数） |

默认下 `perCall ≈ 16 MiB`，`64 × 16 MiB = MaxBufferedBytes`。

---

## 6. 错误与状态

`status.Code` 采用 gRPC 的 17 个数值（0–16）。`status` 包不依赖 protobuf。

```go
status.Error(code, msg)
status.CodeOf(err)           // nil → OK
status.WithDetails(err, ...) // 中性 Detail{TypeURL, Value}；仅 framing/grpc 翻译为 Any
```

本地哨兵（均可 `errors.Is`）：

| 哨兵 | 含义 |
|---|---|
| `status.ErrCardinality` | 响应条数与形态不符 |
| `status.ErrSessionsExhausted` | 会话池耗尽 |
| `status.ErrCallsExhausted` | 调用准入耗尽 |

连接级错误（不属于任何调用）经 `WithConnErrorObserver` 上报，且不使 `Transport.Serve` 返回。

---

## 7. 代码生成

```bash
go run ./cmd/argos generate stub --from proto --proto-path . example/echo/echo.proto
# 一致性：make test-generate（stub --check vs example/echo）
```

- 描述符字段不导出，经 `MustMethod` / `MustService` 构造。
- 标识带服务前缀：`EchoService_Echo`、`EchoServiceDesc`。
- 服务端：`RegisterEchoService(srv, impl)`——不生成 `switch method`。
- 客户端：具名 Client 复用底层 `client.Client`；单次 RPC 只关 CallStream。

---

## 8. 工具链

```bash
make test            # go test ./...
make test-race
make lint
make accept          # 根包 Invariant|Accept|Section9
make test-generate
make test-integration
make test-deps       # 传递依赖门禁
make verify          # 上列全部
```

本机若 `go` 与 `GOROOT` 不一致（gvm）：`export GOROOT=/data/root/.gvm/1.27.1/go` 且 `PATH` 含 `$GOROOT/bin`。

---

## 附录：Carrier 能力（查阅）

| Conn / Carrier | 典型组合 | 要点 |
|---|---|---|
| `ByteStreamCarrier` | tcp / ws + envelope | 长度前缀帧；Sequential 空闲守望读 |
| `StreamConn` | http2 + grpc | `OpenStream` 在响应 headers 前返回 |
| `MessageCarrier` / HTTP | http1 + wholebody | 整 body 一次提交；仅 unary |
| `DatagramCarrier` | udp + envelope | 一请求一响应数据报；`OneCallPerConn` |

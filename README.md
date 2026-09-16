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

- 配置文件 / 热加载 / 插件生态（配置只走代码：`argos.DefaultConfig()` 是进程级默认对象，但没有文件格式、没有 reload）
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

- **三轴可命名注册**——`DefaultConfig().RegisterTransport/Framing/Codec(name, factory)` 供文本配置解析；`ServiceTransportName` 等与直接写工厂二选一。`binding/*` 在 `init` 里注册常用名（`tcp` / `http2` / `envelope` / `grpc` / `protobuf` 等）。每个 Client / 监听面仍各 `Assemble()` 一次，得到互不共享的三元组。
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
| `argos`（根） | `transport`、`framing`、`codec`、`filter` | Config / Protocol / ServiceConfig；不 import `compressor` |
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
type Protocol struct {
    Transport TransportFunc
    Framing   FramingFunc
    Codec     CodecFunc
    TransportName, FramingName, CodecName string // 查 Config 上的注册表
}
// ResolveProtocol 把名字解析成工厂，再 Assemble()；工厂不得 Dial/Serve
```

客户端与服务端共用 `Config.Services[name]`：三轴（Transport / Framing / Codec）、客户端 `Target`、服务端 `ServiceListenAddress` 或 `ServiceListener`（同一服务多传输）。

便利预设（返回 `argos.Protocol`）：

```go
grpcbinding.New(opts...)           // http2 × grpc；WithTLS / WithCompressor
envelopebinding.NewTCP() / NewWS() / NewUDP()
wholebodybinding.New()             // http1 × wholebody，默认 JSON
grpcbinding.Service(opts...)       // 写入 WithService 的 ServiceProtocol
```

### 4.2 客户端 / 服务端装配

```
client.New(argos.WithServiceName(name), ...)
  → 解析 Config（WithConfig 指定的对象或进程默认）
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
// 服务端：协议与监听写在 WithService；代码只 Register impl
srv := server.New(
    argos.WithService("echo.v1.EchoService",
        grpcbinding.Service(),
        argos.ServiceListenAddress(":7001"),
    ),
)
// RegisterEchoService(srv, impl) 由生成桩提供
go srv.Run(ctx)

// 客户端：生成桩内置了 service name，自己持有 Client
ec, err := echov1.NewEchoServiceClient(argos.WithTarget("ip://127.0.0.1:7001"))
if err != nil { ... }
defer ec.Close()
resp, err := ec.Echo(ctx, &echov1.EchoRequest{Msg: "hi"})
```

不经生成桩、直接用组合层（自定义 Framing、或只要 `Open` 的裸调用）：

```go
cli, err := client.New(
    argos.WithServiceName("echo.v1.EchoService"),
    argos.WithProtocol(grpcbinding.New()),
    argos.WithTarget("ip://127.0.0.1:7001"),
)
defer cli.Close()
st, err := cli.Open(ctx, echov1.EchoService_Echo)
```

多传输示例见 `example/echo`（grpc / envelope×tcp|ws|udp / wholebody×http1）。收门资产：`example/resp`、`example/synth`。

自定义组合：填 `argos.Protocol` 三个工厂，与 `binding/grpc.New` 同型——不必改 `client`/`server`。

### 4.4 调用收尾（用法约定）

- 客户端读完业务消息后须读到**终态**（STATUS / trailers）再 `Close`，未读终态的会话不得回池。
- `Client.Close` 是契约，不是可选项：它持有会话、Transport 和会话池的回收 goroutine。被丢弃而未 `Close` 的 Client 由 GC 兜底释放并经 `WithConnErrorObserver` 上报一次，但那是安全网，不是释放时机——本项目没有进程级连接池，连接的归属和寿命是显式的。
- `HalfClose` 结束发送，不关闭连接；`Call.Close` 结束本次调用。
- 跨交换的连接级状态（事务等）建模为**一次长期双向流**，内核不提供会话亲和 API。

---

## 5. 配置

`argos.Config` 是**普通结构体**：字段写你关心的，其余留零值——零值即内置默认。进程默认对象是 `argos.DefaultConfig()`，`client.New` / `server.New` 不指定配置时就从它出发。

```go
// 进程级：启动时改一次，之后建的 Client/Server 都从这里出发
argos.DefaultConfig().MaxMessageSize = 8 << 20
argos.DefaultConfig().ConnErrorObserver = logConnError

// 某个 Client/Server 要另一份配置：字面量只写关心的字段
cfg := &argos.Config{MaxMessageSize: 1 << 20, Filters: []filter.Filter{auth}}
srv := server.New(argos.WithConfig(cfg), argos.WithListenAddress(":7001"))

// 单个旋钮的临时覆盖仍走 Option
cli, err := client.New(
    argos.WithServiceName("echo.v1.EchoService"),
    argos.WithTarget("ip://127.0.0.1:7001"),
    argos.WithOpenFilter(clientAuth),
    argos.WithMaxMessageSize(4<<20),
)
```

- **构造时快照并校验**：`client.New` / `server.New` 各自 clone 一份，之后改原对象不影响已建实例；改进程默认对象必须在建任何实例之前（否则是 data race）。
- **零值 = 默认**；要显式关掉可选限额用 `argos.Disabled`（仅 `MaxIdleSessions` / `SessionIdleTimeout` / `MaxSessionLifetime` 接受，其余字段给 `Disabled` 直接报错）。
- **端不匹配的 Option 编译期拒绝**：`argos.Option` 两端通用（含 `WithService`），`argos.ClientOption` 只进 `client.New`（`WithServiceName` / `WithTarget` / `WithProtocol` / `WithOpenFilter` / 会话池四项），`argos.ServerOption` 只进 `server.New`（`WithListenAddress` / `WithFilter` / 入站连接三项 / HTTP 两项）。同一份 `*Config` 仍可同时喂给两端。
- 客户端服务选择的优先级：调用侧 `WithProtocol`（或单轴覆盖）/ `WithTarget` > `Services[name]`；服务名只来自 `WithServiceName`，不随 `WithConfig` 从别的 Client 继承。
- `server.New` 不返回 error：被拒的 Option 组合由 `Run` 报出；`Run` 为每个已 Register 的服务在 `Services` 里启动对应监听。
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
| `MaxInboundConnIdle` | 50s | 服务端空闲（无禁用值） |
| `MaxInboundConnAge` | 30m | 服务端寿命（无禁用值） |
| `HTTPReadHeaderTimeout` | 10s | 服务端：HTTP 头块 / upgrade（无禁用值） |
| `HTTPIdleTimeout` | 50s | 服务端：keep-alive 空闲（无禁用值） |

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
- 客户端只有一个工厂：`NewEchoServiceClient(opts...)` 内置 service name 并自持 `client.Client`，`Close` 关它；显式 `WithServiceName` 可覆盖内置名。
- 单次 RPC 只关 CallStream，不关 Client。
- RPC 不得取名 `Close`：会与客户端接口的 `Close() error` 撞名，生成器直接报错。

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

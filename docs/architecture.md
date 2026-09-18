# 架构与目标

Argos 是 **C/S 协议组合运行时**：对外选型为 **Transport × Codec**；**Transport 轴**（`transport.Transport`）内含字节管道、握手、分帧与会话池，产出 **Conn → Session → Call**，再由 **Codec** 编解码载荷。`client` / `server` 提供统一的 Dial、池化、准入、路由与 Filter。**RPC（含 gRPC）是重要且完整验证的子集**，不是唯一适用形态；命令式、连接级握手、无 metadata 通道的协议同属设计区（见 `example/resp`、`example/synth`）。

## 协议覆盖范围

**核心设计区**——组合层（会话池、`Open` / `AcceptCall`、两级路由、Filter）价值最大：

- 连接可复用，工作单元是 **一次或一段交换**（unary、client/server/bidi 流）。
- 交换在 API 上可对应 **method / 命令 / HTTP :path** 等（由 Transport 轴内分帧映射到 `descriptor.Method` 或 synthetic method）。
- 典型：**gRPC**、**httpunary**、**类 Redis/Memcached 命令往返**（`example/resp`）。

**条件覆盖**——映射存在且分层仍诚实，但状态与 wire 不完全等于「一次 `Open` = 一次 Call」：

- **连接级状态**（事务、SUBSCRIBE 独占、prepared stmt）：落在 `Session`（`Reusable()`、exclusive）或 **长期 Call**；组合层 **无** DB Session 式亲和 API（见 [usage.md](usage.md) 调用收尾）。
- **同连接 pipeline（无多路复用标号）**：须在 **轴内 Session 队列**，不应暴露为多个并发 Call 抢同一 Sequential `Carrier`。
- **以无消息边界的字节管道为主的 payload**（大 HTTP body、COPY/LOAD）：`Pipe` 侧的 `ByteStreamCarrier` 可流式 I/O；**现成 `Call` 以 `[]byte` 消息为主**，httpunary/grpc 内置路径不主张为第一公民；需扩展轴内分帧语义或 Session 状态机。

**非目标**（与「能不能映射」无关，是产品边界）：见下节。

## 目标

1. **流式和一问一答用同一套 API**——unary 当作单元素流，内核不另开 unary 专用路径。
2. **协议 = Transport × Codec**（分帧在 Transport 实现内，不是第三选配维）；不兼容组合在建连或开承载后明确失败，不静默降级。
3. **连接是一等事实**：`Transport` → `Conn` → `Session` → `Call`；握手、复用、连接级状态有明确落点。
4. **可组装性可测**：内置与 `example/*` 中的组合只靠 **Transport 名 × Codec 名** 装配，`client` / `server` 不为具体协议特判。已验证组合见 [compatibility-matrix.md](compatibility-matrix.md)。
5. **原生 gRPC**：与 grpc-go 在 h2c / TLS·ALPN 下互通（形态、状态码、metadata、详情、deadline、压缩）。
6. **服务名是一等概念**：服务端按名登记，客户端按名打开调用。

## 非目标

- 配置文件 / 热加载 / 插件生态（配置只走代码：`argos.DefaultOptions()`，无文件格式、无 reload）
- 通用非 gRPC 的 HTTP/2 协议栈；http1 流式；udp 上的可靠传输或多路复用
- 完整 RESP / 数据库协议（`example/resp` 仅为收门资产）
- 不承诺与仓库历史旧线的线格式或状态码数值兼容

## 概念模型

对外 **Transport × Codec**。`transport.Transport` 轴内再分 **Pipe（字节面）** 与 **Session（分帧面）**：

| 轴内子层 | 连接级 | 调用级 |
|---|---|---|
| **Pipe** | `Conn` | `Carrier` |
| **Session / 分帧** | `Session` | `Call` |

| 概念 | 职责 | 不做什么 |
|---|---|---|
| **Transport**（`transport.Transport`） | 产品面：拨号/监听、握手、分帧、池化、`OpenCall` / `Serve` | 不做载荷序列化（Codec）；组合层不拆成第三工厂 |
| **Conn** | 一条连接的生命周期与能力（窄接口） | 不认调用；不解析协议字节 |
| **Carrier** | 一次交换的承载面 | 不拥有连接（只能 `Abort` 本次） |
| **Session** | 某 `Conn` 上的协议实例：握手、复用、切分调用（实现面为 `session.Framing` + `*Session`） | 不路由业务方法；不生产调用 ctx |
| **Call** | 一次交换的分帧实例（RPC 调用、Redis 命令、HTTP unary 等均映射于此） | 只搬字节；不关闭 Conn |
| **Codec** | 字节 ↔ 消息 | 不做 I/O |
| **Stream** | `Call` × `Codec` 的解码流 | unary = 单元素流 |
| **Filter / OpenFilter** | 服务端包调用 / 客户端包「打开调用」 | 只见 ctx、method、Stream |
| **CallMetadata** | 方向化 headers/trailers 权威状态 | 不编码线格式 |
| **Service / Server / Client / Resolver** | 具名服务、宿主、调用方、寻址 | Server 只做两级查表；Client 不做解析 |

要点：

- **Transport × Codec**——`ServiceOptions` 存已注册的 transport / codec **名称**（`codec.Register` / `transport.Register`）；`ServiceTransport` + `ServiceCodec` 与 client `WithTransport` + `WithCodec` 引用这些名称，装配时 `New(name)` 实例化。
- **Compressor 不是核心概念**——仅 gRPC 路径（`grpc`）。
- **复用不是第三选配维**——轴内 `session.Framing.Reuse()` 声明承载力；借还由轴上的客户端会话池执行。

```go
type ReuseModel uint8

const (
    OneCallPerConn ReuseModel = iota // 如自定义 × udp
    Sequential                       // 如 example/resp×tcp
    Concurrent                       // 如 grpc×http2
)
```

### 三种 context

| ctx | 父 | 覆盖范围 |
|---|---|---|
| 连接 ctx | 组合层持有 | 整条 Conn/Session；**无 deadline** |
| 握手子 ctx | 连接 ctx | 仅 `New*Session` 一次；挂 `HandshakeTimeout` |
| 调用 ctx | 连接 ctx（服务端）或调用方传入（客户端） | 单次调用；可有 deadline |

`HandshakeTimeout` 不得挂在连接 ctx 上，否则复用连接会在建立后约 10s 集体失效。

## 分层与依赖

组合只发生在 `client` / `server`（及 `stream`）。依赖是 DAG，由 `invariants_test.go` 强制。

| 包 | 允许依赖的本仓库包 | 说明 |
|---|---|---|
| `descriptor` / `status` / `metadata` / `codec` | —— | `status` 不得依赖 protobuf/genproto |
| `budget` | `status` | |
| `compressor` | —— | **仅** `grpc` 可依赖 |
| `transport` | `descriptor`、`metadata`、`budget` | 接口签名需要；**不** import `codec` |
| `transport/{tcp,ws,udp,http1,http2}` | `transport`、`status` | 字节管道：不得 import `descriptor` / `metadata` / `budget` / `codec` / `internal/session` |
| `transport/grpc` | `transport` + `transport/http2` + `descriptor` / `metadata` / `budget` / `status` + `internal/session` + `internal/sessionpool` + `internal/transportbind` + `internal/httpstatus` + `compressor` + genproto | 唯一可 import genproto |
| `transport/httpunary` | `transport` + `transport/http1` + `descriptor` / `metadata` / `budget` / `status` + `internal/session` + `internal/sessionpool` + `internal/transportbind` + `internal/httpstatus` | 不得 import `compressor` / genproto |
| `stream` / `filter` / `resolver` | 见表意 | |
| `argos`（根） | `transport`、`codec`、`filter` | Options / ServiceOptions；不 import 具体 transport 实现 |
| `client` / `server` | 除 `internal/*` 外上述；client 另加 resolver、sessionpool | 唯一组合层 |

关键不变量（摘要）：

1. 全仓一个通用路由器（`server`：Service → Method）。
2. 不用 gRPC 的程序不得传递依赖 `grpc` / `compressor` / genproto。
3. 复用/池只在 axis 内或 `internal/sessionpool`（允许名单在 `invariants_test.go`：完整线栈 + example + client + `internal/fake`、`internal/transportbind`），`server` 永不池化。
4. 一条连接一个 `AcceptCall` 循环；组合层不为 `example/*` 特判。
5. `transport/` 下每个包都必须显式分类（字节管道 or 完整线栈），分类表在 `invariants_test.go` 的 `classifiedTransportPkgs`。

扩展时的依赖摘要亦见 [overview.md](overview.md#依赖方向摘要)。

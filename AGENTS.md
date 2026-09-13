# AGENTS.md · argos

给在本仓库工作的编码代理的约定。Go 惯例对齐 [Effective Go](https://go.dev/doc/effective_go) 与 [Code Review Comments](https://go.dev/wiki/CodeReviewComments)。

---

## 项目是什么

**argos** 验证「IDL × Transport × Codec → 协议」组合模型：一份 `.proto`、一份业务 impl，可同时以 gRPC（http2）、REST（http1）、WebSocket、TCP/UDP 信封、Telnet 调试口对外。

| | |
|---|---|
| 定位 | **验证模型**，不是生产 RPC 框架 |
| 入口 | 根包 `With*` + 子包：`server`、`client`、`stream`、`filter`、`errs`、`metadata` 等 |
| 传输 | `transport/{http2,http1,ws,tcp,udp,telnet}/` |
| 真源 | 代码 + 测试 + 本文件；计划/设计文档不入库（勿建 `docs/`） |

---

## 目录与依赖

```
argos.go option.go       # 根包：WithTransport/WithCodec 等共用配置
errs/ metadata/          # 运行时基础类型
filter/ stream/          # 调用链与消息层
server/ client/          # 服务端绑定与客户端 Open
selector/                # 客户端 target 寻址：scheme 选实现，body 解析为 dial 地址（可含服务发现）
codec/                   # Codec 接口；codec/{protobuf,json} 实现
transport/               # Transport/Framer 接口；transport/{http2,...} 实现
internal/wire/           # 自有族二进制信封（tcp/ws/udp）
internal/statusmap/      # §8 HTTP/grpc-status 映射（http1/http2）
internal/codegen/        # 工具链：ir → gen ← frontend；stub 编排 generate
internal/cmd/            # CLI 子命令（urfave/cli v3），按功能分子包
  generate/              # argos generate stub
  frontend/              # argos frontend list
cmd/argos/               # 仅 main，调用 internal/cmd.App()
example/echo/            # 示例与六传输集成测试
argos_test.go            # 根包 loopback 集成测（测公开 API）
```

**分层是否合理**

| 层 | 结论 |
|---|---|
| 根包 `With*` | ✅ 配置在根 `argos`；`server`/`client` import 根包取 `Option`/`Config` |
| `errs`/`metadata` 独立 | ✅ 传输实现不必 import 根包 |
| `server`/`client` 分设 | ✅ 职责清晰；共用配置在根包 |
| `stream` 与 `transport.Framer` 并存 | ✅ 字节层 vs 消息层，见 stream 包注释 |
| `internal/codegen` 与 `internal/cmd` 分离 | ✅ 生成逻辑可测、CLI 只做 flag 接线 |
| `codec/codec.go` 仅接口 | ✅ 与 `transport/transport.go` 对称 |
| 暂不动 | `argos_test.go` 留根目录（测 server/client/With* 回路）；`example/echo` 兼示例与集成测 |

**依赖方向（硬约束）**

- 生成代码与业务：按需 import 根 `argos`（With*）与子包（常见 `server`、`client`、`stream`、`errs`）；业务 impl **不得** import `transport/*`、`codec/*`
- `transport/*`：import `transport`、`errs`、`metadata`；自有族加 `internal/wire`；http1/http2 加 `internal/statusmap`
- 根 `argos` **不得** import 任何 `transport/*`

传输实现内本地 struct 命名为 **`channel`**，避免与 `import ".../transport"` 冲突。构造函数签名：`func New() transport.Transport`（无地址；listen/dial 由 ServerOption/ClientOption 传入）。

**Server listen / Client dial**

- **Server**：`WithListenAddress` + `WithTransport(http2.New())`。loopback 测试可共享同一 Transport 实例 + `WithListenAddress`。`Run` 时 `ListenAndServe(..., ServerOptions...)`。
- **Client**：`WithTarget` + `WithTransport(http2.New())`；Open 时 `selector.Parse` → `transport.WithDialAddress` → `Open(..., ClientOptions...)`。
- **Transport / Codec 注册表**（`transport` / `codec` 包，根 `argos` 不导出）：各子包 `init` 自动 `Register` 内置名（`http2`、`protobuf` 等）；`Get(name)` 取 factory；`WithTransport("http2")` / `WithCodec("protobuf")` 按名解析。插件可额外 `Register`。**无配置文件**——listen 与组合在代码里写。
- **Selector**（`selector` 包，根 `argos` 不导出）：Target 格式 `scheme://service-identifier`（必填 scheme）；可插拔寻址/服务发现。内置 **ip** 在 `selector/ip` 注册。

---

## 用 argos 开发服务（Agent 指南）

本节面向**在本仓库外或本仓库内新建业务服务**的 AI 代理：如何从零搭一个可被多种传输调用的服务。完整可运行样板见 `example/echo/`。

### 核心约束（先读）

| 规则 | 说明 |
|------|------|
| 业务 / 生成桩 | 按需 import 根 `argos` + 子包（如 `server`、`client`、`stream`、`filter`、`errs`、`metadata`） |
| 业务 impl | **不得** import `transport/*`、`codec/*`（grep 不到 http/grpc/ws 等） |
| 一个 Service | 恰好绑定 **一个 Transport + 一个 Codec** |
| 多协议对外 | 同一份 `impl` 注册到 **多个** `Service`（各配不同 Transport/Codec/端口） |
| 派发 | 只在 `*.argos.go` 的 `Register*` 里 method switch；**不要**在 impl 或 transport 里写派发 |
| 错误 | 返回 `errs.Error(code, msg)`；不用 `google.golang.org/grpc/status` |
| Codec | **必填**，无默认；与传输独立选配 |

### 推荐目录（用户项目或 `example/<svc>/`）

```
mysvc/
  mysvc.proto          # IDL
  mysvc.pb.go          # 消息类型（与 .proto 同包；示例已入库，见下）
  mysvc.argos.go       # argos generate stub（Register* + Client 桩）
  impl.go              # 业务实现：仅 pb 类型；Filter 等按需加 filter/stream/metadata/errs
  auth.go              # 可选：Filter
  main.go              # //go:build ignore 或 cmd/ 下：选 transport、起 Server
```

### 开发流程

**1. 写 `.proto`，用 argos CLI 一次生成 message + 桩**

内置 **protocompile** 解析 `.proto`，**不需要** 系统安装 `protoc`：

```bash
go run github.com/argos-io/argos/cmd/argos generate stub \
  --from proto --proto-path . mysvc.proto
```

一次产出：

| 文件 | 内容 |
|------|------|
| `mysvc.pb.go` | `proto.Message` 类型（proto 源） |
| `mysvc.msg.go` | 非 proto 插件 IDL 的 message 默认名 |
| `mysvc.argos.go` | `Register*` / Client 桩 |

金样 `example/echo/echo.{pb,argos}.go`；`make test-generate` 对 `--check` 路径自动校验同 base 的 message + stub（normalize diff）。

**其他 IDL**：exec 插件 `{cmd} emit-ir -- files`  stdout 吐 **IR v2 JSON**（含 `messages` + `services`）；`argos generate stub --plugin {cmd}`。插件缺 messages 硬失败；手写金样仍可用 `--check` fallback。

**2. 实现 `impl.go`**

- 实现 `XxxServiceServer`；方法签名用 pb 类型。
- Unary：`func (s *impl) Foo(ctx, *Req) (*Resp, error)`
- 服务端流：第三个参数为生成接口 `XxxService_BarServer`，调 `Send(msg)`
- **impl 里不出现**传输名、协议名、listen 地址。

**3. 启动 Server**

`main.go` **可以** import `transport/*`、`codec/*`（业务 impl 不行）。多端口 = 多个 `NewService`，同一份 impl：

```go
import (
    "github.com/argos-io/argos"
    protobufcodec "github.com/argos-io/argos/codec/protobuf"
    "github.com/argos-io/argos/server"
    "github.com/argos-io/argos/transport/http2"
)

srv := server.New()
impl := mysvc.NewXxxImpl()
for _, opts := range [][]argos.Option{{
    argos.WithTransport(http2.New()),
    argos.WithListenAddress(":9090"),
    argos.WithCodec(protobufcodec.New()),
}} {
    svc := srv.NewService(opts...)
    mysvc.RegisterXxxService(svc, impl)
}
srv.Run(ctx)
```

按名构造（可选）：import 子包后内置名已注册，可直接 `WithTransport("http2")` / `WithCodec("protobuf")`；常规写法仍用 `WithTransport(http2.New())`。

**4. 客户端**

生成桩提供 `NewXxxServiceClient(opts ...argos.Option)`。

**Loopback 测试**（同进程、共享 Transport 实例）：

```go
tr := http2.New()
client := mysvc.NewXxxServiceClient(
    argos.WithTransport(tr),
    argos.WithCodec(protobufcodec.New()),
)
```

**生产式拨号**（独立地址）：

```go
import _ "github.com/argos-io/argos/selector/ip" // 注册 ip scheme

client := mysvc.NewXxxServiceClient(
    argos.WithTarget("ip://127.0.0.1:9090"),
    argos.WithTransport(http2.New()),
    argos.WithCodec(protobufcodec.New()),
)
```

### 多传输暴露同一服务

同一 `impl` 注册多次即可；每次 `NewService` 不同 Transport + Codec + 端口：

| 对外形态 | Transport | Codec | 典型 listen |
|----------|-----------|-------|-------------|
| gRPC（grpcurl） | `http2` | `protobuf` | `:9090` |
| REST/JSON | `http1` | `json` | `:8080` |
| 自有信封 | `tcp` / `ws` / `udp` | `protobuf` | `:7000` 等 |
| 调试口 | `telnet` | `json` | `:2323` |

框架里没有 `Protocol` 类型；上表是**组合结果**，不是代码里的枚举。

### Filter 与 Metadata

```go
import (
    "github.com/argos-io/argos/errs"
    "github.com/argos-io/argos/filter"
    "github.com/argos-io/argos/metadata"
    "github.com/argos-io/argos/stream"
)

func Auth(ctx context.Context, method string, st stream.Stream, next filter.Handler) error {
    if metadata.FromContext(ctx)["authorization"] == nil {
        return errs.Error(errs.Unauthenticated, "missing token")
    }
    return next(ctx, method, st)
}
```

- Filter 包在 `WithFilter` 链上；**短路时不调用 `next`** → 不会 `CloseSend`。
- 客户端 Filter 可在 `next` 前写入 metadata（见 `example/echo/auth.go`）。
- 状态码在 `onCall` 返回后由传输写回；业务只返回 `errs.Error(...)`。

### 流式 RPC

- 服务端流：impl 收 `WatchServer`，循环 `Send`；结束 return `nil` 或 error。
- 客户端流：生成 Client 通常用 goroutine + channel 包装 `Open`（见 `echo.argos.go` 的 `Watch`）。
- 支持流式的传输：`http2`、`tcp`、`ws`（`http1`/`udp`/`telnet` 为 unary 语义）。

### Agent 常见误操作

| 误操作 | 正确做法 |
|--------|----------|
| impl import `transport/http2` | 只在 `main.go` 选传输 |
| 一个 Service 绑多种传输 | 多个 `NewService`，共享同一 `impl` |
| 手写 method 路由在 impl | 只改 `*.argos.go` 生成物或重新 generate |
| 忘记 `WithCodec` | `Run` / `Open` 返回 error |
| Client 用 `WithTarget` 但未 blank import `selector/ip` | 加 `_ "github.com/argos-io/argos/selector/ip"` |
| 用 gRPC status / codes | 用 `errs.Error` + `errs.CodeOf` |
| 新建 `docs/` 或设计 md 入库 | 真源：代码 + 测试 + 本文件 |

### 改完服务后怎么验

```bash
make test-generate    # 桩与生成器一致
go test ./...         # 含 example 集成测
make verify           # 提交前全量（含协议验收）
```

用户项目若无 `example/echo` 全套脚本，至少为改动路径补 `_test.go`；在本仓库改框架时跑 `make verify`。

---

## Go 编码约定

### 包与命名

- 一个目录一个包；包名简短、小写、无下划线（`http2` 不是 `http_2`）
- 导出 API 必须有文档注释；未导出符号仅在必要时注释
- 错误用 `errs.Error(code, msg)` / `errs.CodeOf`；**不**忽略 `error` 返回值
- `NewServer` / `NewClient` / `NewService` 不返回 `error`；缺 codec/transport、`WithTransport`/`WithCodec` 类型错误等组装问题在 `Run` / `Open`（或 `Resolve*`）返回 `error`

### 接口与实现

- `Transport` / `Framer` / `Codec` / `Filter` 定义在各自包；按需直接 import
- 派发**只允许**在 `server/binding.invoke`；传输实现里不得有第二份 method switch
- Filter 短路时不调用 `next` → 不 `CloseSend`；status 由传输在 `onCall` 返回后写
- **Client.Open 不调用 CloseSend**；半关闭由 transport 在 client 侧 `Open` 实现里处理；Server 在 `dispatchEnd` 里 CloseSend

### 测试

见下节「测试体系」。新增行为必须带测试；修 bug 先写失败测试再修。

### 工具链

```bash
make test              # 全量
make test-unit         # 内核小包
make test-integration  # test-protocol + transport + example/echo
make test-protocol     # 外部客户端：grpcurl/curl/python3 脚本 × 六传输
make test-race         # -race
make lint              # go vet + staticcheck（若已安装）
make accept            # 验收脚本（静态检查 + go test）
make test-generate     # argos generate stub --check
make build-argos       # 构建 cmd/argos
make verify            # make all + make test-integration（提交前推荐）
```

- **Lint**：以 `go vet` + `staticcheck` 为准（与 Go 1.27 工具链一致）。`golangci-lint` 需版本支持 `go.mod` 中的 Go 版本，否则 typecheck 会误报 embedded field。
- **提交前**：`make verify` 通过（含协议验收 `test-protocol`）；Agent 流程见 `.agents/skills/argos-test-fix/`

---

## 测试体系

| 层级 | 范围 | 命令 | 位置示例 |
|---|---|---|---|
| **单元** | 无网络、单包逻辑 | `make test-unit` | `filter/`, `stream/`, `errs/`, `internal/wire/` |
| **集成** | 传输 + echo 端到端 | `make test-integration` | `transport/*_test.go`, `example/echo/*_test.go` |
| **协议验收** | 外部客户端 ↔ 服务端 | `make test-protocol` | `example/echo/protocol_accept_test.go`, `scripts/accept-*.sh` |
| **内核** | Server/Client/invoke 回路 | `go test .` | `argos_test.go`（loopback 假 Transport） |
| **生成物** | 生成 = 手写 | `make test-generate` | `internal/codegen/gen/` |
| **验收** | 仓库级不变量 | `make accept` | `scripts/accept-all.sh` |

集成测试约定：

- 监听地址用 `127.0.0.1:0`；客户端与服务端**共用同一** `Transport` 实例（port 0 自拨号）
- 协议验收：`make test-protocol`（`ARGOS_PROTOCOL_ACCEPT=1`，缺 grpcurl/curl/python3 则 fail）；`ACCEPT_EXTERNAL=1 make accept` 对固定端口手起 `example/echo/main.go` 时跑全部 `scripts/accept-*.sh`
- 表驱动优先；子测试用 `t.Run`

**不要**把 `transport/` 以外的包 import 进业务 `example/echo/impl.go`（验收 #4）。

---

## 代码生成（Path B）

```
IDL ──frontend──▶ internal/codegen/ir ──gen──▶ *.argos.go
```

| 前端 | 用法 |
|---|---|
| `proto`（内置） | `argos generate stub --from proto --proto-path <dir> file.proto` → `*.pb.go` + `*.argos.go` |
| `ir`（内置） | `argos generate stub --from ir file.ir.json`（手写 IR fallback，可仅 services） |
| Exec 插件 | `argos generate stub --plugin <cmd> -- files...`（`emit-ir` → IR v2 JSON，默认 `*.msg.go` + stub） |

生成逻辑**只在** `internal/codegen`（`gen/message`、`gen/stubgen`）；业务只跑 `argos generate stub`。

---

## 不要提交

- 设计/计划/决策记录（放仓库外，勿建 `docs/`）
- 构建产物（`bin/argos` 等）
- 仅为本地 review 写的临时脚本

设计变更在本地文档维护；代码与测试是仓库真源。

---

## 常见改动检查单

1. 改了导出类型/方法 → 跑 `make test-generate`（`internal/codegen/gen` 与 `echo.argos.go`）
2. 改了 §8 码表 → 只改 `internal/statusmap`，http1/http2 引用它
3. 改了 `Framer` 契约 → 六个 `transport/*` 与 `argos_test` loopback 一起跑
4. 新增传输 → `New() transport.Transport` + 集成测试 +（若适用）`scripts/accept-*.sh`

---

## 术语

- **Transport**：完整通道（监听 + 拨号）
- **Framer**：单次调用的消息边界
- **Codec**：`proto.Message` ↔ 字节
- **Filter**：包裹型拦截器，服务端与客户端同型
- **Selector**：target 的 scheme 插件；将 service-identifier 解析为 dial 地址（内置 ip 为直连，外部实现可做服务发现）
- **协议**：组合结果（如 gRPC = http2 + protobuf），**不是**框架里的类型

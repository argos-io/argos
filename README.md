# argos

验证「**IDL × Transport × Codec → 协议**」：同一份 `.proto`、同一份业务实现，可同时对外提供 gRPC、REST、WebSocket、TCP/UDP 信封 RPC 与 Telnet 调试口——业务代码里不出现任何传输或协议名。

> 当前目标是**证明组合模型成立**，不是生产级 RPC 框架。

## 模型

```
              ┌─ IDL ──────── protobuf（消息类型）
组合出一个协议 ─┼─ Transport ── 网络通道（边界、路由、status）
              └─ Codec ────── 消息 ↔ 字节
```

框架里没有 `Protocol` 类型。例如 **gRPC** 是 `transport/http2` + `codec/protobuf` 按 gRPC 线缆约定实现后自然出现的结果，`grpcurl` 能调通即为验收。

## 目录

| 路径 | 说明 |
|---|---|
| `argos.go` | 公共 API 门面（生成代码 import 此包） |
| `server/` `client/` | 服务注册与客户端 `Open` |
| `transport/` | http2、http1、ws、tcp、udp、telnet |
| `codec/` | protobuf、protojson |
| `example/echo/` | Echo 示例与六传输集成测试 |
| `cmd/argos/` | CLI 入口（`main` 仅几行） |
| `internal/cmd/` | CLI 子命令：`generate stub`、`frontend list` |
| `internal/codegen/` | IR、生成器、IDL 前端（proto / ir / 插件） |
| `.agents/skills/argos-test-fix/` | Agent skill：跑测 → 定位 → 修复 |

## 快速开始

**要求**：Go 1.27+（`make verify` 仅此即可）。跑协议验收时另需 `grpcurl` / `curl` / `python3`（CI 会装）。

```bash
git clone https://github.com/argos-io/argos.git
cd argos
make verify    # 提交前推荐：lint + 全量测试 + 协议/传输集成
```

### 跑 Echo 六端口服务

```bash
go run example/echo/main.go
```

| 传输 | 地址 | 客户端示例 |
|---|---|---|
| gRPC (h2c) | `:9090` | 见下方 grpcurl |
| HTTP/JSON | `:8080` | `scripts/accept-curl.sh 127.0.0.1:8080 hi` |
| WebSocket | `:8081` | `scripts/accept-ws.sh 127.0.0.1:8081 hi` |
| TCP | `:7000` | `scripts/accept-envelope.sh 127.0.0.1:7000 hi` |
| UDP | `:7001` | `scripts/accept-udp.sh 127.0.0.1:7001 hi` |
| Telnet | `:2323` | `scripts/accept-telnet.sh 127.0.0.1:2323 hi`（带 `ServerAuth` filter） |

gRPC 示例（需 `-import-path`）：

```bash
grpcurl -plaintext \
  -proto example/echo/echo.proto \
  -import-path example/echo \
  -d '{"msg":"hi"}' \
  localhost:9090 echo.v1.EchoService/Echo
```

流式 Watch（http2 / tcp / ws）：

```bash
scripts/accept-grpcurl-watch.sh 127.0.0.1:9090 watch
scripts/accept-envelope-watch.sh 127.0.0.1:7000 watch
scripts/accept-ws-watch.sh 127.0.0.1:8081 watch
```

### 使用者代码（示意）

```go
// 服务端：一个 Service 绑定一个 Transport（实例或注册名 "http2"）
svc := server.NewService(
    argos.WithTransport(http2.New()), // 或 argos.WithTransport("http2")
    argos.WithListenAddress(":9090"),
    argos.WithCodec(protobufcodec.New()), // 或 argos.WithCodec("protobuf")
)
echov1.RegisterEchoService(svc, impl)

// 客户端
c := echov1.NewEchoServiceClient(
    argos.WithTarget("ip://127.0.0.1:9090"),
    argos.WithTransport(http2.New()),
    argos.WithCodec(protobufcodec.New()),
)
resp, err := c.Echo(ctx, &echov1.EchoRequest{Msg: "hi"})
```

业务 `impl` 只实现 `EchoServiceServer`，不 import 任何 `transport/*`。

## 代码生成

IDL → Go 走 **`argos generate stub` 一条路径**：内置 **protocompile** 解析 `.proto`，**无需安装 `protoc`**。一次产出 `*.pb.go`（message）+ `*.argos.go`（Register / Client 桩）。

```bash
# 从 .proto 生成 echo.pb.go + echo.argos.go
go run ./cmd/argos generate stub \
  --from proto --proto-path . example/echo/echo.proto

# 校验金样（CI / make test-generate；自动检查同 base 的 pb + argos）
go run ./cmd/argos generate stub --check example/echo/echo.argos.go \
  --from proto --proto-path . example/echo/echo.proto

# 手写 IR fallback（可仅 services，message 手写）
go run ./cmd/argos generate stub --from ir service.ir.json

# 外部 IDL 插件（emit-ir → IR v2 JSON，默认 *.msg.go + stub）
go run ./cmd/argos generate stub --plugin ./argos-idl-foo -- input.thrift
```

```bash
make build-argos     # 构建 bin/argos
make test-generate   # --check 与手写 diff 为空
```

## 测试

```bash
make verify            # 提交前推荐（= make all + make test-integration）
make all               # lint + test + race + test-generate + accept
make test-integration  # test-protocol + transport + example/echo
make test-protocol     # 外部客户端协议验收（grpcurl/curl/python3 脚本）
make test-unit         # 内核小包
make test-race         # 竞态
make lint              # go vet + staticcheck
make accept            # 仓库验收（impl 零痕迹、唯一 dispatch 路径等）
```

| 层级 | 说明 |
|---|---|
| 单元 | `filter`、`stream`、`errs`、`internal/wire` 等 |
| 集成 | 各 `transport/*_test.go`、`example/echo/*_test.go` |
| 协议验收 | `example/echo/protocol_accept_test.go` + `scripts/accept-*.sh`（unary + Watch 流式） |
| 验收 | `scripts/accept-all.sh`（#4 业务零传输痕迹、#6 派发仅在 `binding.invoke`） |

### `test-protocol` vs `ACCEPT_EXTERNAL`

| | `make test-protocol` | `ACCEPT_EXTERNAL=1 make accept` |
|---|---|---|
| 何时跑 | `make test-integration` / `make verify` 自动跑 | 手动，需先起服务 |
| 端口 | 动态 `127.0.0.1:0`（测试内起服） | 固定端口（`example/echo/main.go` 默认） |
| 工具 | grpcurl、curl、python3 **必须**（缺则 fail） | 同上 |
| 覆盖 | unary Echo × 六传输 + Watch × http2/tcp/ws | 同上 + Watch 流式 smoke（http2/tcp/ws） |

```bash
# 对手动六端口服务的 smoke（非 CI 默认）
go run example/echo/main.go   # 另开终端
ACCEPT_EXTERNAL=1 make accept
```

Agent 修复流程见 [`.agents/skills/argos-test-fix/SKILL.md`](.agents/skills/argos-test-fix/SKILL.md)（`/argos-test-fix`）。

## 内置传输一览

| Transport | 方法路由 | 流式 | 典型组合 |
|---|---|---|---|
| `http2` | `:path` | ✅ | gRPC + protobuf |
| `http1` | `POST /{service}/{method}` | ❌ unary | REST + json |
| `ws` | 信封字段 | ✅ | 自有 RPC + protobuf |
| `tcp` / `udp` | 信封字段 | tcp ✅ / udp ❌ | 自有 RPC + protobuf |
| `telnet` | 首行方法名 | ❌ | 调试 + json |

## 开发

- 代理/贡献者约定见 [AGENTS.md](AGENTS.md)

## License

[MIT](LICENSE)

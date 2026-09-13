# argos

**一份 IDL、一份业务实现，六种对外形态。**

argos 是一个 Go 实验项目，用来验证一种 RPC 组合模型：**协议不是框架里的枚举，而是 IDL、Transport、Codec 三者相乘的结果。**

同一份 `echo.proto`、同一个 `impl.go`，可以同时对外提供 gRPC、REST/JSON、WebSocket、TCP/UDP 信封 RPC 和 Telnet 调试口。业务代码里不出现 `http`、`grpc`、`ws` 等传输名——选什么组合、监听哪个端口，只在启动服务的 `main` 里决定。

> **定位**：证明「组合模型」能跑通，不是生产级 RPC 框架。真源是代码、测试与 [AGENTS.md](AGENTS.md)。

---

## 为什么做 argos

常见做法是「选框架 = 选协议」：gRPC 一套生成物，REST 再写一套，自有二进制协议又是一套。换对外形态往往意味着换栈或大量适配层。

argos 反过来问：**如果协议只是三个正交维度的组合，会怎样？**

| 维度 | 职责 | 例子 |
|------|------|------|
| **IDL** | 服务与方法签名、消息类型 | protobuf（`.proto`） |
| **Transport** | 网络通道：连接、路由、状态码写回 | `http2`、`http1`、`ws`、`tcp`、`udp`、`telnet` |
| **Codec** | 消息与字节的互转 | `protobuf`、`json` |

三者独立选配。例如 **gRPC** 并不是代码里的类型，而是 `http2` + `protobuf` 按 gRPC 线缆约定实现之后，**自然出现**的行为——`grpcurl` 能调通，就是验收。

```
         ┌─ IDL ─────────── 方法名、Request/Response 类型
协议  =  ┼─ Transport ──── 怎么连、怎么路由、怎么收尾
         └─ Codec ─────────  body 怎么编解码
```

---

## 一张图看懂

```
  echo.proto          impl.go              main.go（你选组合）
      │                  │                      │
      ▼                  ▼                      ▼
  消息 + 桩代码      纯业务逻辑          Transport + Codec + 端口
      │                  │                      │
      └────────── RegisterEchoService ──────────┘
                              │
          ┌───────────────────┼───────────────────┐
          ▼                   ▼                   ▼
      :9090 gRPC          :8080 REST          :7000 TCP 信封
     (http2+protobuf)    (http1+json)       (tcp+protobuf)
          ...              ws / udp / telnet ...
```

**一份 impl，注册多次 `NewService`**，每次绑定不同的 Transport + Codec + 端口即可。

---

## 30 秒体验

**环境**：Go 1.27+

```bash
git clone https://github.com/argos-io/argos.git
cd argos
make verify          # 全量测试 + 六协议外部客户端验收
go run example/echo/main.go   # 六端口 Echo 服务
```

起服务后，可用常见工具直接调用：

| 对外形态 | 端口 | 怎么试 |
|----------|------|--------|
| gRPC (h2c) | `:9090` | `grpcurl -plaintext -proto example/echo/echo.proto -import-path example/echo -d '{"msg":"hi"}' localhost:9090 echo.v1.EchoService/Echo` |
| REST/JSON | `:8080` | `scripts/accept-curl.sh 127.0.0.1:8080 hi` |
| WebSocket | `:8081` | `scripts/accept-ws.sh 127.0.0.1:8081 hi` |
| TCP 信封 | `:7000` | `scripts/accept-envelope.sh 127.0.0.1:7000 hi` |
| UDP | `:7001` | `scripts/accept-udp.sh 127.0.0.1:7001 hi` |
| Telnet | `:2323` | `scripts/accept-telnet.sh 127.0.0.1:2323 hi` |

完整示例与集成测试在 [`example/echo/`](example/echo/)。

---

## 写服务时长什么样

**业务 impl**——只依赖 protobuf 类型，不出现传输名：

```go
func (s *echoImpl) Echo(ctx context.Context, req *EchoRequest) (*EchoResponse, error) {
    return &EchoResponse{Msg: "hello " + req.GetMsg()}, nil
}
```

**启动**——在 `main` 里选 Transport 和 Codec（可多端口、同一份 impl）：

```go
import (
    protobufcodec "github.com/argos-io/argos/codec/protobuf"
    "github.com/argos-io/argos/option"
    "github.com/argos-io/argos/server"
    "github.com/argos-io/argos/transport/http2"
)

srv := server.New()
impl := echov1.NewEchoImpl()

svc := srv.NewService(
    option.WithTransport(http2.New()),
    option.WithListenAddress(":9090"),
    option.WithCodec(protobufcodec.New()),
)
echov1.RegisterEchoService(svc, impl)

srv.Run(ctx)
```

**客户端**——生成桩提供 `NewEchoServiceClient`；地址用 `WithTarget`（内置 `ip://` scheme）：

```go
client := echov1.NewEchoServiceClient(
    option.WithTarget("ip://127.0.0.1:9090"),
    option.WithTransport(http2.New()),
    option.WithCodec(protobufcodec.New()),
)
resp, _ := client.Echo(ctx, &echov1.EchoRequest{Msg: "hi"})
```

从 `.proto` 生成 message 与桩（内置 protocompile，**无需系统安装 protoc**）：

```bash
go run ./cmd/argos generate stub --from proto --proto-path . example/echo/echo.proto
```

---

## 内置组合一览

| Transport | 典型对外形态 | 流式 | 常见 Codec |
|-----------|-------------|------|------------|
| `http2` | gRPC（h2c） | ✅ | protobuf |
| `http1` | REST | unary | json |
| `ws` | WebSocket 信封 | ✅ | protobuf |
| `tcp` / `udp` | 自有二进制信封 | tcp ✅ / udp unary | protobuf |
| `telnet` | 行协议调试口 | unary | json |

Filter（鉴权、日志等）在 Transport 之上、业务之下，服务端与客户端共用同一套类型。

---

## 仓库里有什么

| 区域 | 说明 |
|------|------|
| [`server/`](server/) [`client/`](client/) [`option/`](option/) 等 | 公开 API 在各子包；根 [`argos.go`](argos.go) 仅模块入口注释 |
| [`example/echo/`](example/echo/) | 可运行的六传输示例 + 协议验收测试 |
| [`transport/`](transport/) | 六种传输实现 |
| [`codec/`](codec/) | protobuf、json 编解码 |
| [`cmd/argos/`](cmd/argos/) | CLI：`generate stub`、`frontend list` |
| [`internal/codegen/`](internal/codegen/) | IR、生成器、proto 前端 |

架构约束、开发流程、测试分层见 **[AGENTS.md](AGENTS.md)**。提交前推荐 `make verify`。

---

## License

[MIT](LICENSE)

# AGENTS.md · Argos

给在本仓库干活的编码代理用的约定。人读整体设计看根目录 `README.md`；**行为以代码 + 测试 + 本文件为准**。Go 风格对齐 [Effective Go](https://go.dev/doc/effective_go) 与 [Code Review Comments](https://go.dev/wiki/CodeReviewComments)。

---

## 项目是什么

**Argos** 用 **Transport × Codec** 拼 **C/S 协议**（`transport.Transport` 实例 = 传输 + 分帧 + 连接/池）。gRPC/proto 是一等路径，**非 RPC**（如 `example/resp` RESP2×tcp）同属验证范围。

| | |
|---|---|
| 定位 | **验证「C/S 协议能这么分层拼」**，不是拿来直接上生产的通用框架 |
| 阶段 | 里程碑 ⓪–⑦ 已完成（tag `milestone-7`） |
| 文档 | `README.md` 产品概述；设计与用法见 `docs/`（见下「协议接入」） |

---

## 目录

```
descriptor/  status/  metadata/
transport/  transport_impl.go  transport/{tcp,ws,udp,http1,http2,grpc,httpunary}/
codec/  codec/{protobuf,json}/
compressor/  compressor/{gzip,grpccodec}/
stream/  filter/
resolver/  resolver/ip/
client/  server/
internal/session/  internal/sessionpool/  internal/transportbind/  internal/httpstatus/  internal/fake/
internal/codegen/  internal/codegen/{ir,frontend,gen,stub,check}/
cmd/argos/
example/{echo,resp,synth}/
argos 根包（Options / ClientOption / ServerOption / ServiceOptions）
docs/          # 协议接入（Transport 契约与检查单）
Makefile · .github/workflows/ci.yml · invariants_test.go
```

依赖方向硬约束见 [docs/architecture.md](docs/architecture.md)；由 `invariants_test.go` 强制。

---

## 协议接入（AI 必读）

新协议或新组合 **Transport × Codec** 时，先读 [`docs/README.md`](docs/README.md)，再按层深入：

| 任务 | 文档 | 源码真源 |
|------|------|----------|
| **Transport 契约**（OpenCall / Serve / CallConcurrency / CodecName / 生命周期） | [`docs/transport.md`](docs/transport.md) | `transport/transport_impl.go` |
| 新 `Pipe`（字节面）或新线栈 axis 实现 | [`docs/transport.md`](docs/transport.md) | `transport/{tcp,ws,udp,http1,http2}`, `transport/grpc/link.go`, `example/resp/link.go` |
| Call / AcceptCall 分帧语义（轴内） | [`docs/session.md`](docs/session.md) | `internal/session`, `transport/grpc/framing.go` |
| Codec + 挂接 | [`docs/codec-and-wiring.md`](docs/codec-and-wiring.md) | `service.go`, `example/echo/main.go` |
| 选型已有组合 | [`docs/compatibility-matrix.md`](docs/compatibility-matrix.md) | 各 `transport/*/link.go` 与 example |

**硬性约定（摘要）**

1. **按名注册**：`codec.Register` / `transport.Register`；`ServiceOptions` 与 `WithTransport` / `WithCodec` 引用注册名，装配时 `New(name)` 实例化。
2. **axis 生命周期归构造方**：`transport.Transport` 无 `Close`/`Shutdown`——生命周期就是构造它的 ctx，`Serve` 随该 ctx 结束；要显式释放的实现自带 `Close`（如 `transport/grpc`）。组合层对共享实例**既不 `Close` 也不改配置**：limits / 池 / codec 名只在构造期用 `WithLimits` / `WithPool` / `WithCodecName` 定死——那些数字只有构造处一个来源，`argos.Options` 不带它们，没有第二份可对照的拷贝；装配期只核对 codec 名（`internal/transportbind.CheckCodecName`），不一致即报错；谁构造谁关闭。组合层自身亦无可释放资源：`client.Client` 无 `Close`；`server.Server` 靠 `Run(ctx)` 的 ctx 停止，`Shutdown(ctx)` 只是取消该 ctx 并等 `Run` 返回，不碰 transport，也不打断在途连接。**停止 = 不再接受新调用**，不等于在途调用已结束：`Run` 只等监听面的 `Serve` 返回，处理器跑在 transport 自己的协程上，server 从不 join 它们；要等连接排空，由轴的所有者调轴自己的 `Shutdown(ctx)`。
3. **ServerConn.Handshake**：握手超时与错误上报在 server 组合层；Transport 内只做协议 I/O。
4. **`AcceptCall`**：`ErrCallRejected` 须返回可 `Finish` 的 `ServerCall`；连接级错误才结束 accept 循环。
5. **transport 根包** 可 import `descriptor` / `metadata`（接口签名需要），**不** import `codec`；完整线栈实现（`transport/grpc`、`transport/httpunary`）可 import `internal/session`、`internal/sessionpool`（池在 axis 内），字节管道（`tcp`/`ws`/`udp`/`http1`/`http2`）不得 import `descriptor` / `metadata`。
6. 行为变更配测试；提交前 `make verify`。

`docs/` 只放**接入与契约**说明，不写计划草稿或未决决策（仍直接改 README + 代码）。

---

## 工具链

```bash
make test              # go test ./...
make test-race         # go test -race ./...
make lint              # go vet + 有则 staticcheck
make accept            # 根包 Invariant|Accept|Section9
make test-generate     # stub --check vs example/echo
make test-integration  # example/echo multi-transport
make test-deps         # 传递依赖门禁（Transitive*）
make verify            # 上列全部
```

提交前：`make verify` 全绿。

本机若 `go` 与 `GOROOT` 版本不一致（gvm），先 `export GOROOT=/data/root/.gvm/1.27.1/go` 且 `PATH` 含 `$GOROOT/bin`。

---

## 工作约定

- **先理解再动手**：改前读文件；最小改动；不建多余文档
- **TDD**：行为变更配测试；公开接口改动先停并说清
- **并行 Subagent**：改动彼此独立时可并行；同一文件不拆给两个代理；契约与验收留在主代理；回收后重跑声称通过的测试再 `make verify`
- **不提交**：计划/决策草稿、构建产物、仅本地 review 的临时脚本；设计变更改 `README.md` / `docs/` 并随代码入库

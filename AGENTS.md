# AGENTS.md · Argos

给在本仓库干活的编码代理用的约定。人读整体设计看根目录 `README.md`；**行为以代码 + 测试 + 本文件为准**。Go 风格对齐 [Effective Go](https://go.dev/doc/effective_go) 与 [Code Review Comments](https://go.dev/wiki/CodeReviewComments)。

---

## 项目是什么

**Argos** 用 Transport × Framing × Codec 三轴拼 **C/S 协议**；连接（`Conn`/`Session`）在 API 里是一等公民。gRPC/proto 是一等路径，**非 RPC**（如 `example/resp` RESP2×tcp）同属验证范围。

| | |
|---|---|
| 定位 | **验证「C/S 协议能这么分层拼」**，不是拿来直接上生产的通用框架 |
| 阶段 | 里程碑 ⓪–⑦ 已完成（tag `milestone-7`） |
| 文档 | `README.md` 产品概述；设计与用法见 `docs/`（见下「协议接入」） |

---

## 目录

```
descriptor/  status/  metadata/  budget/
transport/  transport/{tcp,ws,udp,http1,http2}/
framing/  framing/{grpc,wholebody}/
codec/  codec/{protobuf,json}/
compressor/  compressor/{gzip,grpccodec}/
stream/  filter/
resolver/  resolver/ip/
client/  server/
internal/fake/  internal/sessionpool/  internal/httpstatus/
internal/codegen/  internal/codegen/{ir,frontend,gen,stub,check}/
cmd/argos/
example/{echo,resp,synth}/
argos 根包（Config/Option/ServiceConfig）
docs/          # 协议接入（Transport/Framing/Codec 抽象与检查单）
Makefile · .github/workflows/ci.yml · invariants_test.go
```

依赖方向硬约束见 [docs/architecture.md](docs/architecture.md)；由 `invariants_test.go` 强制。

---

## 协议接入（AI 必读）

扩展或新组合 **Transport × Framing × Codec** 时，先读 [`docs/README.md`](docs/README.md)，再按轴深入：

| 任务 | 文档 | 源码真源 |
|------|------|----------|
| 新传输 / 新 Conn·Carrier | [`docs/transport.md`](docs/transport.md) | `transport/transport.go` |
| 新分帧 / Session·Call | [`docs/framing.md`](docs/framing.md) | `framing/framing.go` |
| Codec + 三轴工厂挂接 | [`docs/codec-and-wiring.md`](docs/codec-and-wiring.md) | `service.go`, `example/echo/axes.go` |
| 选型已有组合 | [`docs/compatibility-matrix.md`](docs/compatibility-matrix.md) | 各 `transport/*`, `framing/*` |

**硬性约定（摘要）**

1. **无全局 registry**：三轴只用 `ServiceTransport` / `ServiceFraming` / `ServiceCodec` 工厂；勿恢复按名解析。
2. **只有 `Transport.Close()`**：Framing/Codec 工厂无生命周期；资源在 Session/Call/Conn。
3. **Transport** 不 import framing；**Framing** 不 import codec（grpc 除外可 import compressor/genproto）。
4. **Framing** 在 `New*Session` 对 `Conn` 做窄接口 assert；不匹配即 error。
5. **`AcceptCall`**：`ErrCallRejected` 须返回可 `Finish` 的 `ServerCall`；连接级错误才结束 accept 循环。
6. **发送失败** 用 `transport.SendError`；`ReceiveOpen` 语义见 `transport/transport.go`。
7. 行为变更配测试；提交前 `make verify`。

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

# AGENTS.md · argos v2

给在本仓库工作的编码代理的约定。整体设计与用法见根目录 `README.md`；实现真源是代码 + 测试 + 本文件。Go 惯例对齐 [Effective Go](https://go.dev/doc/effective_go) 与 [Code Review Comments](https://go.dev/wiki/CodeReviewComments)。

---

## 项目是什么

**argos v2** 重写运行时内核：Transport × Framing × Codec 三轴组装协议；连接（`Conn`/`Session`）是一等事实。分支 `v2`。

| | |
|---|---|
| 定位 | **验证可组装模型**，不是生产 RPC 框架 |
| 阶段 | 里程碑 ⓪–⑦ 已完成（tag `milestone-7`） |
| 文档 | `README.md` 整体设计与用法；勿另建 `docs/` 堆计划/决策记录 |

---

## 目录

```
descriptor/  status/  metadata/  budget/
transport/  transport/{tcp,ws,udp,http1,http2}/
framing/  framing/{envelope,grpc,wholebody}/
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
Makefile · .github/workflows/ci.yml · invariants_test.go
```

依赖方向硬约束见 `README.md` §3；由 `invariants_test.go` 强制。

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
- **不提交**：计划/决策草稿、构建产物、仅本地 review 的临时脚本；设计变更直接改 `README.md` 并随代码入库

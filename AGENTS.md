# AGENTS.md · argos v2

给在本仓库工作的编码代理的约定。设计真源是工作副本根目录的 `README.md`（本地排除、不入库）；仓库真源是代码 + 测试 + 本文件。Go 惯例对齐 [Effective Go](https://go.dev/doc/effective_go) 与 [Code Review Comments](https://go.dev/wiki/CodeReviewComments)。

---

## 项目是什么

**argos v2** 重写运行时内核：Transport × Framing × Codec 三轴组装协议；连接（`Conn`/`Session`）是一等事实。当前分支 `v2`，按 `README.md` §13 里程碑落地。

| | |
|---|---|
| 定位 | **验证可组装模型**，不是生产 RPC 框架 |
| 阶段 | ⓪–⑦✅ 全部完成（`milestone-7`）；分支 `v2` |
| 探针 | 已删除（① Task 1.17）；价值转入 `internal/fake` 与正式测试 |
| 真源 | 代码 + 测试 + 本文件；设计/计划/决策记录不入库（勿建 `docs/`） |

---

## 落地执行

规格与步骤级计划在 `README.md` §13。每个任务以可测交付物结束，走 TDD，**一个任务一次提交**。当前：**里程碑 ⓪–⑦ 全部完成**（最新 tag `milestone-7`）。

**⑥**（已完成）：`make verify` = lint + test + test-race + accept + test-generate + test-integration + test-deps；目录表已对齐；**Task 6.3** 全量 §9 核对见根包 `section9_test.go`（`TestSection9Checklist` / `TestSection9VerifyGate`）。

**⑦**（已完成）：`example/resp`（含 SUBSCRIBE）、`example/synth`、零核心接口改动证据、组合层源码扫描、连接抖动负载记录（§6.1 ⚠️ 默认值仍为占位，见 `example/resp/LOAD.md`）。

后续可选：用 7.5 数据回写 §6.1 确认默认值；补全 §9 未覆盖细格（TLS×全形态×17 码等；见 `section9_test.go` soft-gap 注释）。

### 并行 Subagent 规范

改动彼此独立时，**主代理编排、子代理按任务并行**；契约与验收留在主代理。这是本仓库的默认执行方式，不是可选项。

| 规则 | 做法 |
|---|---|
| **先读依赖图** | 并行切分以 `README.md` §13.5 为准，不得臆造依赖 |
| **一任务一代理** | 每个 Task 派一个 fresh subagent；同一文件不得分给两个代理 |
| **冻结契约** | 分发前写死接口签名、错误哨兵、验收断言；子代理不得自行改 §2.1 公开接口——红了就停并上报 |
| **文件所有权** | 子代理只改任务清单里的路径；共享辅助（如 `probe/carrier.go`）由主代理先改完再放行依赖方 |
| **验证范围** | 子代理只跑自己相关的 `go test`（如 `-run TestXxx`）；并行期间兄弟任务可能是红的 |
| **提交** | 子代理可按任务完成 `git commit`（消息用计划里写死的那句）；冲突时主代理串行化提交 |
| **主代理验收** | 全部回收后重跑声称通过的测试，再 `make verify`；抽查 diff 是否弱化断言 |
| **报告** | 必须含：结论（PASS/FAIL/需改设计）、改了哪些文件、实测数据（写入 `probe/FINDINGS.md` 的片段） |

**① 的推荐并行切分**（§13.5）：

```
1.1 descriptor ─┐
1.2 status     ─┼─▶ 1.3 metadata ∥ 1.4 budget ∥ 1.5 transport ∥ 1.9 resolver
                │         │
                └─────────┴─▶ 1.6 framing ─▶ 1.7 stream/codec ─▶ 1.8 filter
                                              │
                         1.5+1.6 ─▶ 1.10 fake ─▶ 1.10b pool ─▶ 1.10c
                                              │
                         1.6+1.8 ─▶ 1.11 ─▶ 1.11b/1.12/1.12b
                                              │
                         汇合 ─▶ 1.13…1.15 ─▶ 1.16 ─▶ 1.17 删 probe/
```

- **可并行**：1.1∥1.2；1.2 完成后 1.3∥1.4∥1.5∥1.9；其后按依赖图。
- **关键路径**：1.1→1.5→1.6→1.7→1.10b→1.10c→1.13→…→1.15。
- **不适合并行**：公开接口定稿本身；跨包共享的假 Transport/Framing 签名未冻前。

**⓪**（已完成）：0.1∥0.9∥0.13 → 0.4–0.8∥0.14 → 0.16 / tag `milestone-0`。

### 探针任务清单（子代理 prompt 模板）

派发时附上：

1. 仓库根路径与当前分支 `v2`
2. `README.md` 中该 Task 的完整步骤（含代码块与验收命令）
3. 文件所有权（Create/Modify/不得触碰）
4. 成功标准：指定 `go test … -race` 命令 + commit message
5. 若断言失败且无法在探针层修通 → **停下来报「需改设计」**，不要绕过

---

## 目录（v2 ⑥）

```
descriptor/ status/ metadata/ budget/
transport/ transport/{tcp,ws,udp,http1,http2}/
framing/ framing/{envelope,grpc,wholebody}/
codec/ codec/{protobuf,json}/
compressor/ compressor/{gzip,grpccodec}/
stream/ filter/
resolver/ resolver/ip/
client/ server/
binding/ binding/{envelope,grpc,wholebody}/
internal/fake/ internal/sessionpool/ internal/httpstatus/
internal/codegen/ internal/codegen/{ir,frontend,gen,stub,check}/
cmd/argos/
example/echo/   # example/{resp,synth} → 里程碑 ⑦
argos 根包（Config/Option/Binding）
Makefile · .github/workflows/ci.yml · invariants_test.go
```

依赖方向硬约束见 `README.md` §3.1；由 `invariants_test.go` 强制。

---

## 工具链（⓪ → ⑥）

```bash
make test              # go test ./...
make test-race         # go test -race ./...
make lint              # go vet + 有则 staticcheck
make accept            # 根包 Invariant|Accept|Section9（§3 / §3.1 / §9）
make test-generate     # stub --check vs example/echo
make test-integration  # example/echo multi-transport
make test-deps         # 传递依赖门禁（Transitive*）
make verify            # §13.1 全集：上列全部
```

提交前：`make verify` 全绿。§6.1 标 ⚠️ 的连接级默认值在任务 7.5 前不得当确认值引用。

本机若 `go` 与 `GOROOT` 版本不一致（gvm），先 `export GOROOT=/data/root/.gvm/1.27.1/go` 且 `PATH` 含 `$GOROOT/bin`。

---

## 不要提交

- 设计/计划/决策记录（含根目录 `README.md` 设计稿；已在 `.git/info/exclude`）
- 构建产物
- 仅为本地 review 写的临时脚本

设计变更在本地 `README.md` 维护；代码与测试是仓库真源。

---

## 常见改动检查单

1. 改了探针共享辅助 → 先合并再放行依赖任务的并行代理
2. 探针红且像接口问题 → 停、写清复现、改本地设计，不硬编码绕过
3. 跨多个独立 Task → 按上文「并行 Subagent 规范」切分
4. 进入 ① 前 → Task 0.16 完成且 `probe/FINDINGS.md` 覆盖全部探针

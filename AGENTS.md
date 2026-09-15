# AGENTS.md · argos v2

给在本仓库工作的编码代理的约定。设计真源是工作副本根目录的 `README.md`（本地排除、不入库）；仓库真源是代码 + 测试 + 本文件。Go 惯例对齐 [Effective Go](https://go.dev/doc/effective_go) 与 [Code Review Comments](https://go.dev/wiki/CodeReviewComments)。

---

## 项目是什么

**argos v2** 重写运行时内核：Transport × Framing × Codec 三轴组装协议；连接（`Conn`/`Session`）是一等事实。当前分支 `v2`，按 `README.md` §13 里程碑落地。

| | |
|---|---|
| 定位 | **验证可组装模型**，不是生产 RPC 框架 |
| 阶段 | 里程碑 ⓪ 探针 → ① 契约冻结 → ②–⑦ 实现与收门 |
| 探针 | `probe/` 一次性；① 结束时整目录删除，**不要演变成实现** |
| 真源 | 代码 + 测试 + 本文件；设计/计划/决策记录不入库（勿建 `docs/`） |

---

## 落地执行（里程碑 ⓪）

规格与步骤级计划在 `README.md` §13。每个任务以可测交付物结束，走 TDD，**一个任务一次提交**（祈使句英文 commit）。

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

**⓪ 的推荐并行切分**（0.0 完成后）：

```
0.0 Makefile+CI          ← 串行前置，主代理或单代理
        │
        ├─ 多路复用链
        │    0.1（重跑骨架）──┬── 0.4 TLS
        │                     ├── 0.5 cardinality
        │                     ├── 0.6 bidi
        │                     ├── 0.7 reuse
        │                     └── 0.8 backpressure
        │    0.9（重跑 OpenFilter，可与 0.1 同时）
        │    0.3/0.10 FINDINGS 补写（只改 FINDINGS.md 段落，可并行）
        │
        └─ 顺序复用链
             0.13 ──▶ 0.14
             
汇合：0.16（两条链全绿后）
```

- **可并行**：0.1∥0.9∥0.13；0.1 完成后 0.4–0.8 彼此并行；FINDINGS 补写不碰 `.go` 时可与探针并行。
- **必须串行**：0.1→依赖骨架的 0.4–0.8；0.13→0.14（复用 `seqconn.go`）；全部探针→0.16。
- **不适合并行**：接口设计本身、跨任务共享文件的机械重命名、0.16 回写规格。

### 探针任务清单（子代理 prompt 模板）

派发时附上：

1. 仓库根路径与当前分支 `v2`
2. `README.md` 中该 Task 的完整步骤（含代码块与验收命令）
3. 文件所有权（Create/Modify/不得触碰）
4. 成功标准：指定 `go test … -race` 命令 + commit message
5. 若断言失败且无法在探针层修通 → **停下来报「需改设计」**，不要绕过

---

## 目录（v2 目标，⓪ 阶段仅有部分）

```
probe/                   # ⓪ 一次性探针（① 删除）
Makefile                 # 0.0：lint / test / test-race / verify
.github/workflows/ci.yml
descriptor/ status/ …    # ① 起陆续出现；见 README §13.2
```

依赖方向硬约束见 `README.md` §3.1；未实现前不要提前造空包。

---

## 工具链（⓪）

```bash
make test       # go test ./...
make test-race  # go test -race ./...
make lint       # go vet + 有则 staticcheck
make verify     # lint + test + test-race（全集要到任务 6.1）
```

提交前：`make verify` 全绿。§6.1 标 ⚠️ 的连接级默认值在任务 7.5 前不得当确认值引用。

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

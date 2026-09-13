---
name: argos-test-fix
description: >-
  Run argos full test tiers (make verify / all + test-integration), diagnose
  every failure, fix code and update tests until all green. Use when fixing CI
  failures, regressions, transport/protocol issues, or when the user asks to run
  tests, fix failing tests, or verify changes before commit.
---

# argos 测试定位修复

在 argos 仓库中：**跑全部分层测试 → 定位每一个失败 → 修代码或更新用例 → 重跑直到全绿**。

真源：`Makefile`、`argos_test.go`、`AGENTS.md`「测试体系」。

## 快速入口

```bash
cd /path/to/argos

make verify           # 提交前终点（= make all + make test-integration）
make all              # lint + test + race + test-generate + accept
make test-integration # transport + example/echo（go test 内 goroutine 起服）
make test-unit        # 内核小包，无 transport 网络
make test-race        # 竞态
make test-generate    # 桩代码与 echo.argos.go 一致
make lint             # go vet + staticcheck（若已安装）
make accept           # 仓库级不变量（argos_test.go TestAccept）
```

**提交前完整验证（skill 默认终点）**：

```bash
make verify
```

等价于 `make all && make test-integration`。

## 测试分层

| 层级 | 命令 | 覆盖 |
|------|------|------|
| 静态 | `make lint` | go vet、staticcheck |
| 单元 | `make test-unit` | errs、metadata、filter、stream、client、server、codec、wire、statusmap、codegen/gen |
| 内核集成 | `go test .` | `argos_test.go` loopback |
| 传输集成 | `make test-integration` | 六个 `transport/*`、`example/echo/*_test.go` |
| 竞态 | `make test-race` | 全仓库 `-race` |
| 生成物 | `make test-generate` | `internal/codegen/gen` ↔ 手写 `echo.argos.go` |
| 验收 | `make accept` | impl 零传输痕迹、唯一 dispatch、`binding.invoke` |

## 工作流（必须按序）

```
1. make verify                    # 或分步 make all → make test-integration
2. 收集全部失败（包、测试名、栈）；有多个失败时逐一记录，不得修一个就停
3. 对每个失败分类（见下表），最小修复 + 必要时补/改用例
4. 从对应失败层定向重跑；该层全绿后，再跑 make verify
5. 重复 2–4 直到 make verify exit 0
6. 汇报：每个失败的根因、修改文件、实际跑过的验证命令
```

**必须遵守**：

- **每一个**失败都要定位根因并修复（或 intentional 变更时更新用例并说明）；不得遗漏、不得只报告不修。
- **不要**在未定位前大范围重构。修 bug 时优先写/改失败测试再改实现。
- 改 `transport/*`、`example/echo/*_test.go` 后，**必须**重跑 `make test-integration`。

## 失败分类与定位

| 现象 | 常见原因 | 去哪看 |
|------|----------|--------|
| `go test` 断言失败 | 行为变更、竞态、端口冲突 | 失败 `_test.go` + 被测包 |
| `go test -race` | 共享 state、goroutine 泄漏 | 栈里第一个项目文件 |
| `go vet` / staticcheck | 未使用符号、错误用法 | 指出的文件行 |
| `test-generate` diff | gen 与 `echo.argos.go` 不一致 | `internal/codegen/gen`、IR、手写桩 |
| `TestAccept/*` 失败 | impl 含传输名、dispatch 泄漏、binding.invoke 缺失 | `argos_test.go`、`example/echo/impl.go`、`server/binding.go` |
| `TestClientEchoSixTransports` 失败 | 某传输 unary 回路 | 对应 `transport/*`、`example/echo/client_test.go` |
| `TestWatchStreaming` 失败 | 流式 http2/tcp/ws | `watch_test.go`、对应 transport |
| transport 集成 flaky | listen 未 ready、连接未关 | `waitListen` 模式、Cleanup |

### 定向重跑

```bash
make test-integration                           # 全 transport/echo
go test -v -run TestClientEchoSixTransports ./example/echo/...
go test -v -run 'TestClientEchoSixTransports/telnet' -count=1 ./example/echo/...
go test -v -run TestAccept ./...
go test -v -run TestName ./path/to/pkg/...
go test -count=1 ./transport/http2/...
go test -race -run TestName ./...
```

## 修完后的用例规则

- **行为 bug**：加回归测试或收紧现有断言；不要删失败断言「糊弄」
- **intentional 行为变更**：改测试期望，并在回复里说明理由
- **生成物变更**：改 `internal/codegen/gen` 后跑 `make test-generate`；若手写桩仍为准则同步 `example/echo/echo.argos.go`
- **新 transport / 协议组合**：补 `transport/*_test.go`、`example/echo/client_test.go` / `watch_test.go` 表驱动用例
- **新导出 API**：子包各自 `*_test.go`；server/client/With* 回路用 `argos_test.go`

## 项目约束（修时不要破）

- 派发只在 `server/binding.invoke`
- `example/echo/impl.go` 不得出现传输/协议名
- 传输实现 struct 名 `channel`；`New() transport.Transport`
- 生成桩按需 import 根 `argos` + 子包（`server`、`client`、`stream`、`errs`）；业务 impl 不得 import `transport/*`、`codec/*`
- Lint 以 `go vet` + `staticcheck` 为准（golangci-lint 可能与 Go 1.27 不兼容）

## 输出格式

修复完成后逐项说明：

1. **失败清单**：每个失败的测试名 + 一句话根因
2. **修改文件**
3. **验证命令**（必须包含实际跑通的全套）：
   - `make verify`

若仍有未覆盖项（如无 staticcheck），明确写出。

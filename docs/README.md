# Argos · 文档

框架做什么、和别的 RPC 栈有何不同，见根目录 [README.md](../README.md)。本目录是**设计与接入**细节；**接口以 Go 源码和测试为准**（`transport/transport.go`、`framing/framing.go`、`codec/codec.go`）。

| 文档 | 读者 | 内容 |
|------|------|------|
| [architecture.md](architecture.md) | 所有人 | 目标、概念模型、包依赖 DAG |
| [usage.md](usage.md) | 使用框架 | 运行时路径、配置与默认值、错误、调用约定、`make verify` |
| [grpc-ecosystem.md](grpc-ecosystem.md) | gRPC 栈 | Health / Reflection / Retry 接入示例 |
| [codegen.md](codegen.md) | 使用 proto | stub 生成、message model |
| [overview.md](overview.md) | 扩展组合 | 三轴挂接摘要、扩展验收 |
| [transport.md](transport.md) | 新传输 | `Transport` / `Conn` / `Carrier` |
| [framing.md](framing.md) | 新分帧 | `Framing` / `Session` / `Call` |
| [codec-and-wiring.md](codec-and-wiring.md) | 编码与装配 | `Codec`、三轴工厂、`ServiceConfig` |
| [compatibility-matrix.md](compatibility-matrix.md) | 选型 | 内置组合与 Carrier 能力 |

编码代理最短检查单：[AGENTS.md](../AGENTS.md)「协议接入」。

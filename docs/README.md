# argos v2 · 扩展与接入文档

本目录说明如何**新增或组合** Transport、Framing、Codec，以及如何把三轴挂到 `client` / `server`。整体产品目标与用法仍以根目录 [README.md](../README.md) 为准；**接口契约以 Go 源码与测试为真源**（尤其 `transport/transport.go`、`framing/framing.go`、`codec/codec.go`）。

| 文档 | 读者 | 内容 |
|------|------|------|
| [overview.md](overview.md) | 人 / AI | 扩展模型、生命周期、依赖方向、验收命令 |
| [transport.md](transport.md) | 实现新传输 | `Transport` / `Conn` / `Carrier` 窄接口 |
| [framing.md](framing.md) | 实现新分帧 | `Framing` / `Session` / `Call` 义务与错误语义 |
| [codec-and-wiring.md](codec-and-wiring.md) | 实现编码与装配 | `Codec`、三轴工厂、`ServiceConfig` |
| [compatibility-matrix.md](compatibility-matrix.md) | 选型 | 内置组合与所需 Carrier 能力对照表 |

给编码代理的**最短检查单**见 [AGENTS.md](../AGENTS.md) 中的「协议接入」一节；详细步骤以上表各文件为准。

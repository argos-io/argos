# 扩展模型概览

## 三轴各自回答什么

| 轴 | 问题 | 典型产出 |
|----|------|----------|
| **Transport** | 怎么建连、一条连接上 I/O 长什么样 | `Conn`；按次交换用 `Carrier` |
| **Framing** | 握手、复用几条调用、帧/状态写在哪 | `Session` → `Call` |
| **Codec** | 业务消息 ↔ 字节 | 纯函数，无 I/O |

协议 = 三轴**合法组合**。组合层（`client` / `server`）不为具体实现写 `switch`；不兼容的组合在 `New*Session` 或 `OpenStream` 等处**明确失败**，不静默降级。

## 连接是一等事实

```
Transport.Dial/Serve → Conn
  → Framing.New*Session → Session
    → OpenCall / AcceptCall → Call
      → stream.Wrap(Call, Codec) → Stream
```

- **只有 `Transport` 带 `Close()`**（三轴工厂本身无生命周期）。`Framing` / `Codec` 不得持有需释放的资源；资源归 `Session` / `Call` / `Conn`。
- 装配中途失败时，组合层只回滚 **Transport**（已创建的 `Framing`/`Codec` 实例无 Close）。

## 如何挂到运行时

1. 在 `ServiceConfig` 上设置三个工厂（或 `JoinService`）：
   - `ServiceTransport` → `argos.TransportFunc`
   - `ServiceFraming` → `argos.FramingFunc`
   - `ServiceCodec` → `argos.CodecFunc`
2. 客户端：`client.New(WithServiceName, WithTarget, JoinClient(WithTransport, WithFraming, WithCodec))`
3. 服务端：`server.New(WithService(..., ServiceListenAddress))` + `RegisterXxxService`
4. **无全局注册表**：不要往 `Config` 上挂按名字解析的 registry；参考 [example/echo/axes.go](../example/echo/axes.go)。

## 依赖方向（摘要）

由 `invariants_test.go` 强制。要点：

- `transport/*` 不得 import `framing` / `descriptor`
- `framing/*` 不得 import `codec`；仅 `framing/grpc` 可 import `compressor` 与 genproto
- 不用 gRPC 的二进制不得传递依赖 `framing/grpc`
- 复用策略只在 `internal/sessionpool`（客户端池读 `Framing.Reuse()`）

完整表见 [README.md §3](../README.md#3-分层与依赖)。

## 扩展验收

```bash
export GOROOT=/data/root/.gvm/1.27.1/go   # 若本机 gvm 与 go 不一致
export PATH="$GOROOT/bin:$PATH"
make verify    # 含 invariants、integration、deps 门禁
```

建议为新组合至少提供：

- 包内单元/环回测试（可参考 `internal/fake`）
- 若走组合层：在 `example/*` 或现有 echo 多传输测试中挂一条路径

## 下一步读什么

- 实现传输层 → [transport.md](transport.md)
- 实现分帧层 → [framing.md](framing.md)
- 消息类型与装配 → [codec-and-wiring.md](codec-and-wiring.md)
- 选型已有组合 → [compatibility-matrix.md](compatibility-matrix.md)

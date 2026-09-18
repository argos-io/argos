# 扩展模型概览

你要加新传输、新分帧或换 codec 时，先弄清 Transport × Codec 各管哪一段。目标协议不限于 gRPC：**凡能落在「连接 + Session 握手 + Call 交换（可选消息流）」上的 C/S 线协议** 都走同一套 `client` / `server` 组合层；覆盖边界见 [architecture.md](architecture.md#协议覆盖范围)。

## Transport × Codec 各自回答什么

| 层 | 问题 | 典型产出 |
|----|------|----------|
| **Transport** | 怎么建连、一条连接上 I/O 长什么样 | `Conn`；按次交换用 `Carrier` |
| **Framing**（在 Transport 实现内） | 握手、复用几条调用、帧/状态写在哪 | `Session` → `Call` |
| **Codec** | 业务消息 ↔ 字节 | 纯函数，无 I/O |

协议 = **已注册的** Transport 名 × Codec 名。`client` / `server` 不会为某个具体实现写 `switch`；配错了在装配或 `New*Session` 等处**直接报错**，不会悄悄换协议。

## 连接是一等事实

```
Transport.Dial/Serve → Conn
  → Framing.New*Session → Session
    → OpenCall / AcceptCall → Call
      → stream.Wrap(Call, Codec) → Stream
```

- **`Transport` 没有 `Close()`**：生命周期是构造它的 ctx（`Serve` 随该 ctx 结束）。limits / 池 / codec 名在构造期用 `WithLimits` / `WithPool` / `WithCodecName` 定死；组合层装配时只核对 codec 名是否与 Codec 一致。要显式释放的实现（如 `transport/grpc`）自带 `Close`，由**构造方**调用。
- 装配中途失败时，组合层不关闭任何 axis（axis 归构造方）；axis 内部创建到一半的资源由 axis 自己回收。

## 如何挂到运行时

1. 在 `codec` / `transport` 包 `Register` 名称（内置子包 `init` 已注册常见组合）；`ServiceOptions` 写 **Transport + Codec 名称**
2. 客户端：`client.New(WithServiceName, WithTarget, WithTransport, WithCodec)`（值为注册名）
3. 服务端：`server.New(WithServerService(..., ServiceListenAddress))` + `Register(desc, XxxHandlers(impl))`
4. 进程级默认与限额：`argos.DefaultOptions()`、`argos.Options` 字段；见 [usage.md](usage.md)
5. 参考 [example/echo](../example/echo) 与 [codec-and-wiring.md](codec-and-wiring.md)

## 依赖方向（摘要）

由 `invariants_test.go` 强制。要点：

- `transport/*` 不得 import `codec`（协议 = axis × codec，import 它就把两者焊死）；字节管道（`tcp` / `ws` / `udp` / `http1` / `http2`）另不得 import `descriptor` / `metadata` / `budget`，完整线栈（`grpc` / `httpunary`）才可以
- 仅 `transport/grpc` 可 import `compressor` 与 genproto
- 不用 gRPC 的二进制不得传递依赖 `grpc`

## 扩展验收

新组合：`make verify` 全绿；若进入 echo 集成，更新 `example/echo` 与 [compatibility-matrix.md](compatibility-matrix.md)。

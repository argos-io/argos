# Codec 与三轴装配

## `Codec`

包：`github.com/argos-io/argos/codec` + `codec/json`、`codec/protobuf` 等。

```go
type Codec interface {
    Marshal(v any) ([]byte, error)
    Unmarshal(b []byte, v any) error
}
```

| 规则 | 说明 |
|------|------|
| 无 I/O | 只处理完整消息字节 |
| 无 `Close` | 无资源所有权 |
| `Unmarshal` | 不得 retain `b` 的子切片；要持久化须拷贝 |
| 可选 `Named` | `CodecName() string`，用于与 `SessionSpec.CodecName` 对齐 |

内置 `protobuf` / `json` 面向 `proto.Message`。httpunary 默认 JSON + protojson；grpc 默认 protobuf。类型与 codec 不匹配在**首次调用**失败（见 [codegen.md](codegen.md)）。

自定义消息模型（非 proto）须自带 `Codec` 并在 `ServiceCodec` 工厂中返回。

## 三轴工厂类型

根包 `argos`：

```go
type TransportFunc func() (transport.Transport, error)
type FramingFunc   func() (framing.Framing, error)
type CodecFunc     func() (codec.Codec, error)
```

工厂**只构造**实例，不得 `Dial` / `Serve`。每个 `client.New` / 每个监听面 `Assemble()` 各调用一次，得到独立三元组。

## `ServiceConfig` 装配

```go
server.New(
    argos.WithService("my.v1.Service",
        argos.ServiceTransport(func() (transport.Transport, error) { return tcp.New(), nil }),
        argos.ServiceFraming(func() (framing.Framing, error) { return myframing.New(), nil }),
        argos.ServiceCodec(func() (codec.Codec, error) { return protobuf.New(), nil }),
        argos.ServiceListenAddress(":7001"),
    ),
)

client.New(
    argos.WithServiceName("my.v1.Service"),
    argos.JoinClient(
        argos.WithTransport(trFactory),
        argos.WithFraming(frFactory),
        argos.WithCodec(cdFactory),
    ),
    argos.WithTarget("ip://127.0.0.1:7001"),
)
```

多监听面：`ServiceListener(addr, ServiceTransport(...), ServiceFraming(...), ...)`，见 `example/echo`。

预设组合可封装为函数，返回三个 `Func`（模式见 `example/echo/axes.go` 的 `GRPCAxes`、`EnvelopeTCPAxes` 等）。

## `SessionSpec` / `CallSpec`

装配时 Framing 收到：

- `SessionSpec{CodecName, Config}` — `Config` 为 `framing.Config` 快照（`MaxMessageSize`、`MaxFrameSize`、metadata 限额、`OpenTimeout`、`MaxDrainBytes` 等）
- `CallSpec{Metadata}` — 组合层创建的 `CallMetadata`；入站 metadata 必须写入此 handle

Framing **不**从全局 `Config` 读；客户端 per-call `budget` 在 call ctx 上（见 [usage.md](usage.md)）。

## 生成桩与路由

- Transport/Framing/Codec 与 `RegisterXxxService` **无关**；路由在 `server.Register` / 生成桩内。
- 客户端工厂 `NewXxxClient` 内置 `service name`；三轴通过 `JoinClient` 或 `Config.Services` 提供。

## 检查单

- [ ] 三个工厂在 `ServiceConfig` 或 `JoinClient` 中成组出现
- [ ] `CodecName` 与 codec 实现一致（若实现了 `Named`）
- [ ] 未引入已删除的 registry API
- [ ] `make verify` 通过；若新组合进入 echo 集成，更新 `example/echo` 测试

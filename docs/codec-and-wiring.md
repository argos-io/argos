# Codec 与 Transport × Codec 装配

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

自定义 `Codec` 在 `codec.Register` 中登记名称；子包可在 `init` 中注册（与 `resolver/ip` 相同模式）。

## 注册表（`codec` / `transport`）

**产品路径**：`ServiceOptions` 上写 **已注册的 transport 名 + codec 名**；`client.New` / 每个监听面装配时各调一次对应工厂的 `New(name)`。

```go
// codec.Register("mycodec", func() (codec.Codec, error) { ... })
// transport.Register("mytr", func() (transport.Transport, error) { ... })

// 内置（blank import 触发 init）：
//   "protobuf", "json"     — codec/protobuf, codec/json
//   "grpc", "httpunary"    — transport/grpc, transport/httpunary
```

## `ServiceOptions` 装配

```go
server.New(argos.WithServerOptions(&argos.Options{
    Services: map[string]argos.ServiceOptions{
        "my.v1.Service": {
            Transport: "grpc", Codec: "protobuf", ListenAddress: ":7001",
        },
    },
}))

client.New(
    argos.WithServiceName("my.v1.Service"),
    argos.WithTransport("grpc"),
    argos.WithCodec("protobuf"),
    argos.WithTarget("ip://127.0.0.1:7001"),
)
```

客户端也可在 `Options.Services` 写 `Target`（及默认 Transport/Codec），由 `WithServiceName` 选中。

多监听面：在 `ServiceOptions.Listeners` 写 `[]argos.ServiceListen`（每项含 `Address`、`Transport`、`Codec`）；见 `example/echo/main.go`。

## `SessionSpec` / `CallSpec`

Transport 实现内部握手时使用：

- `SessionSpec{CodecName, Options}` — `Options` 为 `session.Options` 快照，数值来自轴构造期用 `WithLimits(transport.Limits{...})` 定死的限额（帧/消息/metadata 上限、`ReadAheadMessages`、`OpenTimeout`、`MaxDrainBytes` 等）；`argos.Options` 上不带这些数字
- `CallSpec{Metadata}` — 组合层创建的 `CallMetadata`

实现**不**从全局 `Options` 直接读（见 [usage.md](usage.md)）。

## 生成桩与路由

- Transport/Codec 与 `XxxHandlers` **无关**；路由在 `server.Register(desc, handlers)` / 生成桩内。
- 客户端工厂 `NewXxxClient` 内置 `service name`；Transport + Codec 通过 `WithTransport` / `WithCodec` 或 `Options.Services` 提供。

## 检查单

- [ ] Transport 与 Codec 名称写在 `ServiceOptions` 字段上，或 client 侧 `WithTransport` + `WithCodec`，且已在对应包 `Register`
- [ ] `CodecName` 与 codec 实现一致（若实现了 `Named`）
- [ ] `make verify` 通过；若新组合进入 echo 集成，更新 `example/echo` 测试

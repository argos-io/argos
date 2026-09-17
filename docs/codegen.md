# 代码生成

```bash
go run ./cmd/argos generate stub --from proto --proto-path . example/echo/echo.proto
make test-generate   # stub --check vs example/echo
```

## 生成约定

- 描述符字段不导出；经 `MustMethod` / `MustService` 构造。
- 标识带服务前缀：`EchoService_Echo`、`EchoServiceDesc`。
- 服务端：`RegisterEchoService(srv, impl)`——不生成 `switch method`。
- 客户端：`NewEchoServiceClient(opts...)` 内置 service name 并自持 `client.Client`；`WithServiceName` 可覆盖。
- 单次 RPC 只关 CallStream，不关 Client。
- RPC 不得取名 `Close`（与客户端 `Close() error` 冲突）。

## 消息模型（message model）

流水线：`frontend → IR → message model + stub`。`*.argos.go` 与具体 IDL 无关。

| `message_model` | 行为 |
|---|---|
| `protobuf`（默认） | 生成 `*.pb.go` / `*.msg.go`（proto3） |
| `none` | 只生成 stub |
| 其他（如 `flatbuffers`） | IR 须含 `message_files`；`emit-ir` 插件一次输出 |

**Codec**：内置 `codec/protobuf`、`codec/json` 要求 `proto.Message`。binding 默认见 [codec-and-wiring.md](codec-and-wiring.md)。非 protobuf 须自定义 `WithCodec`；错配在**首次调用**失败。

**插件 IR**：`{plugin} emit-ir -- files...` → JSON（可含 `message_model`、`message_files`）。

**符号冲突**：stub 与 message 生成器写盘前交叉校验包级标识符。

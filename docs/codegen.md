# 代码生成

```bash
go run ./cmd/argos generate stub --from proto --proto-path . example/echo/echo.proto
make test-generate   # stub --check vs example/echo
```

## 生成约定

- 描述符字段不导出；经 `MustMethod` / `MustService` 构造。
- 标识带服务前缀：`EchoService_Echo`、`EchoServiceDesc`。
- 服务端：`EchoServiceHandlers(impl)` + `srv.Register(EchoServiceDesc, ...)`——不生成 `switch method`。
- 客户端：`NewEchoServiceClient(opts...)` 内置 service name，`WithServiceName` 可覆盖；返回的只是 `client.Client` 的句柄，不暴露 `Close`。
- 单次 RPC 只关本次 `CallStream`；生成的客户端没有 `Close`——连接与池由调用方持有的 Transport 轴释放。
- RPC 可取名 `Close`：客户端接口只声明 RPC，`Close() error` 只出现在每次调用的 `<Service>_<Method>Client` 包装上，与服务名派生的客户端类型不同，不冲突。

## 消息模型（message model）

流水线：`frontend → IR → message model + stub`。`*.argos.go` 与具体 IDL 无关。

| `message_model` | 行为 |
|---|---|
| `protobuf`（默认） | 生成 `*.pb.go` / `*.msg.go`（proto3） |
| `none` | 只生成 stub |
| 其他（如 `flatbuffers`） | IR 须含 `message_files`；`emit-ir` 插件一次输出 |

**Codec**：内置 `codec/protobuf`、`codec/json` 要求 `proto.Message`。Transport × Codec 注册名与 echo 默认见 [codec-and-wiring.md](codec-and-wiring.md)。非 protobuf 须自定义 `WithCodec`；错配在**首次调用**失败。

**插件 IR**：`{plugin} emit-ir -- files...` → JSON（可含 `message_model`、`message_files`）。

**符号冲突**：stub 与 message 生成器写盘前交叉校验包级标识符。

# 内置组合与 Carrier 能力矩阵

用于选型与自检：每个 **Transport 轴**在 `New*Session` 里对 `Conn` 的窄接口断言必须与其底层 pipe 一致。

## 已验证组合（里程碑资产）

| 注册名（Transport × Codec） | 实现内 pipe | `Reuse()` | 证明点 |
|-----------------------------|-------------|-----------|--------|
| `grpc` × `protobuf` | http2 + gRPC 分帧 | Concurrent | 多路复用、H2 头/trailers、gRPC 压缩 |
| `httpunary` × `json` | http1 + 整包 unary | Concurrent | 整 body 一次提交、仅 unary |
| `resp`（example） | tcp + RESP2 | Sequential | 连接级 HELLO/AUTH、无 metadata |
| `synth`（example） | tcp + 合成协议 | Sequential | 服务端先发 greeting 等合成行为 |

产品选型：在 **`ServiceOptions`**（或 `ServiceListen` 多面）写 transport / codec 注册名，client 侧用 `WithTransport` / `WithCodec`（如 `grpc` + `protobuf`、`httpunary` + `json`）。echo 见 [`example/echo/main.go`](../example/echo/main.go)。

上表「pipe」列仅便于对照 `Reuse()` 与 Carrier；对外只配 Transport 名与 Codec 名。

## Conn 角色（按 pipe）

| | 客户端 `Conn` | 服务端 |
|--|---------------|--------|
| tcp / ws / udp | `CarrierConn` | `CarrierConn` |
| http1 | `StreamConn`（连接池句柄） | 每 HTTP 请求一个 `CarrierConn` |
| http2 | `StreamConn` | `StreamConn`（每 stream 一个 Carrier） |

## Carrier 能力 × 用途

| 能力 | grpc | httpunary | example/resp (tcp) |
|------|------|-----------|---------------------|
| `ByteStreamCarrier` | ✓ | ✓ | ✓ |
| `MessageCarrier` | — | — | — |
| `DatagramCarrier` | — | — | — |
| `SendCloser` | ✓ | ✓ | ✓ |
| `RequestHeaderReader` | ✓ | ✓ | — |
| `ResponseHeaderReader` | ✓ | ✓ | — |
| `ResponseTrailerReader` | ✓ | — | — |
| `ResponseWriter` (H2) | ✓ | — | — |
| `UnaryResponseWriter` (H1) | — | ✓ | — |

## 形态（Shape）支持

| Transport | Unary | 客户端流 | 服务端流 | 双向流 |
|---------|-------|----------|----------|--------|
| grpc | ✓ | ✓ | ✓ | ✓ |
| httpunary | ✓ | ✗（Accept 拒绝） | ✗ | ✗ |
| example/resp | ✓ | ✗ | ✓（SUBSCRIBE 等） | ✗ |

## 不打算支持的组合

- Transport 轴与底层 `Conn` 能力不匹配（如 httpunary 配无 `StreamConn`/`UnaryResponseWriter` 路径）→ 应在 `New*Session` 失败。
- udp 上多路复用、非 gRPC 的通用 H2 应用协议 → 见 README 非目标。

新增组合时：在本表加一行，并补充 `New*Session` 断言测试或 echo 集成用例。

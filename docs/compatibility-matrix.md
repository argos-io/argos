# 内置组合与 Carrier 能力矩阵

用于选型与自检：**Framing 在 `New*Session` 里 assert 的能力必须与实际 Transport 一致**。

## 已验证组合（里程碑资产）

| Transport | Framing | Codec | `Reuse()` | 证明点 |
|-----------|---------|-------|-----------|--------|
| http2 | grpc | protobuf | Concurrent | 多路复用、H2 头/trailers、gRPC 压缩 |
| http1 | httpunary | json | Concurrent | 整 body 一次提交、仅 unary |
| tcp | resp（example） | — | Sequential | 连接级 HELLO/AUTH、无 metadata |
| tcp | synth（example） | — | Sequential | 服务端先发 greeting 等合成行为 |

产品选型：**Transport 实例 × Codec**（如 `transport/grpc` + protobuf、`transport/httpunary` + json）。echo 入口：`example/echo/binding.go`（`GRPCTransport`、`HTTPUnaryRPCTransport`）。

上表按实现内部分解为 pipe + framing，便于对照 `Reuse()` 与 Carrier；对外配置不再要求三工厂。

## Conn × Framing 角色

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

| Framing | Unary | 客户端流 | 服务端流 | 双向流 |
|---------|-------|----------|----------|--------|
| grpc | ✓ | ✓ | ✓ | ✓ |
| httpunary | ✓ | ✗（Accept 拒绝） | ✗ | ✗ |
| example/resp | ✓ | ✗ | ✓（SUBSCRIBE 等） | ✗ |

## 不打算支持的组合

- 任意 Framing 配不匹配的 Conn（如 httpunary 配无 `StreamConn`/`UnaryResponseWriter` 路径）→ 应在 `New*Session` 失败。
- udp 上多路复用、非 gRPC 的通用 H2 应用协议 → 见 README 非目标。

新增组合时：在本表加一行，并补充 `New*Session` 断言测试或 echo 集成用例。

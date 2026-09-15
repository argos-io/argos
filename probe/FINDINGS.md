# 探针结论（里程碑 ⓪）

> 里程碑 ① 冻结接口前，每条探针结论在此追加。格式：日期 · 探针名 · 结论 · 数据。

## Task 0.2 — `ReceiveOpen` 不确定窗口

**日期**：2026-09-15  
**探针**：`TestSendWindowReceiveOpen`  
**参数**：chunk 16 KiB × 4096 循环（最多 64 MiB）；50 次/轮；`-race -count=10`

### 窗口命中率

| 轮次 | 命中/50 | 比例 |
|------|---------|------|
| 1 | 6 | 12% |
| 2 | 10 | 20% |
| 3 | 5 | 10% |
| 4 | 11 | 22% |
| 5 | 10 | 20% |
| 6 | 10 | 20% |
| 7 | 8 | 16% |
| 8 | 14 | 28% |
| 9 | 11 | 22% |
| 10 | 14 | 28% |
| **合计** | **99/500** | **19.8%** |

### 结论

- **不确定窗口真实存在**：写失败后 `responded()` 仍为 false 的采样点稳定出现（每轮 5–14 次，约 10–28%）。
- **保守 `ReceiveOpen`（不确定时返回 true）给出正确结果**：无论是否落在窗口内，写失败后 `ResponseHeaders` 均返回 `Grpc-Status: 16`（Unauthenticated），500/500 次一致。
- **§2.4 契约成立**：`false` 仅用于已确知不可恢复；窗口内返回 `true` 由后续 `Recv` 给出确定结果——本探针验证了后者。

## Task 0.11 — UDP 单包 envelope 往返

**日期**：2026-09-15  
**探针**：`TestUDP*`  
**参数**：`127.0.0.1` loopback · `net.ListenPacket("udp", …)` · `-race`

### 实测数据

| 项 | 结果 |
|---|---|
| 单请求/单响应数据报计数 | 客户端、服务端各 1 读 + 1 写 |
| `OPEN\|END` 零消息 | 首次 `Recv` 得 `io.EOF`；响应仍单包 |
| 错误 call ID | 不匹配响应被丢弃，正确 ID 后续到达 |
| IPv4 UDP 有效载荷上限 | **65507** 字节可发；**65508** 字节 `WriteTo` 失败 |

### 结论

- **单包往返成立**：请求 `OPEN + DATA + END`、响应 `HEADERS + DATA + STATUS` 各编码为一个 UDP 数据报，两侧计数均为 1。
- **零消息可区分**：`OPEN\|END` 与零长度 `DATA` 语义分离；零消息路径首次 `Recv` 直接 EOF。
- **call ID 校验成立**：响应归属靠 call ID 匹配，不匹配包丢弃而非误用。
- **§4.5 尺寸上限可验证**：loopback 上 65507 字节为实测可发上限，支持 Binding 启动期 `MaxMessageSize`/`MaxFrameSize` 校验依据。

## Task 0.3 — grpc-go ↔ probe 双向 h2c unary

**日期**：2026-09-15  
**探针**：`TestGRPCGoClientToProbeServer` · `TestProbeClientToGRPCGoServer`  
**参数**：h2c · `grpc.health.v1.Health/Check` · `-race -count=1`

### 实测数据

| 方向 | 结果 |
|------|------|
| grpc-go client → probe h2c server | `Check` → `SERVING`；`Grpc-Status: 0` |
| probe client → grpc-go server（h2c） | LPM 响应 `SERVING`；EOF 后 trailers `Grpc-Status: 0` |

### 结论

- **原生互通成立**：双向 health Check over h2c 均 PASS；native grpc-go 与探针 framing 可互操作。
- **Content-Type 须为 `application/grpc+proto`**：探针服务端显式设置该值，grpc-go 客户端方可接受。
- **Trailer 须提前声明**：响应头需 `Trailer: Grpc-Status, Grpc-Message`，再 `WriteHeader(200)`；status trailer 在 body 写出后设置。
- **Flush 必要**：body LPM 写出后须 `Flush()`，否则客户端可能等不到完整响应/trailers。
- **Trailers 仅在 body EOF 后可读**：探针客户端必须读到第二次 `Recv` 的 `io.EOF` 才能拿到完整 `Grpc-Status`（§2.2 单响应终态）。

## Task 0.10 — HTTP/1 Finish 一次性提交

**日期**：2026-09-15  
**探针**：`TestHTTP1FinishOnError` · `TestHTTP1CannotChangeAfter200`  
**参数**：`httptest.NewServer` · buffered Send → Finish · `-race -count=1`

### 实测数据

| 场景 | 状态码 | body |
|------|--------|------|
| Send 缓冲成功体后 Finish(500) | **500** | 仅错误 JSON；无成功体泄漏 |
| `WriteHeader(200)` 后再 `WriteHeader(500)` | **200**（第二次被忽略） | 仍写出错误 JSON；日志 `superfluous response.WriteHeader` |

### 结论

- **响应在 Finish 一次性提交**：Send 只缓冲、不落线；Finish 才 `WriteHeader` + body，handler 事后 error 仍能提交 500。
- **标准 ResponseWriter 不可改已提交状态**：一旦 `WriteHeader(200)`，后续 `WriteHeader(500)` 被忽略（net/http 打 superfluous 警告），客户端永远看到 200。
- **支撑 UnaryResponseWriter 契约**：wholebody × http1 必须缓冲至 Finish，否则无法在 Send 之后用错误状态覆盖——§4.6 成立。

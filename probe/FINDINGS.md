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

## Task 0.1 — OpenStream 在响应 headers 之前返回（重跑）

**日期**：2026-09-15  
**探针**：`TestOpenStreamReturnsBeforeResponseHeaders`  
**参数**：h2c · `newH2Endpoint` + `openH2Stream` · `-race`

### 结论

- **成立**：第五轮把非阻塞约束从 Dial 挪到 `StreamConn.OpenStream`。`newH2Endpoint` 无 I/O；`openH2Stream` 在响应 headers 到达前返回可写 `*h2Carrier`（`io.Pipe` + 后台 `RoundTrip`）。
- **断言**：返回后、写首字节前 `responded()==false`；服务端读到 body 且未写响应头时仍为 false；释放后 `ResponseHeaders` 成功。
- **无需改设计**；`dialH2` 已删除，sendwindow / grpc-go 互通调用点已切换。

## Task 0.9 — OpenFilter 短路不占池中会话（重跑）

**日期**：2026-09-15  
**探针**：`TestOpenFilterShortCircuitDoesNotBorrowSession` 等  
**参数**：内存 `sessionPoolStub` · `-race`

### 结论

- **成立**：链终点为「取会话 → OpenCall」。短路过滤器不调用 `next` 时，`openCallCount`、`borrowCount` 均为 0，且 `idle`/`inUse` 与调用前一致——既不产生网络资源，也不占用池中空闲会话。
- 包装顺序、追加失败、不得清除已有失败、(nil,nil) 误用检出均仍绿。

## Task 0.13 — 顺序复用借还

**日期**：2026-09-15  
**探针**：`probe/sequential_test.go` · `probe/seqconn.go`  
**参数**：tcp length-prefixed · Sequential 会话池 · `-race`

### 实测数据

| 项 | 实测 |
|---|---|
| 单连接 100 次 | DialCount=1, Borrow=100, Return=100, 无串包 |
| Call.Close 不关 Conn | 100 次后 Close flag=false |
| 并发借出 8 | DialCount=8, distinct=8, 无排队 |
| Busy 兜底 | BusyRetry=3, ResourceExhausted↔ErrSessionsExhausted, Busy 不外泄 |
| 故障丢弃 #50 | Reusable=false, 后续 DialCount=2 |
| Cap=2 / 4 并发 | ok=2 exhausted=2, 立即返回 |
| 承载卫生 | on: 无串包+新连接; off: call31 读到 "EXTRA" |
| Read 交接 | ConcurrentHits=0 |
| 跨调用缓冲 | Close 后 Session 保留下一帧 |
| Conn ctx | 非 call 子 ctx; HandshakeTimeout 不杀空闲连接 |

### 结论

- **§2.1 Session / Sequential 借还模型可实现**；步骤 1–5e 全绿，无需改设计。

## Task 0.4 — TLS/ALPN、trailers-only、grpc-timeout、`-bin` metadata

**日期**：2026-09-15  
**探针**：`TestGRPCGoTLS`  
**参数**：自签 TLS · ALPN `h2` · `-race -count=1`

### 实测数据

| 子测试 | 结果 |
|--------|------|
| `TLS_ALPN` | unary OK；`NegotiatedProtocol == "h2"` |
| `TrailersOnly` | Status:5 → `codes.NotFound`；无 initial metadata |
| `Timeout` | `Grpc-Timeout` 可解析且 ≤ 100ms |
| `BinaryMetadata` | `-bin` 两侧原字节；线上 unpadded base64 |

### 结论

- TLS/ALPN、trailers-only、`grpc-timeout`、`-bin` metadata 均成立；无需改设计。

## Task 0.5 — 单响应终态与 cardinality

**日期**：2026-09-15  
**探针**：`TestCardinality`  
**参数**：probe→probe h2c · `-race`

### 结论

- 成功路径须「一条 DATA + 再读 EOF」；零条/两条在 OK 下检出 cardinality；非 OK 优先于响应体；第二条须独立消息对象（§2.2）。

## Task 0.6 — 真实双向流与 headers 握手

**日期**：2026-09-15  
**探针**：`TestHandshakeOrder*` · `TestEmptyMetadata*` · `TestInterleavedBidiStreaming` · `TestBidiEarlyError*`  
**参数**：h2c · grpc-go 手写 bidi · `-race -count=5`

### 结论

- 主动 initial headers、空 metadata 唤醒、交错 bidi 100 条、提前错误可读均成立。

## Task 0.7 — HTTP/2 连接复用与流隔离

**日期**：2026-09-15  
**探针**：`TestHTTP2ConnectionReuse` · `TestHTTP2StreamIsolation` · `TestHTTP2StreamGoroutineLeak`  
**参数**：32 并发 · `-race`

### 实测数据

| 项 | 结果 |
|---|---|
| 唯一 TCP 连接 | **1** |
| 取消 1 路后其余完成 | **31/31** |
| goroutine Δ after settle | **0** |

### 结论

- Concurrent 复用与流隔离成立；无常驻 goroutine 泄漏。

## Task 0.8 — 有界背压与额度峰值

**日期**：2026-09-15  
**探针**：`TestSlowConsumerBudgetPeak` 等  
**参数**：1000×64KiB · ReadAheadMessages=1 · `-race`

### 实测数据

| 项 | 结果 |
|---|---|
| 慢消费峰值计费 | **131072** B（128 KiB）≤ perCall 16 MiB |
| 取消中途 | 额度归零；无泄漏 |
| 4 MiB / 4MiB+1 | 通过 / 分配前拒绝 |

### 结论

- 有界预读与额度上限在真实流控下成立。

## Task 0.14 — 服务端 AcceptCall 循环

**日期**：2026-09-15  
**探针**：`TestAcceptCall*` / `TestShutdown*` / `TestHandshake*` / `TestIdleAndSlowloris` / `TestResidualFrameSkip`  
**结论**：成立

### 实测数据

| 项 | 结果 |
|---|---|
| 单连接 10 调用 | HandlerCount=10，同一 onConn |
| 空闲 Shutdown 叫醒 | ~166µs |
| HandshakeTimeout | 挂握手子 ctx；完成后不约束存续 |
| 残余帧 / MaxDrainBytes | 跳过 N+1；超限 → 连接级错误 |

### 结论

- accept ctx 与连接 ctx 分离足够表达优雅关闭；OpenTimeout 自首字节起算；有界 drain 保持连接卫生。步骤 1–5d 全绿，无需改设计。

---

## 里程碑 ⓪ 汇总（Task 0.16）

**日期**：2026-09-15  
**验证**：`go test ./probe/ -race -count=3` 全绿；`make verify` 全绿。

| 任务 | 结论 |
|---|---|
| 0.0 Makefile+CI | 成立 |
| 0.1 OpenStream 非阻塞 | 成立（重跑） |
| 0.2 ReceiveOpen 窗口 | 成立 |
| 0.3 grpc-go h2c | 成立 |
| 0.4 TLS/ALPN 等 | 成立 |
| 0.5 cardinality | 成立 |
| 0.6 bidi/headers | 成立 |
| 0.7 复用/隔离 | 成立 |
| 0.8 有界背压 | 成立（峰值 128 KiB） |
| 0.9 OpenFilter+池 | 成立（重跑） |
| 0.10 HTTP/1 Finish | 成立 |
| 0.11 UDP 单包 | 成立 |
| 0.13 Sequential 借还 | 成立 |
| 0.14 AcceptCall 循环 | 成立 |

**与规格不符项**：无。公开接口可按 §2.1 冻结进入里程碑 ①。

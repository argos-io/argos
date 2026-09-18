# gRPC 生态可选包（Health / Reflection / Retry）

业务仍走 **http2 + grpc + protobuf** Transport × Codec（legacy 三工厂过渡）；Health、Reflection 是**额外 Register 的标准 gRPC 服务**，与 `echo.v1.EchoService` 等业务服务共用同一监听面（相同 `ServiceListenAddress` + 相同Transport × Codec（legacy 三工厂过渡）工厂时，`server.Run` 只开一条 listener，按 method 路由）。

包路径（按需 import，不进入根包 `argos`）：

| 包 | 作用 |
|---|---|
| `transport/transport/grpc/health` | `grpc.health.v1.Health`（Check / List / Watch） |
| `transport/transport/grpc/reflection` | `grpc.reflection.v1.ServerReflection`（v1，对齐 grpc-go `RegisterV1`） |
| `transport/transport/grpc/retry` | 客户端 `Attempts` 包装 Open（非内置重试策略） |

Filter / OpenFilter 见 [usage.md](usage.md) 运行时路径；观测、鉴权仍用 `WithFilter` / `WithOpenFilter`。

---

## 服务端：Echo + Health + Reflection（同端口）

```go
package main

import (
	"context"
	"log"

	"github.com/argos-io/argos"
	echov1 "github.com/argos-io/argos/example/echo"
	"github.com/argos-io/argos/transport/transport/grpc/health"
	"github.com/argos-io/argos/transport/transport/grpc/reflection"
	"github.com/argos-io/argos/server"
	healthpb "google.golang.org/transport/grpc/health/grpc_health_v1"
	"google.golang.org/protobuf/reflect/protoregistry"
)

const (
	echoService = "echo.v1.EchoService"
	listen      = ":9090"
)

func main() {
	srv := server.New(
		argos.WithServerService(echoService,
			argos.ServiceTransport("grpc"), argos.ServiceCodec("protobuf"),
			argos.ServiceListenAddress(listen),
		),
		argos.WithServerService(health.ServiceName,
			argos.ServiceTransport("grpc"), argos.ServiceCodec("protobuf"),
			argos.ServiceListenAddress(listen),
		),
		argos.WithServerService(reflection.ServiceV1,
			argos.ServiceTransport("grpc"), argos.ServiceCodec("protobuf"),
			argos.ServiceListenAddress(listen),
		),
	)

	if err := srv.Register(echov1.EchoServiceDesc, echov1.EchoServiceHandlers(echov1.NewEchoImpl())); err != nil {
		log.Fatal(err)
	}
	hs := health.NewServer()
	hs.SetServingStatus(echoService, healthpb.HealthCheckResponse_SERVING)
	if err := health.Register(srv, hs); err != nil {
		log.Fatal(err)
	}

	files := new(protoregistry.Files)
	if err := files.RegisterFile(echov1.File_example_2f_echo_2f_echo_2e_proto); err != nil {
		log.Fatal(err)
	}
	if err := reflection.Register(srv, reflection.Options{
		Services: []string{echoService, health.ServiceName, reflection.ServiceV1},
		Files:    files,
	}); err != nil {
		log.Fatal(err)
	}

	log.Fatal(srv.Run(context.Background()))
}
```

`File_example_2f_echo_2f_echo_2e_proto` 来自同包的 `echo.pb.go`（`echov1.File_example_2f_echo_2f_echo_2e_proto`）。

**grpcurl（h2c）**

```bash
grpcurl -plaintext localhost:9090 list
grpcurl -plaintext localhost:9090 describe echo.v1.EchoService
```

**Shutdown 探活**：在 `srv.Shutdown` 前调用 `hs.Shutdown()`，与 grpc-go 一致，全部变为 `NOT_SERVING`。

---

## Reflection 描述符从哪来

| 来源 | 用法 |
|------|------|
| 生成 `*.pb.go` | `files.RegisterFile(pkg.File_…_proto)`（见上） |
| 多个 proto | 多次 `RegisterFile`，或维护 `FileDescriptorSet` 二进制后 `protodesc.NewFiles` |
| ListServices | `Options.Services` 填**完整服务名**（与 `RegisterXxxService` / IDL package 一致） |

Reflection 只回答 schema；**不会**自动扫描 `server.Register` 的路由表。

---

## 客户端：OpenFilter 与重试

`OpenFilter` 每层对 `next` **最多调用一次**，多轮重试不要写在单个 OpenFilter 的 for 循环里（会触发 `ErrOpenFilterMisuse`）。

**推荐**：终端 Open 外包 `retry.Attempts`；metadata / trace 仍用 OpenFilter。

```go
import (
	"context"

	"github.com/argos-io/argos"
	"github.com/argos-io/argos/client"
	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/filter"
	"github.com/argos-io/argos/transport/transport/grpc/retry"
	"github.com/argos-io/argos/metadata"
	"github.com/argos-io/argos/stream"
)

func dialEcho(ctx context.Context) (*client.Client, error) {
	return client.New(
		argos.WithServiceName("echo.v1.EchoService"),
		argos.WithTarget("ip://127.0.0.1:9090"),
		argos.WithTransport(tr),
		argos.WithCodec(cd),
		argos.WithOpenFilter(func(ctx context.Context, m descriptor.Method, next filter.OpenFunc) (stream.Stream, error) {
			if md, ok := metadata.FromContext(ctx); ok {
				_ = md.AddOutgoingHeader("x-trace", "demo")
			}
			return next(ctx, m)
		}),
	)
}

func echoWithRetry(ctx context.Context, cli *client.Client, m descriptor.Method) error {
	open := func() (stream.Stream, error) {
		return cli.Open(ctx, m)
	}
	st, err := retry.Attempts(retry.Policy{MaxAttempts: 3}, open)
	if err != nil {
		return err
	}
	defer st.Close()
	// … Send / Recv …
	return nil
}
```

默认重试 `Unavailable` 与 `DeadlineExceeded`；自定义 `Policy.Retriable` 即可。未实现 grpc-go ServiceOptions 透明重试 / pushback。

---

## 背压（budget）

无需额外 API：`MaxBufferedBytes` 与 `MaxConcurrentCalls` 已在 `Open` / `handleCall` 准入时 carve `perCall`，grpc / httpunary 等在读写 payload 时扣减。超限为 `status.ErrCallsExhausted`。

---

## TLS（http2 / http1）

```go
import argoshttp2 "github.com/argos-io/argos/transport/http2"

tr := argoshttp2.New(
	argoshttp2.WithServerTLS(serverTLS),
)
clientTr := argoshttp2.New(
	argoshttp2.WithClientTLS(clientTLS),
)
```

http1 同样提供 `transport/http1.WithServerTLS` / `WithClientTLS`；客户端配置 TLS 时对裸 `host:port` 自动使用 `https://`。

---

## 参考测试

| 用例 | 位置 |
|------|------|
| Echo + Health + Reflection 同服 | `example/echo/grpc_ecosystem_test.go` |
| Health ↔ grpc-go client | `transport/transport/grpc/health/health_test.go` |
| Reflection ListServices | `transport/transport/grpc/reflection/reflection_test.go` |
| Retry Attempts | `transport/transport/grpc/retry/retry_test.go` |

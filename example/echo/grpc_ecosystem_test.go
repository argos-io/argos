package echov1

import (
	"context"
	"testing"
	"time"

	"github.com/argos-io/argos"
	"github.com/argos-io/argos/internal/teststack"
	"github.com/argos-io/argos/server"
	"github.com/argos-io/argos/transport/grpc/health"
	"github.com/argos-io/argos/transport/grpc/reflection"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	reflectionpb "google.golang.org/grpc/reflection/grpc_reflection_v1"
	"google.golang.org/protobuf/reflect/protoregistry"
)

// TestGRPCEcosystemOnEchoServer documents health + reflection on the same gRPC
// listener as echo (see docs/grpc-ecosystem.md).
func TestGRPCEcosystemOnEchoServer(t *testing.T) {
	const echoSvc = "echo.v1.EchoService"

	cfg := baseOptions()
	ax, err := GRPCTransport()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ax.Close() })
	trName := teststack.TransportName(t, ax)
	stack := []argos.ServiceOption{argos.ServiceTransport(trName), argos.ServiceCodec(GRPCCodecName)}
	srv := server.New(
		argos.WithServerOptions(cfg),
		argos.WithServerService(echoSvc, append(stack, argos.ServiceListenAddress(testListenAddr))...),
		argos.WithServerService(health.ServiceName, append(stack, argos.ServiceListenAddress(testListenAddr))...),
		argos.WithServerService(reflection.ServiceV1, append(stack, argos.ServiceListenAddress(testListenAddr))...),
	)
	if err := srv.Register(EchoServiceDesc, EchoServiceHandlers(NewEchoImpl())); err != nil {
		t.Fatal(err)
	}
	hs := health.NewServer()
	hs.SetServingStatus(echoSvc, healthpb.HealthCheckResponse_SERVING)
	if err := health.Register(srv, hs); err != nil {
		t.Fatal(err)
	}
	files := new(protoregistry.Files)
	if err := files.RegisterFile(File_example_2f_echo_2f_echo_2e_proto); err != nil {
		t.Fatal(err)
	}
	if err := reflection.Register(srv, reflection.Options{
		Services: []string{echoSvc, health.ServiceName, reflection.ServiceV1},
		Files:    files,
	}); err != nil {
		t.Fatal(err)
	}

	// A Server stops when Run's ctx is canceled — there is no Server.Close —
	// so the test cancels the ctx it handed to Run.
	runCtx, stopServer := context.WithCancel(context.Background())
	go func() { _ = srv.Run(runCtx) }()
	addr := waitAddr(t, ax)
	t.Cleanup(stopServer)

	cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cc.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	hcli := healthpb.NewHealthClient(cc)
	resp, err := hcli.Check(ctx, &healthpb.HealthCheckRequest{Service: echoSvc})
	if err != nil {
		t.Fatalf("health Check: %v", err)
	}
	if resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("health status %v", resp.GetStatus())
	}

	rcli := reflectionpb.NewServerReflectionClient(cc)
	stream, err := rcli.ServerReflectionInfo(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&reflectionpb.ServerReflectionRequest{
		MessageRequest: &reflectionpb.ServerReflectionRequest_ListServices{},
	}); err != nil {
		t.Fatal(err)
	}
	rresp, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]struct{}{}
	for _, s := range rresp.GetListServicesResponse().GetService() {
		names[s.GetName()] = struct{}{}
	}
	if _, ok := names[echoSvc]; !ok {
		t.Fatalf("reflection list missing %q: %v", echoSvc, names)
	}

	ec, err := NewEchoServiceClient(
		argos.WithClientOptions(cfg),
		argos.WithTransport(trName),
		argos.WithCodec(GRPCCodecName),
		argos.WithTarget("ip://"+addr),
	)
	if err != nil {
		t.Fatal(err)
	}
	out, err := ec.Echo(ctx, &EchoRequest{Msg: "eco"})
	if err != nil {
		t.Fatal(err)
	}
	if out.GetMsg() != "hello eco" {
		t.Fatalf("echo %q", out.GetMsg())
	}
}

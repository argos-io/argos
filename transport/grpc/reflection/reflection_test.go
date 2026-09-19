package reflection

import (
	"context"
	"testing"
	"time"

	"github.com/argos-io/argos"
	_ "github.com/argos-io/argos/codec/protobuf"
	"github.com/argos-io/argos/internal/teststack"
	"github.com/argos-io/argos/server"
	grpctransport "github.com/argos-io/argos/transport/grpc"
	"github.com/argos-io/argos/transport/grpc/internal/testpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	reflectionpb "google.golang.org/grpc/reflection/grpc_reflection_v1"
	"google.golang.org/protobuf/reflect/protoregistry"
)

func startReflection(t *testing.T, services []string) string {
	t.Helper()
	cfg := &argos.Options{
		MaxConcurrentCalls: 64,
		ListenAddress:      "127.0.0.1:0",
	}
	ax, err := grpctransport.NewTransport()
	if err != nil {
		t.Fatal(err)
	}
	trName := teststack.TransportName(t, ax)
	cfg.Services = map[string]argos.ServiceOptions{
		ServiceV1: {
			Transport: trName, Codec: "protobuf", ListenAddress: cfg.ListenAddress,
		},
	}
	srv := server.New(argos.WithServerOptions(cfg))
	files := new(protoregistry.Files)
	if err := files.RegisterFile(testpb.File_echo_proto); err != nil {
		t.Fatal(err)
	}
	if err := Register(srv, Options{Services: services, Files: files}); err != nil {
		t.Fatal(err)
	}
	// A Server stops when Run's ctx is canceled — there is no Server.Close — so
	// the test stops it by canceling that ctx and waiting for Run to return.
	// The axis belongs to this helper, which constructed it.
	runCtx, stopServer := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		_ = srv.Run(runCtx)
	}()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if ax.Addr() != nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if ax.Addr() == nil {
		t.Fatal("no listen addr")
	}
	t.Cleanup(func() {
		stopServer()
		<-runDone
		_ = ax.Close()
	})
	return ax.Addr().String()
}

func TestListServicesV1(t *testing.T) {
	const echoSvc = "echo.v1.Echo"
	addr := startReflection(t, []string{echoSvc, ServiceV1})

	cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cc.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	c := reflectionpb.NewServerReflectionClient(cc)
	stream, err := c.ServerReflectionInfo(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&reflectionpb.ServerReflectionRequest{
		MessageRequest: &reflectionpb.ServerReflectionRequest_ListServices{},
	}); err != nil {
		t.Fatal(err)
	}
	resp, err := stream.Recv()
	if err != nil {
		t.Fatal(err)
	}
	list := resp.GetListServicesResponse().GetService()
	names := make(map[string]struct{}, len(list))
	for _, s := range list {
		names[s.GetName()] = struct{}{}
	}
	if _, ok := names[echoSvc]; !ok {
		t.Fatalf("missing %q in %v", echoSvc, names)
	}
	_ = stream.CloseSend()
}

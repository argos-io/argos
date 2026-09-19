package health

import (
	"context"
	"testing"
	"time"

	"github.com/argos-io/argos"
	_ "github.com/argos-io/argos/codec/protobuf"
	"github.com/argos-io/argos/internal/teststack"
	"github.com/argos-io/argos/server"
	grpctransport "github.com/argos-io/argos/transport/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	grpcstatus "google.golang.org/grpc/status"
)

func startArgosHealth(t *testing.T, hs *Server) string {
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
		ServiceName: {
			Transport: trName, Codec: "protobuf", ListenAddress: cfg.ListenAddress,
		},
	}
	srv := server.New(argos.WithServerOptions(cfg))
	if err := Register(srv, hs); err != nil {
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
		t.Fatal("health server did not bind")
	}
	t.Cleanup(func() {
		stopServer()
		<-runDone
		_ = ax.Close()
	})
	return ax.Addr().String()
}

func TestCheckInteropGRPCGoClient(t *testing.T) {
	hs := NewServer()
	addr := startArgosHealth(t, hs)

	cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = cc.Close() })

	cli := healthpb.NewHealthClient(cc)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := cli.Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("status %v, want SERVING", resp.GetStatus())
	}

	hs.SetServingStatus("my.service", healthpb.HealthCheckResponse_SERVING)
	resp, err = cli.Check(ctx, &healthpb.HealthCheckRequest{Service: "my.service"})
	if err != nil {
		t.Fatalf("Check(my.service): %v", err)
	}
	if resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("status %v, want SERVING", resp.GetStatus())
	}

	_, err = cli.Check(ctx, &healthpb.HealthCheckRequest{Service: "missing"})
	if err == nil {
		t.Fatal("expected NOT_FOUND for unknown service")
	}
	if grpcstatus.Code(err) != codes.NotFound {
		t.Fatalf("unknown service: code %v, err %v", grpcstatus.Code(err), err)
	}
}

func TestListInteropGRPCGoClient(t *testing.T) {
	hs := NewServer()
	addr := startArgosHealth(t, hs)

	cc, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = cc.Close() })

	cli := healthpb.NewHealthClient(cc)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	list, err := cli.List(ctx, &healthpb.HealthListRequest{})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if list.GetStatuses()[""].GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("overall status not SERVING")
	}
}

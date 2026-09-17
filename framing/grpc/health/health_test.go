package health_test

import (
	"context"
	"testing"
	"time"

	"github.com/argos-io/argos"
	"github.com/argos-io/argos/codec"
	"github.com/argos-io/argos/codec/protobuf"
	"github.com/argos-io/argos/framing"
	grpcframing "github.com/argos-io/argos/framing/grpc"
	"github.com/argos-io/argos/framing/grpc/health"
	"github.com/argos-io/argos/server"
	"github.com/argos-io/argos/transport"
	argoshttp2 "github.com/argos-io/argos/transport/http2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	grpcstatus "google.golang.org/grpc/status"
)

func startArgosHealth(t *testing.T, hs *health.Server) string {
	t.Helper()
	cfg := &argos.Config{
		MaxConcurrentCalls: 64,
		ListenAddress:      "127.0.0.1:0",
	}
	var srvTr *argoshttp2.Transport
	bound := make(chan struct{})
	srv := server.New(
		argos.WithConfig(cfg),
		argos.WithService(health.ServiceName,
			argos.ServiceTransport(func() (transport.Transport, error) {
				tr := argoshttp2.New()
				srvTr = tr.(*argoshttp2.Transport)
				select {
				case <-bound:
				default:
					close(bound)
				}
				return tr, nil
			}),
			argos.ServiceFraming(func() (framing.Framing, error) { return grpcframing.New() }),
			argos.ServiceCodec(func() (codec.Codec, error) { return protobuf.New(), nil }),
			argos.ServiceListenAddress(cfg.ListenAddress),
		),
	)
	if err := health.Register(srv, hs); err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Run(context.Background()) }()
	select {
	case <-bound:
	case <-time.After(3 * time.Second):
		t.Fatal("health server transport not assembled")
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if srvTr.Addr() != nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if srvTr.Addr() == nil {
		t.Fatal("health server did not bind")
	}
	t.Cleanup(func() { _ = srv.Close() })
	return srvTr.Addr().String()
}

func TestCheckInteropGRPCGoClient(t *testing.T) {
	hs := health.NewServer()
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
	hs := health.NewServer()
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

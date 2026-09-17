package reflection_test

import (
	"context"
	"testing"
	"time"

	"github.com/argos-io/argos"
	"github.com/argos-io/argos/codec"
	"github.com/argos-io/argos/codec/protobuf"
	"github.com/argos-io/argos/framing"
	grpcframing "github.com/argos-io/argos/framing/grpc"
	"github.com/argos-io/argos/framing/grpc/internal/testpb"
	"github.com/argos-io/argos/framing/grpc/reflection"
	"github.com/argos-io/argos/server"
	"github.com/argos-io/argos/transport"
	argoshttp2 "github.com/argos-io/argos/transport/http2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	reflectionpb "google.golang.org/grpc/reflection/grpc_reflection_v1"
	"google.golang.org/protobuf/reflect/protoregistry"
)

func startReflection(t *testing.T, services []string) string {
	t.Helper()
	cfg := &argos.Config{
		MaxConcurrentCalls: 64,
		ListenAddress:      "127.0.0.1:0",
	}
	var srvTr *argoshttp2.Transport
	bound := make(chan struct{})
	mkAxes := func() (argos.TransportFunc, argos.FramingFunc, argos.CodecFunc) {
		return argos.TransportFunc(func() (transport.Transport, error) {
				if srvTr == nil {
					tr := argoshttp2.New()
					srvTr = tr.(*argoshttp2.Transport)
					select {
					case <-bound:
					default:
						close(bound)
					}
					return tr, nil
				}
				return srvTr, nil
			}),
			argos.FramingFunc(func() (framing.Framing, error) { return grpcframing.New() }),
			argos.CodecFunc(func() (codec.Codec, error) { return protobuf.New(), nil })
	}
	trFn, frFn, cdFn := mkAxes()
	srv := server.New(
		argos.WithConfig(cfg),
		argos.WithService(reflection.ServiceV1,
			argos.ServiceTransport(trFn), argos.ServiceFraming(frFn), argos.ServiceCodec(cdFn),
			argos.ServiceListenAddress(cfg.ListenAddress),
		),
	)
	files := new(protoregistry.Files)
	if err := files.RegisterFile(testpb.File_echo_proto); err != nil {
		t.Fatal(err)
	}
	if err := reflection.Register(srv, reflection.Options{Services: services, Files: files}); err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Run(context.Background()) }()
	select {
	case <-bound:
	case <-time.After(3 * time.Second):
		t.Fatal("server not ready")
	}
	for i := 0; i < 100 && srvTr.Addr() == nil; i++ {
		time.Sleep(5 * time.Millisecond)
	}
	if srvTr.Addr() == nil {
		t.Fatal("no listen addr")
	}
	t.Cleanup(func() { _ = srv.Close() })
	return srvTr.Addr().String()
}

func TestListServicesV1(t *testing.T) {
	const echoSvc = "echo.v1.Echo"
	addr := startReflection(t, []string{echoSvc, reflection.ServiceV1})

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

package http2

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/argos-io/argos"
	protobufcodec "github.com/argos-io/argos/codec/protobuf"
	"github.com/argos-io/argos/errs"
	echov1 "github.com/argos-io/argos/example/echo"
	"github.com/argos-io/argos/internal/statusmap"
)

type echoServer struct{}

func (*echoServer) Echo(
	_ context.Context,
	request *echov1.EchoRequest,
) (*echov1.EchoResponse, error) {
	return &echov1.EchoResponse{Msg: "hello " + request.GetMsg()}, nil
}

func (*echoServer) Watch(
	_ context.Context,
	request *echov1.WatchRequest,
	stream echov1.EchoService_WatchServer,
) error {
	return stream.Send(&echov1.Event{Msg: "hello " + request.GetMsg()})
}

func TestGRPCStatusMapping(t *testing.T) {
	tests := []struct {
		code errs.Code
		want uint32
	}{
		{errs.NotFound, 5},
		{errs.Unauthenticated, 16},
		{errs.Code(1001), 2},
	}
	for _, tc := range tests {
		if got := statusmap.GRPCStatus(tc.code); got != tc.want {
			t.Fatalf("GRPCStatus(%v) = %d, want %d", tc.code, got, tc.want)
		}
	}
	if statusmap.FromGRPCStatus(99) != errs.Unknown {
		t.Fatal("unknown grpc status must map to Unknown")
	}
}

func startEcho(
	t *testing.T,
	opts ...argos.Option,
) *channel {
	t.Helper()
	tr := New().(*channel)
	server := argos.NewServer()
	service := server.NewService(append([]argos.Option{
		argos.WithTransport(tr),
		argos.WithListenAddress("127.0.0.1:0"),
		argos.WithCodec(protobufcodec.New()),
	}, opts...)...)
	echov1.RegisterEchoService(service, &echoServer{})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Run(ctx) }()
	waitBound(tr)
	t.Cleanup(func() {
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Errorf("server.Run: %v", err)
		}
	})
	return tr
}

func TestEchoRoundTrip(t *testing.T) {
	tr := startEcho(t)
	client := echov1.NewEchoServiceClient(
		argos.WithTransport(tr),
		argos.WithListenAddress("127.0.0.1:0"),
		argos.WithCodec(protobufcodec.New()),
	)
	response, err := client.Echo(context.Background(), &echov1.EchoRequest{Msg: "http2"})
	if err != nil {
		t.Fatalf("Echo: %v", err)
	}
	if got, want := response.GetMsg(), "hello http2"; got != want {
		t.Fatalf("response = %q, want %q", got, want)
	}
}

func TestFilterUnauthenticated(t *testing.T) {
	deny := func(
		_ context.Context,
		_ string,
		_ argos.Stream,
		_ argos.Handler,
	) error {
		return argos.Error(argos.Unauthenticated, "no token")
	}
	tr := startEcho(t, argos.WithFilter(deny))

	client := echov1.NewEchoServiceClient(
		argos.WithTransport(tr),
		argos.WithListenAddress("127.0.0.1:0"),
		argos.WithCodec(protobufcodec.New()),
	)
	_, err := client.Echo(context.Background(), &echov1.EchoRequest{Msg: "http2"})
	if err == nil {
		t.Fatal("Echo succeeded, want Unauthenticated")
	}
	if got := argos.CodeOf(err); got != argos.Unauthenticated {
		t.Fatalf("client code = %d (err %v), want Unauthenticated", got, err)
	}

	f, err := tr.Open(context.Background(), "echo.v1.EchoService/Echo")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	writer, err := f.Send()
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if _, writeErr := writer.Write([]byte{0x0a, 0x03, 0x68, 0x69, 0x21}); writeErr != nil {
		t.Fatalf("Write: %v", writeErr)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
	if err := f.CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}
	_, recvErr := f.Recv()
	if recvErr == nil || argos.CodeOf(recvErr) != argos.Unauthenticated {
		t.Fatalf("Recv = %v, want Unauthenticated", recvErr)
	}
}

func waitBound(tr *channel) {
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if tr.Addr() != nil {
			return
		}
		time.Sleep(time.Millisecond)
	}
	panic("transport did not start listening")
}

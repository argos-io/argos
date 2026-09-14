package http2

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/argos-io/argos"
	"github.com/argos-io/argos/filter"
	"github.com/argos-io/argos/server"
	"github.com/argos-io/argos/stream"

	protobufcodec "github.com/argos-io/argos/codec/protobuf"
	"github.com/argos-io/argos/errs"
	echov1 "github.com/argos-io/argos/example/echo"
	"github.com/argos-io/argos/internal/statusmap"
	"github.com/argos-io/argos/metadata"
	"github.com/argos-io/argos/transport"
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

func TestTrailerErrorUnescapesMessage(t *testing.T) {
	err := trailerError(&http.Response{
		Trailer: http.Header{
			trailerStatus:  []string{"3"},
			trailerMessage: []string{"bad%20line%0A%22quote%22"},
		},
	})
	if got, want := errs.CodeOf(err), errs.InvalidArgument; got != want {
		t.Fatalf("code = %d, want %d", got, want)
	}
	if got, want := err.Error(), "bad line\n\"quote\""; got != want {
		t.Fatalf("message = %q, want %q", got, want)
	}
}

func TestTrailerErrorRequiresStatus(t *testing.T) {
	for _, resp := range []*http.Response{
		nil,
		{},
	} {
		err := trailerError(resp)
		if err == nil || err.Error() != "http2: missing grpc-status trailer" {
			t.Fatalf("trailerError(%v) = %v, want missing-status error", resp, err)
		}
	}
}

func TestMetadataLimit(t *testing.T) {
	if err := validateMetadata(metadata.Metadata{"x": {strings.Repeat("v", int(transport.DefaultMaxMetadataSize))}}); err == nil {
		t.Fatal("validateMetadata accepted oversized metadata")
	}
	values := make([]string, 2)
	for i := range values {
		values[i] = strings.Repeat("v", int(transport.DefaultMaxMetadataSize/2))
	}
	if err := validateMetadata(metadata.Metadata{"repeated": values}); err == nil {
		t.Fatal("validateMetadata accepted oversized repeated metadata")
	}
}

func TestOversizedMetadataDoesNotBlockSend(t *testing.T) {
	ctx := metadata.With(context.Background(), metadata.Metadata{
		"x": {strings.Repeat("v", int(transport.DefaultMaxMetadataSize))},
	})
	f := &clientFramer{
		ctx:            ctx,
		endpoint:       "http://127.0.0.1:1/unused",
		metadata:       metadata.FromContext(ctx),
		client:         &http.Client{},
		ready:          make(chan struct{}),
		maxMessageSize: transport.DefaultMaxMessageSize,
	}
	t.Cleanup(func() { _ = f.Close() })

	writer, err := f.Send()
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	writeDone := make(chan error, 1)
	go func() {
		if _, writeErr := writer.Write([]byte("payload")); writeErr != nil {
			writeDone <- writeErr
			return
		}
		writeDone <- writer.Close()
	}()
	select {
	case writeErr := <-writeDone:
		if writeErr == nil {
			t.Fatal("Write succeeded despite oversized metadata")
		}
	case <-time.After(time.Second):
		t.Fatal("Write blocked after the request failed")
	}
}

func startEcho(
	t *testing.T,
	opts ...argos.Option,
) *channel {
	t.Helper()
	tr := New().(*channel)
	server := server.New()
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

func TestClientTransportIsReusedAcrossCalls(t *testing.T) {
	tr := New().(*channel)
	first, err := tr.Open(context.Background(), "svc/First", transport.WithDialAddress("127.0.0.1:1"))
	if err != nil {
		t.Fatalf("first Open: %v", err)
	}
	defer first.Close()
	second, err := tr.Open(context.Background(), "svc/Second", transport.WithDialAddress("127.0.0.1:1"))
	if err != nil {
		t.Fatalf("second Open: %v", err)
	}
	defer second.Close()
	if first.(*clientFramer).client != second.(*clientFramer).client {
		t.Fatal("Open created separate HTTP clients; connection pooling is disabled")
	}
}

func TestRecvAfterCloseBeforeSendReturnsClosed(t *testing.T) {
	f := &clientFramer{
		ctx:            context.Background(),
		ready:          make(chan struct{}),
		maxMessageSize: transport.DefaultMaxMessageSize,
	}
	if err := f.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := f.Recv(); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("Recv after Close = %v, want net.ErrClosed", err)
	}
}

func TestFilterUnauthenticated(t *testing.T) {
	deny := func(
		_ context.Context,
		_ string,
		_ stream.Stream,
		_ filter.Handler,
	) error {
		return errs.Error(errs.Unauthenticated, "no token")
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
	if got := errs.CodeOf(err); got != errs.Unauthenticated {
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
	if recvErr == nil || errs.CodeOf(recvErr) != errs.Unauthenticated {
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

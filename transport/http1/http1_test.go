package http1

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/argos-io/argos"
	jsoncodec "github.com/argos-io/argos/codec/json"
	echov1 "github.com/argos-io/argos/example/echo"
	"github.com/argos-io/argos/transport"
)

type echoServer struct {
	watchSecondSend chan error
}

func (*echoServer) Echo(
	ctx context.Context,
	request *echov1.EchoRequest,
) (*echov1.EchoResponse, error) {
	prefix := ""
	if values := argos.MetadataFromContext(ctx)["authorization"]; len(values) != 0 {
		prefix = values[0] + " "
	}
	return &echov1.EchoResponse{Msg: prefix + "hello " + request.GetMsg()}, nil
}

func (s *echoServer) Watch(
	_ context.Context,
	request *echov1.WatchRequest,
	stream echov1.EchoService_WatchServer,
) error {
	if err := stream.Send(&echov1.Event{Msg: "hello " + request.GetMsg()}); err != nil {
		return err
	}
	err := stream.Send(&echov1.Event{Msg: "second"})
	s.watchSecondSend <- err
	return err
}

func startEcho(
	t *testing.T,
	opts ...argos.Option,
) (*channel, *echoServer) {
	t.Helper()
	tr := New().(*channel)
	impl := &echoServer{watchSecondSend: make(chan error, 1)}
	server := argos.NewServer()
	service := server.NewService(append([]argos.Option{
		argos.WithTransport(tr),
		argos.WithListenAddress("127.0.0.1:0"),
		argos.WithCodec(jsoncodec.New()),
	}, opts...)...)
	echov1.RegisterEchoService(service, impl)

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
	return tr, impl
}

func TestEchoRoundTripWithMetadata(t *testing.T) {
	tr, _ := startEcho(t)
	client := echov1.NewEchoServiceClient(
		argos.WithTransport(tr),
		argos.WithListenAddress("127.0.0.1:0"),
		argos.WithCodec(jsoncodec.New()),
	)
	ctx := argos.WithMetadata(context.Background(), argos.Metadata{
		"authorization": {"Bearer token"},
	})
	response, err := client.Echo(ctx, &echov1.EchoRequest{Msg: "http"})
	if err != nil {
		t.Fatalf("Echo: %v", err)
	}
	if got, want := response.GetMsg(), "Bearer token hello http"; got != want {
		t.Fatalf("response = %q, want %q", got, want)
	}
}

func TestWatchSecondSendFails(t *testing.T) {
	tr, impl := startEcho(t)
	client := echov1.NewEchoServiceClient(
		argos.WithTransport(tr),
		argos.WithListenAddress("127.0.0.1:0"),
		argos.WithCodec(jsoncodec.New()),
	)
	stream := client.Watch(context.Background(), &echov1.WatchRequest{Msg: "http"})
	event, err := stream.Recv()
	if err != nil {
		t.Fatalf("first Recv: %v", err)
	}
	if got, want := event.GetMsg(), "hello http"; got != want {
		t.Fatalf("event = %q, want %q", got, want)
	}
	if err := <-impl.watchSecondSend; err == nil {
		t.Fatal("second server Send succeeded, want error")
	}
	if _, err := stream.Recv(); err == nil {
		t.Fatal("second client Recv succeeded, want error")
	}
}

func TestSecondServerRecvFails(t *testing.T) {
	tr := New().(*channel)
	secondRecv := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- tr.ListenAndServe(ctx, func(
			_ context.Context,
			_ string,
			f argos.Framer,
		) error {
			if _, err := f.Recv(); err != nil {
				return err
			}
			_, err := f.Recv()
			secondRecv <- err
			return err
		}, transport.WithListenAddress("127.0.0.1:0"))
	}()
	waitBound(tr)
	t.Cleanup(func() {
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Errorf("ListenAndServe: %v", err)
		}
	})

	response, err := http.Post(
		"http://"+transport.DialableAddress(tr.Addr())+"/raw/Echo",
		"application/json",
		bytes.NewBufferString("{}"),
	)
	if err != nil {
		t.Fatalf("Post: %v", err)
	}
	defer response.Body.Close()
	if err := <-secondRecv; err == nil {
		t.Fatal("second server Recv succeeded, want error")
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
	tr, _ := startEcho(t, argos.WithFilter(deny))

	response, err := http.Post(
		"http://"+transport.DialableAddress(tr.Addr())+"/echo.v1.EchoService/Echo",
		"application/json",
		bytes.NewBufferString(`{"msg":"http"}`),
	)
	if err != nil {
		t.Fatalf("Post: %v", err)
	}
	defer response.Body.Close()
	if got, want := response.StatusCode, http.StatusUnauthorized; got != want {
		t.Fatalf("status = %d, want %d", got, want)
	}
	var body errorBody
	if err := json.NewDecoder(response.Body).Decode(&body); err != nil {
		t.Fatalf("decode error body: %v", err)
	}
	if body.Code != argos.Unauthenticated || body.Message != "no token" {
		t.Fatalf("error body = %+v, want code 2 and message %q", body, "no token")
	}

	client := echov1.NewEchoServiceClient(
		argos.WithTransport(tr),
		argos.WithListenAddress("127.0.0.1:0"),
		argos.WithCodec(jsoncodec.New()),
	)
	_, err = client.Echo(context.Background(), &echov1.EchoRequest{Msg: "http"})
	if got := argos.CodeOf(err); got != argos.Unauthenticated {
		t.Fatalf("client code = %d (err %v), want Unauthenticated", got, err)
	}
}

func TestClientSecondSendFails(t *testing.T) {
	tr, _ := startEcho(t)
	f, err := tr.Open(context.Background(), "echo.v1.EchoService/Echo")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	writer, err := f.Send()
	if err != nil {
		t.Fatalf("first Send: %v", err)
	}
	if _, err := writer.Write([]byte(`{"msg":"http"}`)); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := f.Send(); err == nil {
		t.Fatal("second client Send succeeded, want error")
	}
	reader, err := f.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if _, err := io.ReadAll(reader); err != nil {
		t.Fatalf("ReadAll: %v", err)
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

package ws

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/argos-io/argos/filter"
	"github.com/argos-io/argos"
	"github.com/argos-io/argos/server"
	"github.com/argos-io/argos/stream"

	"github.com/argos-io/argos/codec/protobuf"
	"github.com/argos-io/argos/errs"
	echov1 "github.com/argos-io/argos/example/echo"
	"github.com/argos-io/argos/internal/wire"
	"github.com/argos-io/argos/metadata"
	"github.com/argos-io/argos/transport"
	"github.com/coder/websocket"
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

func startEcho(t *testing.T, addr string, opts ...argos.Option) *channel {
	t.Helper()
	tr := New().(*channel)
	server := server.New()
	service := server.NewService(append([]argos.Option{
		argos.WithTransport(tr),
		argos.WithListenAddress("127.0.0.1:0"),
		argos.WithCodec(protobuf.New()),
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

func TestEchoBinaryEnvelope(t *testing.T) {
	tr := startEcho(t, ":0")
	client := echov1.NewEchoServiceClient(
		argos.WithTransport(tr),
		argos.WithListenAddress("127.0.0.1:0"),
		argos.WithCodec(protobuf.New()),
	)
	response, err := client.Echo(
		context.Background(),
		&echov1.EchoRequest{Msg: "ws"},
	)
	if err != nil {
		t.Fatalf("Echo: %v", err)
	}
	if got, want := response.GetMsg(), "hello ws"; got != want {
		t.Fatalf("response = %q, want %q", got, want)
	}
}

func TestTextFrameReturnsError(t *testing.T) {
	tr := New().(*channel)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- tr.ListenAndServe(ctx, func(
			_ context.Context,
			_ string,
			_ transport.Framer,
		) error {
			return nil
		}, transport.WithListenAddress("127.0.0.1:0"))
	}()
	waitBound(tr)
	t.Cleanup(func() {
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Errorf("ListenAndServe: %v", err)
		}
	})

	callCtx, callCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer callCancel()
	conn, _, err := websocket.Dial(callCtx, "ws://"+transport.DialableAddress(tr.Addr())+"/", nil)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	if err := conn.Write(callCtx, websocket.MessageText, []byte("not an envelope")); err != nil {
		t.Fatalf("Write text: %v", err)
	}

	msgType, blob, err := conn.Read(callCtx)
	if err != nil {
		t.Fatalf("Read status: %v", err)
	}
	if msgType != websocket.MessageBinary {
		t.Fatalf("status message type = %v, want binary", msgType)
	}
	env, err := wire.UnmarshalEnvelope(blob)
	if err != nil {
		t.Fatalf("UnmarshalEnvelope: %v", err)
	}
	if env.Flags&wire.FlagStatus == 0 {
		t.Fatalf("expected status frame, got flags %08b", env.Flags)
	}
	code, description, err := wire.UnmarshalStatus(env.Payload)
	if err != nil {
		t.Fatalf("UnmarshalStatus: %v", err)
	}
	if errs.Code(code) != errs.Unimplemented {
		t.Fatalf("status code = %d, want Unimplemented", code)
	}
	if description == "" {
		t.Fatal("expected non-empty Unimplemented description")
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

func TestWatchRoundTrip(t *testing.T) {
	tr := startEcho(t, "127.0.0.1:0")
	client := echov1.NewEchoServiceClient(
		argos.WithTransport(tr),
		argos.WithListenAddress("127.0.0.1:0"),
		argos.WithCodec(protobuf.New()),
	)
	stream := client.Watch(context.Background(), &echov1.WatchRequest{Msg: "ws"})
	event, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if got, want := event.GetMsg(), "hello ws"; got != want {
		t.Fatalf("event = %q, want %q", got, want)
	}
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("Recv after last event = %v, want io.EOF", err)
	}
}

func TestFilterShortCircuitStatus(t *testing.T) {
	deny := func(context.Context, string, stream.Stream, filter.Handler) error {
		return errs.Error(errs.Unauthenticated, "no token")
	}
	tr := startEcho(t, "127.0.0.1:0", argos.WithFilter(deny))
	client := echov1.NewEchoServiceClient(
		argos.WithTransport(tr),
		argos.WithListenAddress("127.0.0.1:0"),
		argos.WithCodec(protobuf.New()),
	)
	_, err := client.Echo(context.Background(), &echov1.EchoRequest{Msg: "ws"})
	if errs.CodeOf(err) != errs.Unauthenticated {
		t.Fatalf("code = %v, want Unauthenticated", err)
	}
}

func TestTextFramerMethods(t *testing.T) {
	var tf textFramer
	if _, err := tf.Recv(); errs.CodeOf(err) != errs.Unimplemented {
		t.Fatalf("Recv = %v", err)
	}
	if _, err := tf.Send(); errs.CodeOf(err) != errs.Unimplemented {
		t.Fatalf("Send = %v", err)
	}
	if err := tf.CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}
}

func TestEncodeDecodeMetadata(t *testing.T) {
	md := metadata.Metadata{
		"token": []string{"abc"},
		"x":     []string{"1", "2"},
	}
	blob, err := encodeMetadata(md)
	if err != nil {
		t.Fatalf("encodeMetadata: %v", err)
	}
	got, err := decodeMetadata(blob)
	if err != nil {
		t.Fatalf("decodeMetadata: %v", err)
	}
	if got["token"][0] != "abc" || len(got["x"]) != 2 {
		t.Fatalf("metadata = %v", got)
	}
}

func TestDecodeMetadataTruncated(t *testing.T) {
	if _, err := decodeMetadata([]byte{0, 5}); err == nil {
		t.Fatal("expected truncated metadata error")
	}
}

func TestEncodeMetadataTooLarge(t *testing.T) {
	key := strings.Repeat("a", 65536)
	_, err := encodeMetadata(metadata.Metadata{key: []string{"v"}})
	if err == nil {
		t.Fatal("expected metadata size error")
	}
}

func TestSendAfterCloseSend(t *testing.T) {
	tr := startEcho(t, "127.0.0.1:0")
	f, err := tr.Open(context.Background(), "echo.v1.EchoService/Echo")
	if err != nil {
		t.Fatal(err)
	}
	if err := f.CloseSend(); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Send(); err == nil {
		t.Fatal("expected send-after-CloseSend error")
	}
}

func TestHandlerErrorReturnsStatus(t *testing.T) {
	tr := New().(*channel)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- tr.ListenAndServe(ctx, func(context.Context, string, transport.Framer) error {
			return errs.Error(errs.NotFound, "missing")
		}, transport.WithListenAddress("127.0.0.1:0"))
	}()
	waitBound(tr)
	t.Cleanup(func() {
		cancel()
		<-done
	})

	callCtx, callCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer callCancel()
	conn, _, err := websocket.Dial(callCtx, "ws://"+transport.DialableAddress(tr.Addr())+"/", nil)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	req, err := wire.MarshalEnvelope(wire.Envelope{Method: "svc/M", Payload: []byte("x")})
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Write(callCtx, websocket.MessageBinary, req); err != nil {
		t.Fatal(err)
	}

	msgType, blob, err := conn.Read(callCtx)
	if err != nil {
		t.Fatal(err)
	}
	if msgType != websocket.MessageBinary {
		t.Fatalf("msg type = %v", msgType)
	}
	env, err := wire.UnmarshalEnvelope(blob)
	if err != nil {
		t.Fatal(err)
	}
	if env.Flags&wire.FlagStatus == 0 {
		t.Fatal("expected status frame")
	}
	code, desc, err := wire.UnmarshalStatus(env.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if errs.Code(code) != errs.NotFound || desc != "missing" {
		t.Fatalf("status = (%d, %q)", code, desc)
	}
}

func TestRawEnvelopeWS(t *testing.T) {
	tr := New().(*channel)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	gotCall := make(chan struct{}, 1)
	go func() {
		done <- tr.ListenAndServe(ctx, func(ctx context.Context, method string, f transport.Framer) error {
			if method != "raw/Echo" {
				t.Errorf("method = %q", method)
			}
			if got := metadata.FromContext(ctx)["token"]; len(got) != 1 || got[0] != "abc" {
				t.Errorf("metadata = %v", got)
			}
			reader, err := f.Recv()
			if err != nil {
				return err
			}
			payload, err := io.ReadAll(reader)
			if err != nil {
				return err
			}
			if !bytes.Equal(payload, []byte("ping")) {
				t.Errorf("payload = %q", payload)
			}
			writer, err := f.Send()
			if err != nil {
				return err
			}
			if _, err := writer.Write([]byte("pong")); err != nil {
				return err
			}
			if err := writer.Close(); err != nil {
				return err
			}
			gotCall <- struct{}{}
			return nil
		}, transport.WithListenAddress("127.0.0.1:0"))
	}()
	waitBound(tr)
	t.Cleanup(func() {
		cancel()
		<-done
	})

	callCtx, callCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer callCancel()
	conn, _, err := websocket.Dial(callCtx, "ws://"+transport.DialableAddress(tr.Addr())+"/", nil)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")

	md := []byte{0, 5, 't', 'o', 'k', 'e', 'n', 0, 3, 'a', 'b', 'c'}
	req, err := wire.MarshalEnvelope(wire.Envelope{Method: "raw/Echo", Metadata: md, Payload: []byte("ping")})
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Write(callCtx, websocket.MessageBinary, req); err != nil {
		t.Fatal(err)
	}
	end, err := wire.MarshalEnvelope(wire.Envelope{Flags: wire.FlagEnd})
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Write(callCtx, websocket.MessageBinary, end); err != nil {
		t.Fatal(err)
	}

	for {
		msgType, blob, err := conn.Read(callCtx)
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		if msgType != websocket.MessageBinary {
			continue
		}
		env, err := wire.UnmarshalEnvelope(blob)
		if err != nil {
			t.Fatal(err)
		}
		if env.Flags&wire.FlagStatus != 0 {
			code, _, err := wire.UnmarshalStatus(env.Payload)
			if err != nil {
				t.Fatal(err)
			}
			if errs.Code(code) != errs.OK {
				t.Fatalf("status code = %d", code)
			}
			break
		}
		if len(env.Payload) > 0 && !bytes.Equal(env.Payload, []byte("pong")) {
			t.Fatalf("payload = %q", env.Payload)
		}
	}
	<-gotCall
}

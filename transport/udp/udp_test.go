package udp

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/argos-io/argos/filter"
	"github.com/argos-io/argos/option"
	"github.com/argos-io/argos/server"
	"github.com/argos-io/argos/stream"

	"github.com/argos-io/argos/codec/protobuf"
	"github.com/argos-io/argos/errs"
	echov1 "github.com/argos-io/argos/example/echo"
	"github.com/argos-io/argos/internal/wire"
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

func startEcho(t *testing.T, addr string, opts ...option.Option) *channel {
	t.Helper()
	tr := New().(*channel)
	server := server.New()
	service := server.NewService(append([]option.Option{
		option.WithTransport(tr),
		option.WithListenAddress("127.0.0.1:0"),
		option.WithCodec(protobuf.New()),
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
	tr := startEcho(t, ":0")
	client := echov1.NewEchoServiceClient(
		option.WithTransport(tr),
		option.WithListenAddress("127.0.0.1:0"),
		option.WithCodec(protobuf.New()),
	)
	response, err := client.Echo(
		context.Background(),
		&echov1.EchoRequest{Msg: "udp"},
	)
	if err != nil {
		t.Fatalf("Echo: %v", err)
	}
	if got, want := response.GetMsg(), "hello udp"; got != want {
		t.Fatalf("response = %q, want %q", got, want)
	}
}

func TestSecondSendFails(t *testing.T) {
	tr := startEcho(t, "127.0.0.1:0")
	f, err := tr.Open(context.Background(), "echo.v1.EchoService/Echo")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	writer, err := f.Send()
	if err != nil {
		t.Fatalf("first Send: %v", err)
	}
	if _, err := writer.Write([]byte{0x0a, 0x03, 'u', 'd', 'p'}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := f.Send(); err == nil {
		t.Fatal("second Send succeeded, want error")
	}
	if err := f.CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
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

func TestFilterShortCircuitStatus(t *testing.T) {
	deny := func(context.Context, string, stream.Stream, filter.Handler) error {
		return errs.Error(errs.Unauthenticated, "no token")
	}
	tr := startEcho(t, "127.0.0.1:0", option.WithFilter(deny))
	client := echov1.NewEchoServiceClient(
		option.WithTransport(tr),
		option.WithListenAddress("127.0.0.1:0"),
		option.WithCodec(protobuf.New()),
	)
	_, err := client.Echo(context.Background(), &echov1.EchoRequest{Msg: "udp"})
	if errs.CodeOf(err) != errs.Unauthenticated {
		t.Fatalf("code = %v, want Unauthenticated", err)
	}
}

func TestEncodeDecodeMetadata(t *testing.T) {
	md := metadata.Metadata{"token": []string{"abc"}}
	blob, err := encodeMetadata(md)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeMetadata(blob)
	if err != nil {
		t.Fatal(err)
	}
	if got["token"][0] != "abc" {
		t.Fatalf("metadata = %v", got)
	}
}

func TestDecodeMetadataTruncated(t *testing.T) {
	if _, err := decodeMetadata([]byte{0, 3}); err == nil {
		t.Fatal("expected error")
	}
}

func TestEncodeMetadataTooLarge(t *testing.T) {
	key := strings.Repeat("b", 65536)
	_, err := encodeMetadata(metadata.Metadata{key: []string{"v"}})
	if err == nil {
		t.Fatal("expected metadata size error")
	}
}

func TestStatusErrorOKIsEOF(t *testing.T) {
	err := statusError(wire.MarshalStatus(uint32(errs.OK), ""))
	if !errors.Is(err, io.EOF) {
		t.Fatalf("error = %v, want EOF", err)
	}
}

func TestParseResponseMissingStatus(t *testing.T) {
	req, err := wire.MarshalEnvelope(wire.Envelope{Payload: []byte("x")})
	if err != nil {
		t.Fatal(err)
	}
	end, err := wire.MarshalEnvelope(wire.Envelope{Flags: wire.FlagEnd})
	if err != nil {
		t.Fatal(err)
	}
	dg, err := marshalDatagram(req, end)
	if err != nil {
		t.Fatal(err)
	}
	f := &framer{initiator: true}
	if err := f.parseResponse(dg); err == nil {
		t.Fatal("expected missing status error")
	}
}

func TestRecvBeforeRequest(t *testing.T) {
	tr := startEcho(t, "127.0.0.1:0")
	f, err := tr.Open(context.Background(), "echo.v1.EchoService/Echo")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Recv(); err == nil {
		t.Fatal("expected receive-before-request error")
	}
}

func TestReadFrameTruncated(t *testing.T) {
	if _, err := readFrame(bytes.NewReader([]byte{0, 0, 0, 5, 1, 2})); err == nil {
		t.Fatal("expected truncated frame error")
	}
}

func TestHandlerErrorReturnsStatus(t *testing.T) {
	tr := New().(*channel)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- tr.ListenAndServe(ctx, func(context.Context, string, transport.Framer) error {
			return errs.Error(errs.InvalidArgument, "bad input")
		}, transport.WithListenNetwork("udp"), transport.WithListenAddress("127.0.0.1:0"))
	}()
	waitBound(tr)
	t.Cleanup(func() {
		cancel()
		<-done
	})

	conn, err := net.Dial("udp", transport.DialableAddress(tr.Addr()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	req, err := wire.MarshalEnvelope(wire.Envelope{Method: "svc/M", Payload: []byte("x")})
	if err != nil {
		t.Fatal(err)
	}
	end, err := wire.MarshalEnvelope(wire.Envelope{Flags: wire.FlagEnd})
	if err != nil {
		t.Fatal(err)
	}
	dg, err := marshalDatagram(req, end)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write(dg); err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, 65507)
	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	frames, err := readDatagram(buf[:n])
	if err != nil {
		t.Fatal(err)
	}
	var status *wire.Envelope
	for _, blob := range frames {
		env, err := wire.UnmarshalEnvelope(blob)
		if err != nil {
			t.Fatal(err)
		}
		if env.Flags&wire.FlagStatus != 0 {
			status = &env
		}
	}
	if status == nil {
		t.Fatal("missing status frame")
	}
	code, desc, err := wire.UnmarshalStatus(status.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if errs.Code(code) != errs.InvalidArgument || desc != "bad input" {
		t.Fatalf("status = (%d, %q)", code, desc)
	}
}

func TestRawEnvelopeUDP(t *testing.T) {
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
		}, transport.WithListenNetwork("udp"), transport.WithListenAddress("127.0.0.1:0"))
	}()
	waitBound(tr)
	t.Cleanup(func() {
		cancel()
		<-done
	})

	conn, err := net.Dial("udp", transport.DialableAddress(tr.Addr()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	md := []byte{0, 5, 't', 'o', 'k', 'e', 'n', 0, 3, 'a', 'b', 'c'}
	req, err := wire.MarshalEnvelope(wire.Envelope{Method: "raw/Echo", Metadata: md, Payload: []byte("ping")})
	if err != nil {
		t.Fatal(err)
	}
	end, err := wire.MarshalEnvelope(wire.Envelope{Flags: wire.FlagEnd})
	if err != nil {
		t.Fatal(err)
	}
	dg, err := marshalDatagram(req, end)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write(dg); err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, 65507)
	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatal(err)
	}
	frames, err := readDatagram(buf[:n])
	if err != nil {
		t.Fatal(err)
	}
	var payload []byte
	for _, blob := range frames {
		env, err := wire.UnmarshalEnvelope(blob)
		if err != nil {
			t.Fatal(err)
		}
		if len(env.Payload) > 0 && env.Flags&wire.FlagStatus == 0 {
			payload = env.Payload
		}
	}
	if !bytes.Equal(payload, []byte("pong")) {
		t.Fatalf("payload = %q", payload)
	}
	<-gotCall
}

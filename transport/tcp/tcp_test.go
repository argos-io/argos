package tcp

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/argos-io/argos"
	"github.com/argos-io/argos/filter"
	"github.com/argos-io/argos/metadata"
	"github.com/argos-io/argos/server"
	"github.com/argos-io/argos/stream"

	"github.com/argos-io/argos/codec/protobuf"
	"github.com/argos-io/argos/errs"
	echov1 "github.com/argos-io/argos/example/echo"
	"github.com/argos-io/argos/internal/wire"
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

func TestEchoRoundTrip(t *testing.T) {
	tr := startEcho(t, ":0")
	client := echov1.NewEchoServiceClient(
		argos.WithTransport(tr),
		argos.WithListenAddress("127.0.0.1:0"),
		argos.WithCodec(protobuf.New()),
	)
	response, err := client.Echo(
		context.Background(),
		&echov1.EchoRequest{Msg: "tcp"},
	)
	if err != nil {
		t.Fatalf("Echo: %v", err)
	}
	if got, want := response.GetMsg(), "hello tcp"; got != want {
		t.Fatalf("response = %q, want %q", got, want)
	}
}

func TestWatchRoundTrip(t *testing.T) {
	tr := startEcho(t, "127.0.0.1:0")
	client := echov1.NewEchoServiceClient(
		argos.WithTransport(tr),
		argos.WithListenAddress("127.0.0.1:0"),
		argos.WithCodec(protobuf.New()),
	)
	stream := client.Watch(context.Background(), &echov1.WatchRequest{Msg: "tcp"})
	event, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if got, want := event.GetMsg(), "hello tcp"; got != want {
		t.Fatalf("event = %q, want %q", got, want)
	}
	if _, err := stream.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("Recv after last event = %v, want io.EOF", err)
	}
}

func TestContextCancellationClosesBlockedRecv(t *testing.T) {
	conn, peer := net.Pipe()
	defer peer.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	f := &framer{
		conn:           conn,
		ctx:            ctx,
		initiator:      true,
		frames:         make(chan wire.Envelope, 1),
		done:           make(chan struct{}),
		maxMessageSize: transport.DefaultMaxMessageSize,
		maxFrameSize:   transport.ResolveMaxFrameSize(transport.DefaultMaxMessageSize),
	}
	f.watchContext()
	go f.readAhead()
	defer f.Close()

	result := make(chan error, 1)
	go func() {
		_, err := f.Recv()
		result <- err
	}()
	cancel()

	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Recv error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked Recv was not interrupted by context cancellation")
	}
}

func TestMissingStatusIsProtocolError(t *testing.T) {
	conn, peer := net.Pipe()
	f := &framer{
		conn:           conn,
		ctx:            context.Background(),
		frames:         make(chan wire.Envelope, 1),
		done:           make(chan struct{}),
		maxMessageSize: transport.DefaultMaxMessageSize,
		maxFrameSize:   transport.ResolveMaxFrameSize(transport.DefaultMaxMessageSize),
		initiator:      true,
	}
	go f.readAhead()
	if err := peer.Close(); err != nil {
		t.Fatalf("peer.Close: %v", err)
	}
	defer f.Close()

	_, err := f.Recv()
	if err == nil || err.Error() != "tcp: response ended before status trailer" {
		t.Fatalf("Recv = %v, want missing-status protocol error", err)
	}
}

// A filter that short-circuits never closes the send side, so the status trailer
// is the only frame the client gets. It must surface as the filter's Code and not
// as a codec error.
func TestFilterShortCircuitStatus(t *testing.T) {
	deny := func(
		_ context.Context,
		_ string,
		_ stream.Stream,
		_ filter.Handler,
	) error {
		return errs.Error(errs.Unauthenticated, "no token")
	}
	tr := startEcho(t, "127.0.0.1:0", argos.WithFilter(deny))

	client := echov1.NewEchoServiceClient(
		argos.WithTransport(tr),
		argos.WithListenAddress("127.0.0.1:0"),
		argos.WithCodec(protobuf.New()),
	)
	_, err := client.Echo(context.Background(), &echov1.EchoRequest{Msg: "tcp"})
	if got := errs.CodeOf(err); got != errs.Unauthenticated {
		t.Fatalf("code = %d (err %v), want Unauthenticated", got, err)
	}
	if got, want := err.Error(), "no token"; got != want {
		t.Fatalf("description = %q, want %q", got, want)
	}
}

func TestRawEnvelope(t *testing.T) {
	tr := New().(*channel)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	gotCall := make(chan struct{}, 1)
	go func() {
		done <- tr.ListenAndServe(ctx, func(
			ctx context.Context,
			method string,
			f transport.Framer,
		) error {
			if method != "raw/Echo" {
				t.Errorf("method = %q, want raw/Echo", method)
			}
			if got := metadata.FromContext(ctx)["token"]; len(got) != 1 || got[0] != "abc" {
				t.Errorf("metadata token = %v, want [abc]", got)
			}
			reader, err := f.Recv()
			if err != nil {
				t.Errorf("Recv: %v", err)
				return err
			}
			payload, err := io.ReadAll(reader)
			if err != nil {
				t.Errorf("ReadAll: %v", err)
				return err
			}
			if !bytes.Equal(payload, []byte("ping")) {
				t.Errorf("payload = %q, want ping", payload)
			}
			if _, err := f.Recv(); !errors.Is(err, io.EOF) {
				t.Errorf("Recv after CloseSend = %v, want io.EOF", err)
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
			if err := f.CloseSend(); err != nil {
				return err
			}
			gotCall <- struct{}{}
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

	conn, err := net.Dial("tcp", transport.DialableAddress(tr.Addr()))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	metadata := []byte{
		0, 5, 't', 'o', 'k', 'e', 'n',
		0, 3, 'a', 'b', 'c',
	}
	request, err := wire.MarshalEnvelope(wire.Envelope{
		Method:   "raw/Echo",
		Metadata: metadata,
		Payload:  []byte("ping"),
	})
	if err != nil {
		t.Fatalf("MarshalEnvelope: %v", err)
	}
	if err := writeFrame(conn, request); err != nil {
		t.Fatalf("writeFrame: %v", err)
	}
	closeSend, err := wire.MarshalEnvelope(wire.Envelope{Flags: wire.FlagEnd})
	if err != nil {
		t.Fatalf("MarshalEnvelope: %v", err)
	}
	if err := writeFrame(conn, closeSend); err != nil {
		t.Fatalf("writeFrame: %v", err)
	}

	response := readEnvelope(t, conn)
	if !bytes.Equal(response.Payload, []byte("pong")) {
		t.Fatalf("response payload = %q, want pong", response.Payload)
	}
	end := readEnvelope(t, conn)
	if end.Flags != wire.FlagEnd {
		t.Fatalf("end flags = %08b, want only bit 0 set", end.Flags)
	}
	status := readEnvelope(t, conn)
	if status.Flags != wire.FlagStatus {
		t.Fatalf("status flags = %08b, want only bit 1 set", status.Flags)
	}
	code, description, err := wire.UnmarshalStatus(status.Payload)
	if err != nil {
		t.Fatalf("UnmarshalStatus: %v", err)
	}
	if code != uint32(errs.OK) || description != "" {
		t.Fatalf("status = (%d, %q), want (0, empty)", code, description)
	}
	<-gotCall
}

func TestAcceptsMetadataOnFirstEndFrame(t *testing.T) {
	conn, peer := net.Pipe()
	defer peer.Close()

	called := make(chan struct{}, 1)
	done := make(chan struct{})
	go func() {
		(&channel{}).serveConn(context.Background(), conn, func(ctx context.Context, _ string, f transport.Framer) error {
			if got := metadata.FromContext(ctx)["token"]; len(got) != 1 || got[0] != "abc" {
				t.Errorf("metadata token = %v, want [abc]", got)
			}
			if _, err := f.Recv(); !errors.Is(err, io.EOF) {
				t.Errorf("Recv = %v, want io.EOF", err)
			}
			called <- struct{}{}
			return nil
		}, transport.DefaultMaxMessageSize)
		close(done)
	}()

	first, err := wire.MarshalEnvelope(wire.Envelope{
		Method:   "raw/Method",
		Flags:    wire.FlagEnd,
		Metadata: []byte{0, 5, 't', 'o', 'k', 'e', 'n', 0, 3, 'a', 'b', 'c'},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := writeFrame(peer, first); err != nil {
		t.Fatal(err)
	}

	select {
	case <-called:
		if err := peer.Close(); err != nil {
			t.Fatalf("peer.Close: %v", err)
		}
	case <-done:
		t.Fatal("server returned before invoking onCall")
	case <-time.After(time.Second):
		t.Fatal("server did not accept the empty request")
	}
}

func TestRecvRejectsMetadataAfterFirstRequest(t *testing.T) {
	f := &framer{
		frames:         make(chan wire.Envelope, 1),
		done:           make(chan struct{}),
		maxMessageSize: transport.DefaultMaxMessageSize,
	}
	f.frames <- wire.Envelope{Metadata: []byte{1}, Payload: []byte("late")}
	if _, err := f.Recv(); err == nil {
		t.Fatal("Recv accepted metadata after the first request")
	}
}

func readEnvelope(t *testing.T, reader io.Reader) wire.Envelope {
	t.Helper()
	blob, err := readFrame(reader)
	if err != nil {
		t.Fatalf("readFrame: %v", err)
	}
	env, err := wire.UnmarshalEnvelope(blob)
	if err != nil {
		t.Fatalf("UnmarshalEnvelope: %v", err)
	}
	return env
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

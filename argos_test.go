package argos_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/argos-io/argos"
	"github.com/argos-io/argos/transport"
)

type jsonCodec struct{}

func (jsonCodec) Marshal(w io.Writer, v any) error {
	return json.NewEncoder(w).Encode(v)
}

func (jsonCodec) Unmarshal(r io.Reader, v any) error {
	return json.NewDecoder(r).Decode(v)
}

type recordingWriteCloser struct {
	bytes.Buffer
}

func (*recordingWriteCloser) Close() error { return nil }

type testFramer struct {
	recv            []byte
	sent            recordingWriteCloser
	closeSendCount  atomic.Int32
	dispatchDone    *atomic.Bool
	closedAfterCall atomic.Bool
}

func (f *testFramer) Recv() (io.Reader, error) {
	return bytes.NewReader(f.recv), nil
}

func (f *testFramer) Send() (io.WriteCloser, error) {
	return &f.sent, nil
}

func (f *testFramer) CloseSend() error {
	f.closeSendCount.Add(1)
	if f.dispatchDone != nil && f.dispatchDone.Load() {
		f.closedAfterCall.Store(true)
	}
	return nil
}

type loopbackCall struct {
	ctx    context.Context
	method string
	framer argos.Framer
	result chan error
}

type loopbackTransport struct {
	ready chan struct{}
	calls chan loopbackCall
	once  sync.Once
}

func newLoopbackTransport() *loopbackTransport {
	return &loopbackTransport{
		ready: make(chan struct{}),
		calls: make(chan loopbackCall),
	}
}

func (t *loopbackTransport) ListenAndServe(
	ctx context.Context,
	onCall func(context.Context, string, argos.Framer) error,
	opts ...argos.TransportServerOption,
) error {
	_ = opts
	t.once.Do(func() { close(t.ready) })
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case call := <-t.calls:
			call.result <- onCall(call.ctx, call.method, call.framer)
		}
	}
}

func (t *loopbackTransport) Open(_ context.Context, _ string, opts ...argos.TransportClientOption) (argos.Framer, error) {
	_ = opts
	panic("server loopback tests use call")
}

func (t *loopbackTransport) call(ctx context.Context, method string, f argos.Framer) error {
	result := make(chan error, 1)
	t.calls <- loopbackCall{ctx: ctx, method: method, framer: f, result: result}
	return <-result
}

type openTransport struct {
	open func(context.Context, string) (argos.Framer, error)
}

func (*openTransport) ListenAndServe(
	_ context.Context,
	_ func(context.Context, string, argos.Framer) error,
	_ ...argos.TransportServerOption,
) error {
	panic("client test transport does not listen")
}

func (t *openTransport) Open(ctx context.Context, method string, opts ...argos.TransportClientOption) (argos.Framer, error) {
	_ = opts
	return t.open(ctx, method)
}

func runTestServer(t *testing.T, srv *argos.Server, transport *loopbackTransport) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()
	<-transport.ready
	t.Cleanup(func() {
		cancel()
		<-done
	})
	return cancel
}

func TestServerInvokeUnaryAndClosesSendAfterDispatch(t *testing.T) {
	transport := newLoopbackTransport()
	srv := argos.NewServer()
	service := srv.NewService(argos.WithTransport(transport), argos.WithCodec(jsonCodec{}))

	var dispatchDone atomic.Bool
	service.Register(func(_ context.Context, method string, st argos.Stream) error {
		defer dispatchDone.Store(true)
		if method != "/echo.Echo/Say" {
			t.Fatalf("method = %q", method)
		}
		var request string
		if err := st.Recv(&request); err != nil {
			return err
		}
		if request != "ping" {
			t.Fatalf("request = %q", request)
		}
		return st.Send("pong")
	})
	runTestServer(t, srv, transport)

	input, _ := json.Marshal("ping")
	framer := &testFramer{recv: input, dispatchDone: &dispatchDone}
	if err := transport.call(context.Background(), "/echo.Echo/Say", framer); err != nil {
		t.Fatalf("call: %v", err)
	}
	if framer.closeSendCount.Load() < 1 {
		t.Fatal("CloseSend was not called")
	}
	if !framer.closedAfterCall.Load() {
		t.Fatal("CloseSend ran before dispatch returned")
	}
	var response string
	if err := json.Unmarshal(framer.sent.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response != "pong" {
		t.Fatalf("response = %q", response)
	}
}

func TestServerFilterShortCircuitSkipsDispatchAndCloseSend(t *testing.T) {
	transport := newLoopbackTransport()
	srv := argos.NewServer()
	service := srv.NewService(
		argos.WithTransport(transport),
		argos.WithCodec(jsonCodec{}),
		argos.WithFilter(func(context.Context, string, argos.Stream, argos.Handler) error {
			return argos.Error(argos.Unauthenticated, "missing token")
		}),
	)
	dispatchCalled := false
	service.Register(func(context.Context, string, argos.Stream) error {
		dispatchCalled = true
		return nil
	})
	runTestServer(t, srv, transport)

	framer := &testFramer{}
	err := transport.call(context.Background(), "/echo.Echo/Say", framer)
	if argos.CodeOf(err) != argos.Unauthenticated {
		t.Fatalf("onCall error = %v", err)
	}
	if dispatchCalled {
		t.Fatal("dispatch must not run after a filter short-circuits")
	}
	if framer.closeSendCount.Load() != 0 {
		t.Fatal("filter short-circuit must skip CloseSend")
	}
}

func TestClientOpenPreparesMetadataThenRunsFiltersWithoutClosing(t *testing.T) {
	var order []string
	framer := &testFramer{}
	transport := &openTransport{
		open: func(ctx context.Context, method string) (argos.Framer, error) {
			order = append(order, "open")
			if method != "/echo.Echo/Say" {
				t.Fatalf("method = %q", method)
			}
			md := argos.MetadataFromContext(ctx)
			if md == nil {
				t.Fatal("Open context has no writable Metadata")
			}
			md["from-open"] = []string{"yes"}
			return framer, nil
		},
	}
	client := argos.NewClient(
		argos.WithTransport(transport),
		argos.WithCodec(jsonCodec{}),
		argos.WithFilter(func(ctx context.Context, method string, st argos.Stream, next argos.Handler) error {
			order = append(order, "filter")
			if argos.MetadataFromContext(ctx)["from-open"][0] != "yes" {
				t.Fatal("filter did not receive the context passed to Transport.Open")
			}
			return next(ctx, method, st)
		}),
	)

	if err := client.Open(context.Background(), "/echo.Echo/Say", func(argos.Stream) error {
		order = append(order, "call")
		return nil
	}); err != nil {
		t.Fatalf("Open: %v", err)
	}
	if got := len(order); got != 3 ||
		order[0] != "open" || order[1] != "filter" || order[2] != "call" {
		t.Fatalf("order = %v", order)
	}
	if framer.closeSendCount.Load() != 0 {
		t.Fatal("Client.Open must not call CloseSend")
	}
}

func TestInvokeWithoutRegisterReturnsUnimplemented(t *testing.T) {
	transport := newLoopbackTransport()
	srv := argos.NewServer()
	srv.NewService(argos.WithTransport(transport), argos.WithCodec(jsonCodec{}))
	runTestServer(t, srv, transport)

	framer := &testFramer{}
	err := transport.call(context.Background(), "/missing.Service/Method", framer)
	if argos.CodeOf(err) != argos.Unimplemented {
		t.Fatalf("error = %v", err)
	}
	if framer.closeSendCount.Load() < 1 {
		t.Fatal("CloseSend was not called")
	}
}

func TestAssemblyMistakesReturnErrorAtRunOrOpen(t *testing.T) {
	t.Run("server missing codec", func(t *testing.T) {
		srv := argos.NewServer()
		srv.NewService(argos.WithTransport(newLoopbackTransport()))
		if err := srv.Run(context.Background()); err == nil {
			t.Fatal("expected error")
		}
	})
	t.Run("server missing transport", func(t *testing.T) {
		srv := argos.NewServer()
		srv.NewService(argos.WithCodec(jsonCodec{}))
		if err := srv.Run(context.Background()); err == nil {
			t.Fatal("expected error")
		}
	})
	t.Run("client missing codec", func(t *testing.T) {
		client := argos.NewClient(argos.WithTransport(newLoopbackTransport()))
		err := client.Open(context.Background(), "/echo.Echo/Say", func(argos.Stream) error {
			return nil
		})
		if err == nil {
			t.Fatal("expected error")
		}
	})
	t.Run("client missing transport", func(t *testing.T) {
		client := argos.NewClient(argos.WithCodec(jsonCodec{}))
		err := client.Open(context.Background(), "/echo.Echo/Say", func(argos.Stream) error {
			return nil
		})
		if err == nil {
			t.Fatal("expected error")
		}
	})
}

func TestWithMetadata(t *testing.T) {
	ctx := argos.WithMetadata(context.Background(), argos.Metadata{"k": []string{"v"}})
	md := argos.MetadataFromContext(ctx)
	if md["k"][0] != "v" {
		t.Fatalf("metadata = %v", md)
	}
}

func TestWithTransportNamedRequiresRegistration(t *testing.T) {
	srv := argos.NewServer()
	srv.NewService(argos.WithTransportNamed("no-such-transport"), argos.WithCodec(jsonCodec{}))
	if err := srv.Run(context.Background()); err == nil {
		t.Fatal("expected error for unknown transport name")
	}
}

func TestWithTransportInvalidTypeReturnsError(t *testing.T) {
	srv := argos.NewServer()
	srv.NewService(argos.WithTransport(123), argos.WithCodec(jsonCodec{}))
	if err := srv.Run(context.Background()); err == nil {
		t.Fatal("expected error for invalid WithTransport type")
	}
}

func TestWithCodecInvalidTypeReturnsError(t *testing.T) {
	client := argos.NewClient(argos.WithCodec(123), argos.WithTransport(newLoopbackTransport()))
	err := client.Open(context.Background(), "/echo.Echo/Say", func(argos.Stream) error {
		return nil
	})
	if err == nil {
		t.Fatal("expected error for invalid WithCodec type")
	}
}

func TestFacadeOptionsWireThrough(t *testing.T) {
	tr := newLoopbackTransport()
	srv := argos.NewServer()
	svc := srv.NewService(
		argos.WithTransport(tr),
		argos.WithTransportInstance(tr),
		argos.WithCodec(jsonCodec{}),
		argos.WithCodecInstance(jsonCodec{}),
		argos.WithListenAddress(":8080"),
		argos.WithFilter(func(_ context.Context, _ string, _ argos.Stream, next argos.Handler) error {
			return next(context.Background(), "/m", nil)
		}),
	)
	if svc == nil {
		t.Fatal("nil service")
	}
	client := argos.NewClient(
		argos.WithTransportNamed("missing"), // resolved at Open
		argos.WithCodecNamed("missing"),
		argos.WithTarget("127.0.0.1:9090"),
		argos.WithClientTransportOption(transport.WithDialAddress("127.0.0.1:9090")),
	)
	if client == nil {
		t.Fatal("nil client")
	}
}

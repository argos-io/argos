package argos_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/argos-io/argos/client"
	"github.com/argos-io/argos/filter"
	"github.com/argos-io/argos/metadata"
	"github.com/argos-io/argos"
	"github.com/argos-io/argos/server"
	"github.com/argos-io/argos/stream"

	"github.com/argos-io/argos/errs"
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
	framer transport.Framer
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
	onCall func(context.Context, string, transport.Framer) error,
	opts ...transport.ServerOption,
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

func (t *loopbackTransport) Open(_ context.Context, _ string, opts ...transport.ClientOption) (transport.Framer, error) {
	_ = opts
	panic("server loopback tests use call")
}

func (t *loopbackTransport) call(ctx context.Context, method string, f transport.Framer) error {
	result := make(chan error, 1)
	t.calls <- loopbackCall{ctx: ctx, method: method, framer: f, result: result}
	return <-result
}

type openTransport struct {
	open func(context.Context, string) (transport.Framer, error)
}

func (*openTransport) ListenAndServe(
	_ context.Context,
	_ func(context.Context, string, transport.Framer) error,
	_ ...transport.ServerOption,
) error {
	panic("client test transport does not listen")
}

func (t *openTransport) Open(ctx context.Context, method string, opts ...transport.ClientOption) (transport.Framer, error) {
	_ = opts
	return t.open(ctx, method)
}

func runTestServer(t *testing.T, srv *server.Server, transport *loopbackTransport) context.CancelFunc {
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
	srv := server.New()
	service := srv.NewService(argos.WithTransport(transport), argos.WithCodec(jsonCodec{}))

	var dispatchDone atomic.Bool
	service.Register(func(_ context.Context, method string, st stream.Stream) error {
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
	srv := server.New()
	service := srv.NewService(
		argos.WithTransport(transport),
		argos.WithCodec(jsonCodec{}),
		argos.WithFilter(func(context.Context, string, stream.Stream, filter.Handler) error {
			return errs.Error(errs.Unauthenticated, "missing token")
		}),
	)
	dispatchCalled := false
	service.Register(func(context.Context, string, stream.Stream) error {
		dispatchCalled = true
		return nil
	})
	runTestServer(t, srv, transport)

	framer := &testFramer{}
	err := transport.call(context.Background(), "/echo.Echo/Say", framer)
	if errs.CodeOf(err) != errs.Unauthenticated {
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
		open: func(ctx context.Context, method string) (transport.Framer, error) {
			order = append(order, "open")
			if method != "/echo.Echo/Say" {
				t.Fatalf("method = %q", method)
			}
			md := metadata.FromContext(ctx)
			if md == nil {
				t.Fatal("Open context has no writable Metadata")
			}
			md["from-open"] = []string{"yes"}
			return framer, nil
		},
	}
	client := client.New(
		argos.WithTransport(transport),
		argos.WithCodec(jsonCodec{}),
		argos.WithFilter(func(ctx context.Context, method string, st stream.Stream, next filter.Handler) error {
			order = append(order, "filter")
			if metadata.FromContext(ctx)["from-open"][0] != "yes" {
				t.Fatal("filter did not receive the context passed to Transport.Open")
			}
			return next(ctx, method, st)
		}),
	)

	if err := client.Open(context.Background(), "/echo.Echo/Say", func(stream.Stream) error {
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
	srv := server.New()
	srv.NewService(argos.WithTransport(transport), argos.WithCodec(jsonCodec{}))
	runTestServer(t, srv, transport)

	framer := &testFramer{}
	err := transport.call(context.Background(), "/missing.Service/Method", framer)
	if errs.CodeOf(err) != errs.Unimplemented {
		t.Fatalf("error = %v", err)
	}
	if framer.closeSendCount.Load() < 1 {
		t.Fatal("CloseSend was not called")
	}
}

func TestAssemblyMistakesReturnErrorAtRunOrOpen(t *testing.T) {
	t.Run("server missing codec", func(t *testing.T) {
		srv := server.New()
		srv.NewService(argos.WithTransport(newLoopbackTransport()))
		if err := srv.Run(context.Background()); err == nil {
			t.Fatal("expected error")
		}
	})
	t.Run("server missing transport", func(t *testing.T) {
		srv := server.New()
		srv.NewService(argos.WithCodec(jsonCodec{}))
		if err := srv.Run(context.Background()); err == nil {
			t.Fatal("expected error")
		}
	})
	t.Run("client missing codec", func(t *testing.T) {
		client := client.New(argos.WithTransport(newLoopbackTransport()))
		err := client.Open(context.Background(), "/echo.Echo/Say", func(stream.Stream) error {
			return nil
		})
		if err == nil {
			t.Fatal("expected error")
		}
	})
	t.Run("client missing transport", func(t *testing.T) {
		client := client.New(argos.WithCodec(jsonCodec{}))
		err := client.Open(context.Background(), "/echo.Echo/Say", func(stream.Stream) error {
			return nil
		})
		if err == nil {
			t.Fatal("expected error")
		}
	})
}

func TestWithMetadata(t *testing.T) {
	ctx := metadata.With(context.Background(), metadata.Metadata{"k": []string{"v"}})
	md := metadata.FromContext(ctx)
	if md["k"][0] != "v" {
		t.Fatalf("metadata = %v", md)
	}
}

func TestWithTransportNamedRequiresRegistration(t *testing.T) {
	srv := server.New()
	srv.NewService(argos.WithTransportNamed("no-such-transport"), argos.WithCodec(jsonCodec{}))
	if err := srv.Run(context.Background()); err == nil {
		t.Fatal("expected error for unknown transport name")
	}
}

func TestWithTransportInvalidTypeReturnsError(t *testing.T) {
	srv := server.New()
	srv.NewService(argos.WithTransport(123), argos.WithCodec(jsonCodec{}))
	if err := srv.Run(context.Background()); err == nil {
		t.Fatal("expected error for invalid WithTransport type")
	}
}

func TestWithCodecInvalidTypeReturnsError(t *testing.T) {
	client := client.New(argos.WithCodec(123), argos.WithTransport(newLoopbackTransport()))
	err := client.Open(context.Background(), "/echo.Echo/Say", func(stream.Stream) error {
		return nil
	})
	if err == nil {
		t.Fatal("expected error for invalid WithCodec type")
	}
}

func TestOptionsWireThrough(t *testing.T) {
	tr := newLoopbackTransport()
	srv := server.New()
	svc := srv.NewService(
		argos.WithTransport(tr),
		argos.WithTransportInstance(tr),
		argos.WithCodec(jsonCodec{}),
		argos.WithCodecInstance(jsonCodec{}),
		argos.WithListenAddress(":8080"),
		argos.WithFilter(func(_ context.Context, _ string, _ stream.Stream, next filter.Handler) error {
			return next(context.Background(), "/m", nil)
		}),
	)
	if svc == nil {
		t.Fatal("nil service")
	}
	client := client.New(
		argos.WithTransportNamed("missing"), // resolved at Open
		argos.WithCodecNamed("missing"),
		argos.WithTarget("ip://127.0.0.1:9090"),
		argos.WithClientTransportOption(transport.WithDialAddress("127.0.0.1:9090")),
	)
	if client == nil {
		t.Fatal("nil client")
	}
}

// TestAccept enforces repository invariants (impl 零传输痕迹、dispatch 唯一、binding.invoke).
func TestAccept(t *testing.T) {
	t.Run("impl_no_transport_references", testAcceptImplNoTransportReferences)
	t.Run("dispatch_only_in_binding", testAcceptDispatchOnlyInBinding)
	t.Run("binding_invoke_exists", testAcceptBindingInvokeExists)
}

func testAcceptImplNoTransportReferences(t *testing.T) {
	t.Helper()
	path := filepath.Join("example", "echo", "impl.go")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%s): %v", path, err)
	}
	bad := regexp.MustCompile(`http2|http1|websocket|telnet|grpc|transport/`)
	if bad.Match(data) {
		t.Fatalf("%s contains transport or protocol names", path)
	}
}

func testAcceptDispatchOnlyInBinding(t *testing.T) {
	t.Helper()
	dispatchCall := regexp.MustCompile(`\bdispatch\s*\(`)
	var matches []string
	err := filepath.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "vendor":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()
		sc := bufio.NewScanner(f)
		for sc.Scan() {
			if dispatchCall.MatchString(sc.Text()) {
				matches = append(matches, filepath.ToSlash(path))
				break
			}
		}
		return sc.Err()
	})
	if err != nil {
		t.Fatalf("WalkDir: %v", err)
	}
	want := "server/binding.go"
	if len(matches) != 1 || matches[0] != want {
		t.Fatalf("dispatch call sites in %v, want only %s", matches, want)
	}
}

func testAcceptBindingInvokeExists(t *testing.T) {
	t.Helper()
	path := filepath.Join("server", "binding.go")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(%s): %v", path, err)
	}
	if !regexp.MustCompile(`func \(b \*binding\) invoke`).Match(data) {
		t.Fatalf("%s missing binding.invoke", path)
	}
}

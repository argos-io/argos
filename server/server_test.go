package server_test

import (
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/argos-io/argos"
	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/filter"
	"github.com/argos-io/argos/framing"
	"github.com/argos-io/argos/internal/fake"
	"github.com/argos-io/argos/server"
	"github.com/argos-io/argos/status"
	"github.com/argos-io/argos/stream"
	"github.com/argos-io/argos/transport"
)

const svcName = "test.v1.Echo"
const methodEcho = "Echo"
const fullMethod = svcName + "." + methodEcho

// ---------------------------------------------------------------------------
// testTransport — yields offered Conns to Serve's onConn.
// ---------------------------------------------------------------------------

type testTransport struct {
	mu       sync.Mutex
	conns    chan transport.Conn
	stopCh   chan struct{}
	stopped  bool
	closed   bool
	onConnWG sync.WaitGroup
}

func newTestTransport() *testTransport {
	return &testTransport{
		conns:  make(chan transport.Conn, 8),
		stopCh: make(chan struct{}),
	}
}

func (t *testTransport) Offer(c transport.Conn) {
	t.mu.Lock()
	stopped := t.stopped || t.closed
	t.mu.Unlock()
	if stopped {
		_ = c.Close()
		return
	}
	select {
	case t.conns <- c:
	case <-t.stopCh:
		_ = c.Close()
	}
}

func (t *testTransport) Serve(ctx context.Context, onConn func(context.Context, transport.Conn), _ ...transport.ServerOption) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.stopCh:
			return nil
		case c, ok := <-t.conns:
			if !ok {
				return nil
			}
			t.onConnWG.Add(1)
			go func(c transport.Conn) {
				defer t.onConnWG.Done()
				onConn(ctx, c)
			}(c)
		}
	}
}

func (t *testTransport) Dial(context.Context, transport.DialSpec, ...transport.ClientOption) (transport.Conn, error) {
	return nil, errors.New("testTransport: Dial not supported")
}

func (t *testTransport) Shutdown(ctx context.Context) error {
	t.mu.Lock()
	if !t.stopped {
		t.stopped = true
		close(t.stopCh)
	}
	t.mu.Unlock()

	done := make(chan struct{})
	go func() {
		t.onConnWG.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (t *testTransport) Close() error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	if !t.stopped {
		t.stopped = true
		close(t.stopCh)
	}
	t.mu.Unlock()
	return nil
}

// ---------------------------------------------------------------------------
// rawCodec — byte/string passthrough for fake framing tests.
// ---------------------------------------------------------------------------

type rawCodec struct{}

func (rawCodec) Marshal(v any) ([]byte, error) {
	switch x := v.(type) {
	case []byte:
		return append([]byte(nil), x...), nil
	case string:
		return []byte(x), nil
	case *[]byte:
		return append([]byte(nil), (*x)...), nil
	case *string:
		return []byte(*x), nil
	default:
		return nil, fmt.Errorf("rawCodec: unsupported type %T", v)
	}
}

func (rawCodec) Unmarshal(b []byte, v any) error {
	switch x := v.(type) {
	case *[]byte:
		*x = append([]byte(nil), b...)
		return nil
	case *string:
		*x = string(b)
		return nil
	default:
		return fmt.Errorf("rawCodec: unsupported type %T", v)
	}
}

func (rawCodec) CodecName() string { return "raw" }

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func echoService() descriptor.Service {
	m := descriptor.MustMethod(fullMethod, descriptor.Unary)
	return descriptor.MustService(svcName, m)
}

func startServer(t *testing.T, tr *testTransport, fr framing.Framing, h filter.Handler, opts ...argos.Option) *server.Server {
	t.Helper()
	cfg, err := argos.New(opts...)
	if err != nil {
		t.Fatal(err)
	}
	srv := server.New(cfg)
	err = srv.AddBinding(func() (argos.Binding, error) {
		return argos.Binding{
			Transport: tr,
			Framing:   fr,
			Codec:     rawCodec{},
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	err = srv.Register(echoService(), map[string]filter.Handler{
		methodEcho: h,
	})
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		_ = srv.Run(context.Background())
	}()
	// Give Serve a moment to enter its loop.
	time.Sleep(10 * time.Millisecond)
	t.Cleanup(func() { _ = srv.Close() })
	return srv
}

func openClientCall(t *testing.T, clientConn transport.Conn, fr *fake.Framing, method descriptor.Method) framing.Call {
	t.Helper()
	sess, err := fr.NewClientSession(context.Background(), clientConn, framing.SessionSpec{
		CodecName: "raw",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sess.Close() })
	call, err := sess.OpenCall(context.Background(), method, framing.CallSpec{})
	if err != nil {
		t.Fatal(err)
	}
	return call
}

func unaryRoundTrip(t *testing.T, call framing.Call, payload string) string {
	t.Helper()
	if err := call.Send([]byte(payload)); err != nil {
		t.Fatal(err)
	}
	if err := call.HalfClose(); err != nil {
		t.Fatal(err)
	}
	b, release, err := call.Recv()
	if err != nil {
		t.Fatal(err)
	}
	out := string(b)
	release()
	_, _, err = call.Recv()
	if err != io.EOF {
		t.Fatalf("want EOF after response, got %v", err)
	}
	return out
}

// drainRecv consumes the receive side through EOF so Sequential fake framing
// does not leave an END frame on the wire for the next AcceptCall.
func drainRecv(st stream.Stream) {
	for {
		var discard []byte
		if err := st.Recv(&discard); err != nil {
			return
		}
	}
}

// ---------------------------------------------------------------------------
// 1.14 tests
// ---------------------------------------------------------------------------

func TestUnaryRegisterAndRun(t *testing.T) {
	tr := newTestTransport()
	srvFr := fake.NewFraming(framing.Sequential)
	cliFr := fake.NewFraming(framing.Sequential)
	var got atomic.Int64
	h := func(ctx context.Context, m descriptor.Method, st stream.Stream) error {
		got.Add(1)
		var req []byte
		if err := st.Recv(&req); err != nil {
			return err
		}
		drainRecv(st)
		return st.Send([]byte("pong:" + string(req)))
	}
	srv := startServer(t, tr, srvFr, h)

	client, serverConn := fake.BytePipe()
	tr.Offer(serverConn)

	method := descriptor.MustMethod(fullMethod, descriptor.Unary)
	call := openClientCall(t, client, cliFr, method)
	out := unaryRoundTrip(t, call, "hi")
	_ = call.Close()
	if out != "pong:hi" {
		t.Fatalf("got %q", out)
	}
	if got.Load() != 1 {
		t.Fatalf("handler calls = %d", got.Load())
	}
	_ = srv
}

func TestTenSequentialCallsOneConn(t *testing.T) {
	tr := newTestTransport()
	srvFr := fake.NewFraming(framing.Sequential)
	cliFr := fake.NewFraming(framing.Sequential)
	var n atomic.Int64
	h := func(ctx context.Context, m descriptor.Method, st stream.Stream) error {
		n.Add(1)
		var req []byte
		if err := st.Recv(&req); err != nil {
			return err
		}
		drainRecv(st)
		return st.Send(req)
	}
	startServer(t, tr, srvFr, h)

	client, serverConn := fake.BytePipe()
	tr.Offer(serverConn)

	method := descriptor.MustMethod(fullMethod, descriptor.Unary)
	sess, err := cliFr.NewClientSession(context.Background(), client, framing.SessionSpec{CodecName: "raw"})
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()

	for i := 0; i < 10; i++ {
		call, err := sess.OpenCall(context.Background(), method, framing.CallSpec{})
		if err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
		want := fmt.Sprintf("msg-%d", i)
		got := unaryRoundTrip(t, call, want)
		_ = call.Close()
		if got != want {
			t.Fatalf("call %d: got %q want %q", i, got, want)
		}
	}
	if n.Load() != 10 {
		t.Fatalf("handler calls = %d want 10", n.Load())
	}
}

func TestHandlerErrorDoesNotEndLoop(t *testing.T) {
	tr := newTestTransport()
	srvFr := fake.NewFraming(framing.Sequential)
	cliFr := fake.NewFraming(framing.Sequential)
	var n atomic.Int64
	h := func(ctx context.Context, m descriptor.Method, st stream.Stream) error {
		i := n.Add(1)
		var req []byte
		if err := st.Recv(&req); err != nil {
			return err
		}
		drainRecv(st)
		if i == 5 {
			return status.Error(status.Internal, "boom on 5")
		}
		return st.Send(req)
	}
	startServer(t, tr, srvFr, h)

	client, serverConn := fake.BytePipe()
	tr.Offer(serverConn)

	method := descriptor.MustMethod(fullMethod, descriptor.Unary)
	sess, err := cliFr.NewClientSession(context.Background(), client, framing.SessionSpec{CodecName: "raw"})
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()

	for i := 1; i <= 7; i++ {
		call, err := sess.OpenCall(context.Background(), method, framing.CallSpec{})
		if err != nil {
			t.Fatalf("call %d open: %v", i, err)
		}
		if err := call.Send([]byte("x")); err != nil {
			t.Fatal(err)
		}
		_ = call.HalfClose()
		_, release, err := call.Recv()
		if i == 5 {
			// Finish writes STATUS; Recv should still get terminal EOF from fake
			// after status frame, or an error — either way call must complete.
			if release != nil {
				release()
			}
			_ = err
		} else {
			if err != nil {
				t.Fatalf("call %d recv: %v", i, err)
			}
			release()
			_, _, err = call.Recv()
			if err != io.EOF {
				t.Fatalf("call %d: want EOF, got %v", i, err)
			}
		}
		_ = call.Close()
	}
	if n.Load() != 7 {
		t.Fatalf("handler calls = %d want 7", n.Load())
	}
}

func TestShutdownIdleConnExitsQuickly(t *testing.T) {
	tr := newTestTransport()
	fr := fake.NewFraming(framing.Sequential)
	h := func(ctx context.Context, m descriptor.Method, st stream.Stream) error {
		var req []byte
		if err := st.Recv(&req); err != nil {
			return err
		}
		drainRecv(st)
		return st.Send(req)
	}
	cfg, err := argos.New(argos.WithMaxInboundConnIdle(30 * time.Second))
	if err != nil {
		t.Fatal(err)
	}
	srv := server.New(cfg)
	if err := srv.AddBinding(func() (argos.Binding, error) {
		return argos.Binding{Transport: tr, Framing: fr, Codec: rawCodec{}}, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := srv.Register(echoService(), map[string]filter.Handler{methodEcho: h}); err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Run(context.Background()) }()
	time.Sleep(10 * time.Millisecond)

	client, serverConn := fake.BytePipe()
	tr.Offer(serverConn)

	// One call then idle (blocked in AcceptCall).
	method := descriptor.MustMethod(fullMethod, descriptor.Unary)
	call := openClientCall(t, client, fr, method)
	_ = unaryRoundTrip(t, call, "once")
	_ = call.Close()

	time.Sleep(20 * time.Millisecond) // ensure AcceptCall is blocking

	start := time.Now()
	shCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := srv.Shutdown(shCtx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	elapsed := time.Since(start)
	if elapsed > 100*time.Millisecond {
		t.Fatalf("idle Shutdown took %v; want < 100ms", elapsed)
	}
}

func TestShutdownDrainsInFlightCall(t *testing.T) {
	tr := newTestTransport()
	fr := fake.NewFraming(framing.Sequential)
	entered := make(chan struct{})
	releaseH := make(chan struct{})
	h := func(ctx context.Context, m descriptor.Method, st stream.Stream) error {
		var req []byte
		if err := st.Recv(&req); err != nil {
			return err
		}
		drainRecv(st)
		close(entered)
		select {
		case <-releaseH:
		case <-ctx.Done():
			return ctx.Err()
		}
		return st.Send([]byte("done"))
	}
	srv := startServer(t, tr, fr, h)

	client, serverConn := fake.BytePipe()
	tr.Offer(serverConn)

	method := descriptor.MustMethod(fullMethod, descriptor.Unary)
	call := openClientCall(t, client, fr, method)
	if err := call.Send([]byte("x")); err != nil {
		t.Fatal(err)
	}
	_ = call.HalfClose()

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not enter")
	}

	done := make(chan error, 1)
	go func() {
		done <- srv.Shutdown(context.Background())
	}()

	// Shutdown must not finish until in-flight call completes.
	select {
	case err := <-done:
		t.Fatalf("Shutdown returned early: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(releaseH)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown did not complete after call drain")
	}
	_ = call.Close()
}

func TestNoConcreteFramingTypeSwitch(t *testing.T) {
	dir := "."
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		path := filepath.Join(dir, e.Name())
		src, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		f, err := parser.ParseFile(fset, path, src, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			sw, ok := n.(*ast.TypeSwitchStmt)
			if !ok {
				return true
			}
			s := fset.Position(sw.Pos()).String()
			for _, clause := range sw.Body.List {
				cc, ok := clause.(*ast.CaseClause)
				if !ok {
					continue
				}
				for _, expr := range cc.List {
					name := exprString(expr)
					if strings.Contains(name, "fake.") ||
						strings.Contains(name, "envelope.") ||
						strings.Contains(name, "grpc.") ||
						strings.Contains(name, "tcp.") ||
						strings.Contains(name, "http2.") {
						t.Fatalf("%s: concrete framing/transport type-switch involving %s", s, name)
					}
				}
			}
			return true
		})
		// Also grep for type assertions to fake package.
		if strings.Contains(string(src), "fake.") {
			t.Fatalf("%s imports or references fake concrete types", path)
		}
	}
}

func exprString(e ast.Expr) string {
	switch x := e.(type) {
	case *ast.Ident:
		return x.Name
	case *ast.SelectorExpr:
		return exprString(x.X) + "." + x.Sel.Name
	case *ast.StarExpr:
		return "*" + exprString(x.X)
	default:
		return fmt.Sprintf("%T", e)
	}
}

// ---------------------------------------------------------------------------
// 1.14b — context.Cause mapping
// ---------------------------------------------------------------------------

func TestCauseSentinelsExported(t *testing.T) {
	// 1.14b: causes are distinct and detectable with errors.Is / context.Cause.
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(server.ErrServerShutdown)
	if !errors.Is(context.Cause(ctx), server.ErrServerShutdown) {
		t.Fatal("ErrServerShutdown not recoverable via Cause")
	}
	ctx2, cancel2 := context.WithCancelCause(context.Background())
	cancel2(server.ErrPeerGone)
	if !errors.Is(context.Cause(ctx2), server.ErrPeerGone) {
		t.Fatal("ErrPeerGone not recoverable via Cause")
	}
	ctx3, cancel3 := context.WithCancelCause(context.Background())
	cancel3(server.ErrSessionExpired)
	if !errors.Is(context.Cause(ctx3), server.ErrSessionExpired) {
		t.Fatal("ErrSessionExpired not recoverable via Cause")
	}
	if errors.Is(server.ErrServerShutdown, server.ErrPeerGone) {
		t.Fatal("causes must be distinct")
	}
}

func TestAcceptCancelDoesNotKillInFlight(t *testing.T) {
	// Same as drain test: proves accept ctx cancel ≠ conn ctx cancel.
	TestShutdownDrainsInFlightCall(t)
}

// ---------------------------------------------------------------------------
// 1.14c — idle timer (minimal)
// ---------------------------------------------------------------------------

func TestInboundConnIdleClosesAccept(t *testing.T) {
	tr := newTestTransport()
	fr := fake.NewFraming(framing.Sequential)
	h := func(ctx context.Context, m descriptor.Method, st stream.Stream) error {
		var req []byte
		if err := st.Recv(&req); err != nil {
			return err
		}
		drainRecv(st)
		return st.Send(req)
	}
	srv := startServer(t, tr, fr, h, argos.WithMaxInboundConnIdle(80*time.Millisecond))

	client, serverConn := fake.BytePipe()
	onClose := make(chan struct{})
	// Wrap: detect server session close via client read error after idle.
	tr.Offer(serverConn)

	method := descriptor.MustMethod(fullMethod, descriptor.Unary)
	sess, err := fr.NewClientSession(context.Background(), client, framing.SessionSpec{CodecName: "raw"})
	if err != nil {
		t.Fatal(err)
	}
	call, err := sess.OpenCall(context.Background(), method, framing.CallSpec{})
	if err != nil {
		t.Fatal(err)
	}
	_ = unaryRoundTrip(t, call, "a")
	_ = call.Close()

	// Wait for idle to fire and server to close the session/conn.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		// Next OpenCall may fail once server closed the conn.
		call2, err := sess.OpenCall(context.Background(), method, framing.CallSpec{})
		if err != nil {
			close(onClose)
			break
		}
		_ = call2.Close()
		time.Sleep(30 * time.Millisecond)
	}
	select {
	case <-onClose:
	default:
		// Also acceptable: sess.Reusable() became false / conn closed.
		if sess.Reusable() {
			t.Fatal("expected idle to end the inbound connection")
		}
	}
	_ = srv
}

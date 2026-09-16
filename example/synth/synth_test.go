package synth

import (
	"context"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/argos-io/argos"
	"github.com/argos-io/argos/client"
	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/filter"
	"github.com/argos-io/argos/framing"
	"github.com/argos-io/argos/metadata"
	"github.com/argos-io/argos/server"
	"github.com/argos-io/argos/status"
	"github.com/argos-io/argos/stream"
	"github.com/argos-io/argos/transport"
	"github.com/argos-io/argos/transport/tcp"

	_ "github.com/argos-io/argos/resolver/ip"
)

type hasAddr interface {
	Addr() net.Addr
}

// testConfig is the tuning shared by both halves of a synth test. The knobs
// below no longer belong to one option list — session limits are client-only,
// inbound-connection limits and ListenAddress are server-only — so the shared
// tuning travels as a Config that each constructor names with argos.WithConfig.
func testConfig() *argos.Config {
	return &argos.Config{
		MaxConcurrentCalls:     16,
		MaxBufferedBytes:       16 * 16 * 1024 * 1024,
		MaxIdleSessions:        4,
		MaxSessionsPerEndpoint: 4,
		HandshakeTimeout:       5 * time.Second,
		MaxInboundConnIdle:     30 * time.Second,
		MaxInboundConnAge:      30 * time.Minute,
		ListenAddress:          "127.0.0.1:0",
	}
}

func startSynthServer(t *testing.T, handlers map[string]filter.Handler, extra ...argos.ServerOption) (addr string, fn argos.BindingFunc) {
	t.Helper()
	fn = NewTCP()
	var addrTr hasAddr
	bound := make(chan struct{})
	srv := server.New(append([]argos.ServerOption{argos.WithConfig(testConfig())}, extra...)...)
	if err := srv.AddBinding(func() (argos.Binding, error) {
		b, err := fn()
		if err != nil {
			return b, err
		}
		tr, ok := b.Transport.(hasAddr)
		if !ok {
			t.Fatal("tcp transport missing Addr()")
		}
		addrTr = tr
		select {
		case <-bound:
		default:
			close(bound)
		}
		return b, nil
	}); err != nil {
		t.Fatal(err)
	}

	methods := []descriptor.Method{
		descriptor.MustMethod(MethodPing, descriptor.Unary),
		descriptor.MustMethod(MethodEcho, descriptor.Unary),
		descriptor.MustMethod(MethodExclusive, descriptor.BidiStreaming),
	}
	svc := descriptor.MustService(ServiceName, methods...)
	if err := srv.Register(svc, handlers); err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Run(context.Background()) }()
	select {
	case <-bound:
	case <-time.After(3 * time.Second):
		t.Fatal("BindingFunc not invoked")
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if a := addrTr.Addr(); a != nil {
			addr = a.String()
			t.Cleanup(func() { _ = srv.Close() })
			return addr, fn
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("listener not ready")
	return "", nil
}

func newSynthClient(t *testing.T, addr string, fn argos.BindingFunc, extra ...argos.ClientOption) *client.Client {
	t.Helper()
	cli, err := client.New(append([]argos.ClientOption{
		argos.WithConfig(testConfig()),
		argos.WithServiceName(ServiceName),
		argos.WithBinding(fn),
		argos.WithTarget("ip://" + addr),
	}, extra...)...)
	if err != nil {
		t.Fatalf("client.New: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	return cli
}

func waitTCPAddr(t *testing.T, tr transport.Transport) string {
	t.Helper()
	a, ok := tr.(hasAddr)
	if !ok {
		t.Fatal("transport missing Addr()")
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if addr := a.Addr(); addr != nil {
			return addr.String()
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("listener not ready")
	return ""
}

// TestGreetingReceivedBeforeCall proves NewServerSession sends the greeting
// before AcceptCall, and NewClientSession observes it before OpenCall.
func TestGreetingReceivedBeforeCall(t *testing.T) {
	t.Parallel()
	srvTr := tcp.New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer srvTr.Close()

	go func() {
		_ = srvTr.Serve(ctx, func(_ context.Context, c transport.Conn) {
			hsCtx, hsCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer hsCancel()
			fr := New()
			sess, err := fr.NewServerSession(hsCtx, c, framing.SessionSpec{})
			if err != nil {
				_ = c.Close()
				return
			}
			defer sess.Close()
			// Bounded: an unbounded AcceptCall turns a failure into a hang.
			acceptCtx, acceptCancel := context.WithTimeout(ctx, 5*time.Second)
			defer acceptCancel()
			for {
				md := metadata.New(metadata.RoleResponder, func(metadata.Metadata) error {
					return status.Error(status.Unimplemented, "unused")
				})
				sc, err := sess.AcceptCall(acceptCtx, framing.CallSpec{Metadata: md})
				if err != nil {
					return
				}
				_ = sc.Accept(descriptor.MustMethod(MethodPing, descriptor.Unary))
				_, rel, err := sc.Recv()
				if rel != nil {
					rel()
				}
				if err == nil {
					_, rel2, _ := sc.Recv()
					if rel2 != nil {
						rel2()
					}
				}
				_ = sc.Send([]byte("pong"))
				_ = sc.Finish(nil)
				_ = sc.Close()
			}
		}, transport.WithListenAddress("127.0.0.1:0"))
	}()

	addr := waitTCPAddr(t, srvTr)
	cliTr := tcp.New()
	defer cliTr.Close()
	conn, err := cliTr.Dial(context.Background(), transport.DialSpec{Endpoint: addr})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	hsCtx, hsCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer hsCancel()
	cs, err := New().NewClientSession(hsCtx, conn, framing.SessionSpec{})
	if err != nil {
		t.Fatalf("NewClientSession: %v", err)
	}
	defer cs.Close()

	cliSess := cs.(*clientSession)
	if g := cliSess.state.Greeting(); g != DefaultGreeting {
		t.Fatalf("greeting = %q, want %q (must be read during NewClientSession, before OpenCall)", g, DefaultGreeting)
	}

	call, err := cs.OpenCall(context.Background(), descriptor.MustMethod(MethodPing, descriptor.Unary), framing.CallSpec{})
	if err != nil {
		t.Fatalf("OpenCall: %v", err)
	}
	if err := call.Send([]byte("hi")); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if err := call.HalfClose(); err != nil {
		t.Fatalf("HalfClose: %v", err)
	}
	got, rel, err := call.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	rel()
	if string(got) != "pong" {
		t.Fatalf("payload = %q, want pong", got)
	}
	_, _, err = call.Recv()
	if !errors.Is(err, io.EOF) {
		t.Fatalf("Recv terminal = %v, want EOF", err)
	}
	if err := call.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestCustomMethodFieldRouting(t *testing.T) {
	t.Parallel()
	var hitPing, hitEcho atomic.Int64
	handlers := map[string]filter.Handler{
		"Ping": func(ctx context.Context, m descriptor.Method, st stream.Stream) error {
			hitPing.Add(1)
			var in []byte
			if err := st.Recv(&in); err != nil {
				return err
			}
			_ = st.Recv(&in) // EOF
			return st.Send([]byte("ping-ok"))
		},
		"Echo": func(ctx context.Context, m descriptor.Method, st stream.Stream) error {
			hitEcho.Add(1)
			var in []byte
			if err := st.Recv(&in); err != nil {
				return err
			}
			_ = st.Recv(&in)
			return st.Send(in)
		},
		"Exclusive": func(ctx context.Context, m descriptor.Method, st stream.Stream) error {
			return status.Error(status.Unimplemented, "not in this test")
		},
	}
	addr, fn := startSynthServer(t, handlers)
	cli := newSynthClient(t, addr, fn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ping := descriptor.MustMethod(MethodPing, descriptor.Unary)
	cs, err := cli.Open(ctx, ping)
	if err != nil {
		t.Fatalf("Open Ping: %v", err)
	}
	if err := cs.Send([]byte("x")); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if err := cs.HalfClose(); err != nil {
		t.Fatalf("HalfClose: %v", err)
	}
	var out []byte
	if err := cs.Recv(&out); err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if string(out) != "ping-ok" {
		t.Fatalf("Ping resp = %q", out)
	}
	if err := cs.Recv(&out); !errors.Is(err, io.EOF) {
		t.Fatalf("Recv EOF: %v", err)
	}
	_ = cs.Close()

	echo := descriptor.MustMethod(MethodEcho, descriptor.Unary)
	cs, err = cli.Open(ctx, echo)
	if err != nil {
		t.Fatalf("Open Echo: %v", err)
	}
	if err := cs.Send([]byte("hello")); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if err := cs.HalfClose(); err != nil {
		t.Fatalf("HalfClose: %v", err)
	}
	if err := cs.Recv(&out); err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if string(out) != "hello" {
		t.Fatalf("Echo resp = %q", out)
	}
	_ = cs.Recv(&out)
	_ = cs.Close()

	if hitPing.Load() != 1 || hitEcho.Load() != 1 {
		t.Fatalf("routing hits ping=%d echo=%d (method must come from custom on-wire field)", hitPing.Load(), hitEcho.Load())
	}
}

// TestSendHeadersUnimplemented pins the contract for a carrier with no
// explicit headers channel: CallMetadata.SendHeaders reports the stable
// status.Unimplemented, leaves outgoing headers unfrozen, and the call itself
// still completes. The handler records what it saw and the test goroutine
// asserts on it after the RPC finished — an assertion that only lives inside
// the handler cannot tell a completed call from one that never ran.
func TestSendHeadersUnimplemented(t *testing.T) {
	t.Parallel()

	type sendHeadersObs struct {
		called bool
		code   status.Code
		err    error
	}
	var observed atomic.Value
	observed.Store(sendHeadersObs{})

	handlers := map[string]filter.Handler{
		"Ping": func(ctx context.Context, m descriptor.Method, st stream.Stream) error {
			md, ok := metadata.FromContext(ctx)
			if !ok {
				return status.Error(status.Internal, "synth: missing CallMetadata in handler ctx")
			}
			err := md.SendHeaders()
			observed.Store(sendHeadersObs{called: true, code: status.CodeOf(err), err: err})
			if err == nil {
				return status.Error(status.Internal, "synth: SendHeaders succeeded on a carrier with no header channel")
			}
			// Unimplemented must not freeze outgoing headers.
			if addErr := md.AddOutgoingHeader("x-synth", "1"); addErr != nil {
				return status.Error(status.Internal, "synth: AddOutgoingHeader after unimplemented SendHeaders: "+addErr.Error())
			}
			var in []byte
			if err := st.Recv(&in); err != nil {
				return err
			}
			if err := st.Recv(&in); !errors.Is(err, io.EOF) {
				return status.Error(status.Internal, "synth: want io.EOF after client half-close")
			}
			return st.Send([]byte("ok"))
		},
		"Echo":      func(context.Context, descriptor.Method, stream.Stream) error { return nil },
		"Exclusive": func(context.Context, descriptor.Method, stream.Stream) error { return nil },
	}
	addr, fn := startSynthServer(t, handlers)
	cli := newSynthClient(t, addr, fn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cs, err := cli.Open(ctx, descriptor.MustMethod(MethodPing, descriptor.Unary))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := cs.Send([]byte("x")); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if err := cs.HalfClose(); err != nil {
		t.Fatalf("HalfClose: %v", err)
	}
	var out []byte
	if err := cs.Recv(&out); err != nil {
		t.Fatalf("Recv: %v (unimplemented SendHeaders must not fail the call)", err)
	}
	if string(out) != "ok" {
		t.Fatalf("payload = %q, want ok", out)
	}
	if err := cs.Recv(&out); !errors.Is(err, io.EOF) {
		t.Fatalf("terminal Recv = %v, want io.EOF (call must reach a clean terminal)", err)
	}
	if err := cs.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	obs, _ := observed.Load().(sendHeadersObs)
	if !obs.called {
		t.Fatal("handler never saw CallMetadata.SendHeaders (Ping was not routed to the handler)")
	}
	if obs.code != status.Unimplemented {
		t.Fatalf("handler SendHeaders = %v (code %v), want Unimplemented", obs.err, obs.code)
	}
}

func TestConnStateFromContext(t *testing.T) {
	t.Parallel()
	var sawGreeting atomic.Value
	handlers := map[string]filter.Handler{
		"Ping": func(ctx context.Context, m descriptor.Method, st stream.Stream) error {
			stt, ok := FromContext(ctx)
			if !ok || stt == nil {
				return status.Error(status.Internal, "synth: ConnState missing from handler ctx")
			}
			sawGreeting.Store(stt.Greeting())
			var in []byte
			_ = st.Recv(&in)
			_ = st.Recv(&in)
			return st.Send([]byte("ok"))
		},
		"Echo":      func(context.Context, descriptor.Method, stream.Stream) error { return nil },
		"Exclusive": func(context.Context, descriptor.Method, stream.Stream) error { return nil },
	}
	addr, fn := startSynthServer(t, handlers)
	cli := newSynthClient(t, addr, fn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cs, err := cli.Open(ctx, descriptor.MustMethod(MethodPing, descriptor.Unary))
	if err != nil {
		t.Fatal(err)
	}
	_ = cs.Send([]byte("x"))
	_ = cs.HalfClose()
	var out []byte
	if err := cs.Recv(&out); err != nil {
		t.Fatalf("Recv: %v", err)
	}
	_ = cs.Recv(&out)
	_ = cs.Close()

	g, _ := sawGreeting.Load().(string)
	if g != DefaultGreeting {
		t.Fatalf("handler FromContext greeting = %q, want %q", g, DefaultGreeting)
	}
}

func TestConnStateContextWith(t *testing.T) {
	t.Parallel()
	st := &ConnState{}
	connCtx := ContextWith(context.Background(), st)
	hsCtx, cancel := context.WithTimeout(connCtx, 5*time.Second)
	defer cancel()

	srvTr := tcp.New()
	defer srvTr.Close()
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	go func() {
		_ = srvTr.Serve(ctx, func(_ context.Context, c transport.Conn) {
			_, err := New().NewServerSession(hsCtx, c, framing.SessionSpec{})
			if err != nil {
				_ = c.Close()
				return
			}
			// Session owns conn; leave it until test ends.
			<-ctx.Done()
			_ = c.Close()
		}, transport.WithListenAddress("127.0.0.1:0"))
	}()
	addr := waitTCPAddr(t, srvTr)
	cliTr := tcp.New()
	defer cliTr.Close()
	conn, err := cliTr.Dial(context.Background(), transport.DialSpec{Endpoint: addr})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_, err = New().NewClientSession(hsCtx, conn, framing.SessionSpec{})
	if err != nil {
		t.Fatalf("client handshake: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	got, ok := FromContext(connCtx)
	if !ok || got != st {
		t.Fatalf("FromContext(connCtx) = (%v,%v), want filled pre-placed state", got, ok)
	}
	if got.Greeting() != DefaultGreeting {
		t.Fatalf("greeting = %q", got.Greeting())
	}
}

func TestExclusiveKeepsConnectionOutOfPool(t *testing.T) {
	t.Parallel()
	handlers := map[string]filter.Handler{
		"Ping": func(ctx context.Context, m descriptor.Method, st stream.Stream) error {
			var in []byte
			_ = st.Recv(&in)
			_ = st.Recv(&in)
			return st.Send([]byte("p"))
		},
		"Echo": func(context.Context, descriptor.Method, stream.Stream) error { return nil },
		"Exclusive": func(ctx context.Context, m descriptor.Method, st stream.Stream) error {
			// Multiple exchanges on one bidi call / one connection.
			for i := 0; i < 3; i++ {
				var in []byte
				if err := st.Recv(&in); err != nil {
					return err
				}
				if err := st.Send(append([]byte("echo:"), in...)); err != nil {
					return err
				}
			}
			var in []byte
			if err := st.Recv(&in); !errors.Is(err, io.EOF) {
				return err
			}
			return nil
		},
	}

	var dials atomic.Int64
	fn := func() (argos.Binding, error) {
		b, err := NewTCP()()
		if err != nil {
			return b, err
		}
		inner := b.Transport
		b.Transport = &countingTransport{Transport: inner, dials: &dials}
		return b, nil
	}

	var addrTr hasAddr
	bound := make(chan struct{})
	srv := server.New(argos.WithConfig(testConfig()))
	if err := srv.AddBinding(func() (argos.Binding, error) {
		b, err := NewTCP()()
		if err != nil {
			return b, err
		}
		addrTr = b.Transport.(hasAddr)
		select {
		case <-bound:
		default:
			close(bound)
		}
		return b, nil
	}); err != nil {
		t.Fatal(err)
	}
	methods := []descriptor.Method{
		descriptor.MustMethod(MethodPing, descriptor.Unary),
		descriptor.MustMethod(MethodEcho, descriptor.Unary),
		descriptor.MustMethod(MethodExclusive, descriptor.BidiStreaming),
	}
	if err := srv.Register(descriptor.MustService(ServiceName, methods...), handlers); err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Run(context.Background()) }()
	select {
	case <-bound:
	case <-time.After(3 * time.Second):
		t.Fatal("bind timeout")
	}
	addr := waitTCPAddr(t, addrTr.(transport.Transport))
	t.Cleanup(func() { _ = srv.Close() })

	cli := newSynthClient(t, addr, fn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// The client dials lazily, so the count is still zero here: every dial
	// counted below belongs to the exclusive call.
	if got := dials.Load(); got != 0 {
		t.Fatalf("dials before exclusive Open = %d, want 0", got)
	}

	// Exclusive bidi: three exchanges, then half-close.
	cs, err := cli.Open(ctx, descriptor.MustMethod(MethodExclusive, descriptor.BidiStreaming))
	if err != nil {
		t.Fatalf("Open Exclusive: %v", err)
	}
	for _, msg := range []string{"a", "b", "c"} {
		if err := cs.Send([]byte(msg)); err != nil {
			t.Fatalf("Send %s: %v", msg, err)
		}
		var out []byte
		if err := cs.Recv(&out); err != nil {
			t.Fatalf("Recv %s: %v", msg, err)
		}
		if string(out) != "echo:"+msg {
			t.Fatalf("got %q", out)
		}
	}
	if err := cs.HalfClose(); err != nil {
		t.Fatalf("HalfClose: %v", err)
	}
	var out []byte
	if err := cs.Recv(&out); !errors.Is(err, io.EOF) {
		t.Fatalf("Recv after exclusive = %v, want EOF", err)
	}
	if err := cs.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// One call, one connection: the exclusive call must not have opened a
	// second connection on top of the one dial above.
	dialsAfterExclusive := dials.Load()
	if dialsAfterExclusive != 1 {
		t.Fatalf("dials after exclusive call = %d, want exactly 1", dialsAfterExclusive)
	}

	// Next Ping must dial again — exclusive session is not returned to the pool.
	cs, err = cli.Open(ctx, descriptor.MustMethod(MethodPing, descriptor.Unary))
	if err != nil {
		t.Fatalf("Open Ping: %v", err)
	}
	_ = cs.Send([]byte("x"))
	_ = cs.HalfClose()
	_ = cs.Recv(&out)
	_ = cs.Recv(&out)
	_ = cs.Close()

	if got := dials.Load(); got <= dialsAfterExclusive {
		t.Fatalf("dials after Ping = %d, want > %d (exclusive must keep conn out of pool)", got, dialsAfterExclusive)
	}
}

func TestSequentialReuseAfterPing(t *testing.T) {
	t.Parallel()
	handlers := map[string]filter.Handler{
		"Ping": func(ctx context.Context, m descriptor.Method, st stream.Stream) error {
			var in []byte
			_ = st.Recv(&in)
			_ = st.Recv(&in)
			return st.Send([]byte("p"))
		},
		"Echo":      func(context.Context, descriptor.Method, stream.Stream) error { return nil },
		"Exclusive": func(context.Context, descriptor.Method, stream.Stream) error { return nil },
	}
	var dials atomic.Int64
	fn := func() (argos.Binding, error) {
		b, err := NewTCP()()
		if err != nil {
			return b, err
		}
		b.Transport = &countingTransport{Transport: b.Transport, dials: &dials}
		return b, nil
	}
	addr, _ := startSynthServer(t, handlers)
	cli := newSynthClient(t, addr, fn)
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		cs, err := cli.Open(ctx, descriptor.MustMethod(MethodPing, descriptor.Unary))
		if err != nil {
			t.Fatalf("Open %d: %v", i, err)
		}
		_ = cs.Send([]byte("x"))
		_ = cs.HalfClose()
		var out []byte
		_ = cs.Recv(&out)
		_ = cs.Recv(&out)
		_ = cs.Close()
	}
	if got := dials.Load(); got != 1 {
		t.Fatalf("dials = %d, want 1 (Sequential reuse)", got)
	}
}

// countingTransport wraps Dial to count connection establishments.
type countingTransport struct {
	transport.Transport
	dials *atomic.Int64
}

func (t *countingTransport) Dial(ctx context.Context, spec transport.DialSpec, opts ...transport.ClientOption) (transport.Conn, error) {
	t.dials.Add(1)
	return t.Transport.Dial(ctx, spec, opts...)
}

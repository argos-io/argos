package synth

import (
	"context"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/argos-io/argos"
	"github.com/argos-io/argos/client"
	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/filter"
	"github.com/argos-io/argos/internal/teststack"
	"github.com/argos-io/argos/metadata"
	"github.com/argos-io/argos/server"
	"github.com/argos-io/argos/status"
	"github.com/argos-io/argos/stream"
	"github.com/argos-io/argos/transport"

	_ "github.com/argos-io/argos/resolver/ip"
)

func testOptions() *argos.Options {
	return &argos.Options{
		MaxConcurrentCalls: 16,
		HandshakeTimeout:   5 * time.Second,
		MaxInboundConnIdle: 30 * time.Second,
		MaxInboundConnAge:  30 * time.Minute,
		ListenAddress:      "127.0.0.1:0",
	}
}

type synthHarness struct {
	addr    string
	srvAxis *Transport
	cliAxis *Transport
}

// startSynthServer brings up a synth server and a client over one service.
// Session and pool limits are fixed when an axis is constructed and no Options
// holds a second copy, so there is nothing for a bare axis to disagree with:
// the built-in pool baseline is the one these tests run against.
func startSynthServer(t *testing.T, handlers map[string]filter.Handler, extra ...argos.ServerOption) *synthHarness {
	t.Helper()
	cfg := testOptions()
	srvAxis := New()
	// Default pool keeps no idle sessions (MaxIdleSessions=0); tests expect reuse.
	cliAxis := New(WithPool(0, 8, 50*time.Second, 30*time.Minute))
	srvTr := teststack.TransportName(t, srvAxis)
	srv := server.New(append(append([]argos.ServerOption{argos.WithServerOptions(cfg)}, extra...),
		argos.WithServerService(ServiceName,
			argos.ServiceTransport(srvTr), argos.ServiceCodec("raw"),
			argos.ServiceListenAddress(cfg.ListenAddress),
		))...)

	methods := []descriptor.Method{
		descriptor.MustMethod(MethodPing, descriptor.Unary),
		descriptor.MustMethod(MethodEcho, descriptor.Unary),
		descriptor.MustMethod(MethodExclusive, descriptor.BidiStreaming),
	}
	svc := descriptor.MustService(ServiceName, methods...)
	if err := srv.Register(svc, handlers); err != nil {
		t.Fatal(err)
	}
	// A Server stops when Run's ctx is canceled — there is no Server.Close —
	// and the two axes belong to this helper, which constructed them.
	runCtx, stopServer := context.WithCancel(context.Background())
	go func() { _ = srv.Run(runCtx) }()
	addr := waitTCPAddr(t, srvAxis)
	t.Cleanup(func() {
		stopServer()
		_ = srvAxis.Close()
		_ = cliAxis.Close()
	})
	return &synthHarness{addr: addr, srvAxis: srvAxis, cliAxis: cliAxis}
}

func newSynthClient(t *testing.T, h *synthHarness, extra ...argos.ClientOption) *client.Client {
	t.Helper()
	cliTr := teststack.TransportName(t, h.cliAxis)
	cli, err := client.New(append([]argos.ClientOption{
		argos.WithClientOptions(testOptions()),
		argos.WithServiceName(ServiceName),
		argos.WithTransport(cliTr), argos.WithCodec("raw"),
		argos.WithTarget("ip://" + h.addr),
	}, extra...)...)
	if err != nil {
		t.Fatalf("client.New: %v", err)
	}
	return cli
}

func waitTCPAddr(t *testing.T, ax *Transport) string {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if addr := ax.Addr(); addr != nil {
			return addr.String()
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("listener not ready")
	return ""
}

// TestGreetingReceivedBeforeCall proves Handshake sends the greeting before
// AcceptCall, and the client handshake observes it before OpenCall.
func TestGreetingReceivedBeforeCall(t *testing.T) {
	t.Parallel()
	ax := New()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	defer ax.Close()

	go func() {
		_ = ax.Serve(ctx, func(_ context.Context, c transport.ServerConn) {
			hsCtx, hsCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer hsCancel()
			if err := c.Handshake(hsCtx); err != nil {
				_ = c.Close()
				return
			}
			defer c.Close()
			acceptCtx, acceptCancel := context.WithTimeout(ctx, 5*time.Second)
			defer acceptCancel()
			for {
				md := metadata.New(metadata.RoleResponder, func(metadata.Metadata) error {
					return status.Error(status.Unimplemented, "unused")
				})
				sc, err := c.AcceptCall(acceptCtx, transport.CallSpec{Metadata: md})
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

	addr := waitTCPAddr(t, ax)
	conn, err := ax.tr.Dial(context.Background(), transport.DialSpec{Endpoint: addr})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	car, err := assertByteCarrier(conn)
	if err != nil {
		t.Fatal(err)
	}
	st := &ConnState{}
	s := newSession(ax, conn, car, true, st)
	hsCtx, hsCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer hsCancel()
	if err := s.clientHandshake(hsCtx); err != nil {
		t.Fatalf("clientHandshake: %v", err)
	}
	defer s.closeSession()

	if g := st.Greeting(); g != DefaultGreeting {
		t.Fatalf("greeting = %q, want %q (must be read during handshake, before OpenCall)", g, DefaultGreeting)
	}

	cc := &clientConn{session: s}
	call, err := cc.OpenCall(context.Background(), descriptor.MustMethod(MethodPing, descriptor.Unary), transport.CallSpec{})
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
			_ = st.Recv(&in)
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
	h := startSynthServer(t, handlers)
	cli := newSynthClient(t, h)

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

func TestSendHeadersUnimplemented(t *testing.T) {
	t.Parallel()

	type sendHeadersObs struct {
		called bool
		code   status.Code
		err    error
	}
	var observed sendHeadersObs

	handlers := map[string]filter.Handler{
		"Ping": func(ctx context.Context, m descriptor.Method, st stream.Stream) error {
			md, ok := metadata.FromContext(ctx)
			if !ok {
				return status.Error(status.Internal, "synth: missing CallMetadata in handler ctx")
			}
			err := md.SendHeaders()
			observed = sendHeadersObs{called: true, code: status.CodeOf(err), err: err}
			if err == nil {
				return status.Error(status.Internal, "synth: SendHeaders succeeded on a carrier with no header channel")
			}
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
	h := startSynthServer(t, handlers)
	cli := newSynthClient(t, h)
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

	if !observed.called {
		t.Fatal("handler never saw CallMetadata.SendHeaders (Ping was not routed to the handler)")
	}
	if observed.code != status.Unimplemented {
		t.Fatalf("handler SendHeaders = %v (code %v), want Unimplemented", observed.err, observed.code)
	}
}

func TestConnStateFromContext(t *testing.T) {
	t.Parallel()
	var sawGreeting string
	handlers := map[string]filter.Handler{
		"Ping": func(ctx context.Context, m descriptor.Method, st stream.Stream) error {
			stt, ok := FromContext(ctx)
			if !ok || stt == nil {
				return status.Error(status.Internal, "synth: ConnState missing from handler ctx")
			}
			sawGreeting = stt.Greeting()
			var in []byte
			_ = st.Recv(&in)
			_ = st.Recv(&in)
			return st.Send([]byte("ok"))
		},
		"Echo":      func(context.Context, descriptor.Method, stream.Stream) error { return nil },
		"Exclusive": func(context.Context, descriptor.Method, stream.Stream) error { return nil },
	}
	h := startSynthServer(t, handlers)
	cli := newSynthClient(t, h)
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

	if sawGreeting != DefaultGreeting {
		t.Fatalf("handler FromContext greeting = %q, want %q", sawGreeting, DefaultGreeting)
	}
}

func TestConnStateContextWith(t *testing.T) {
	t.Parallel()
	st := &ConnState{}
	connCtx := ContextWith(context.Background(), st)
	hsCtx, cancel := context.WithTimeout(connCtx, 5*time.Second)
	defer cancel()

	ax := New()
	defer ax.Close()
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	go func() {
		_ = ax.Serve(ctx, func(_ context.Context, c transport.ServerConn) {
			if err := c.Handshake(hsCtx); err != nil {
				_ = c.Close()
				return
			}
			<-ctx.Done()
			_ = c.Close()
		}, transport.WithListenAddress("127.0.0.1:0"))
	}()
	addr := waitTCPAddr(t, ax)
	conn, err := ax.tr.Dial(context.Background(), transport.DialSpec{Endpoint: addr})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	car, err := assertByteCarrier(conn)
	if err != nil {
		t.Fatal(err)
	}
	s := newSession(ax, conn, car, true, &ConnState{})
	if err := s.clientHandshake(hsCtx); err != nil {
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

	cliAxis := New()
	srvAxis := New()
	srvTr := teststack.TransportName(t, srvAxis)
	cliTr := teststack.TransportName(t, cliAxis)
	cfg := testOptions()
	srv := server.New(argos.WithServerOptions(cfg), argos.WithServerService(ServiceName,
		argos.ServiceTransport(srvTr), argos.ServiceCodec("raw"),
		argos.ServiceListenAddress(cfg.ListenAddress),
	))
	methods := []descriptor.Method{
		descriptor.MustMethod(MethodPing, descriptor.Unary),
		descriptor.MustMethod(MethodEcho, descriptor.Unary),
		descriptor.MustMethod(MethodExclusive, descriptor.BidiStreaming),
	}
	if err := srv.Register(descriptor.MustService(ServiceName, methods...), handlers); err != nil {
		t.Fatal(err)
	}
	runCtx, stopServer := context.WithCancel(context.Background())
	go func() { _ = srv.Run(runCtx) }()
	addr := waitTCPAddr(t, srvAxis)
	t.Cleanup(func() {
		stopServer()
		_ = srvAxis.Close()
		_ = cliAxis.Close()
	})

	cli, err := client.New(
		argos.WithClientOptions(testOptions()),
		argos.WithServiceName(ServiceName),
		argos.WithTransport(cliTr), argos.WithCodec("raw"),
		argos.WithTarget("ip://"+addr),
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if got := cliAxis.Dials(); got != 0 {
		t.Fatalf("dials before exclusive Open = %d, want 0", got)
	}

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
	dialsAfterExclusive := cliAxis.Dials()
	if dialsAfterExclusive != 1 {
		t.Fatalf("dials after exclusive call = %d, want exactly 1", dialsAfterExclusive)
	}

	cs, err = cli.Open(ctx, descriptor.MustMethod(MethodPing, descriptor.Unary))
	if err != nil {
		t.Fatalf("Open Ping: %v", err)
	}
	_ = cs.Send([]byte("x"))
	_ = cs.HalfClose()
	_ = cs.Recv(&out)
	_ = cs.Recv(&out)
	_ = cs.Close()

	if got := cliAxis.Dials(); got <= dialsAfterExclusive {
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
	h := startSynthServer(t, handlers)
	cli := newSynthClient(t, h)
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
	if got := h.cliAxis.Dials(); got != 1 {
		t.Fatalf("dials = %d, want 1 (Sequential reuse)", got)
	}
}

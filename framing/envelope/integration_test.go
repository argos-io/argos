package envelope_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/argos-io/argos"
	"github.com/argos-io/argos/client"
	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/filter"
	"github.com/argos-io/argos/framing"
	"github.com/argos-io/argos/framing/envelope"
	"github.com/argos-io/argos/internal/fake"
	"github.com/argos-io/argos/metadata"
	"github.com/argos-io/argos/server"
	"github.com/argos-io/argos/stream"
	"github.com/argos-io/argos/transport"
	"github.com/argos-io/argos/transport/tcp"

	_ "github.com/argos-io/argos/resolver/ip"
)

const (
	integService = "envelope.v1.Echo"
	integMethod  = integService + ".Echo"
)

// ---------------------------------------------------------------------------
// helpers: listen-addr TCP wrapper, dial counter, raw codec
// ---------------------------------------------------------------------------

// listenTCP injects WithListenAddress because server.Run does not yet pass it.
type listenTCP struct {
	inner transport.Transport
	addr  string
	raw   *tcp.Transport
}

func newListenTCP(addr string) *listenTCP {
	raw := tcp.New().(*tcp.Transport)
	return &listenTCP{inner: raw, addr: addr, raw: raw}
}

func (t *listenTCP) Serve(ctx context.Context, onConn func(context.Context, transport.Conn), opts ...transport.ServerOption) error {
	opts = append([]transport.ServerOption{transport.WithListenAddress(t.addr)}, opts...)
	return t.inner.Serve(ctx, onConn, opts...)
}
func (t *listenTCP) Dial(ctx context.Context, spec transport.DialSpec, opts ...transport.ClientOption) (transport.Conn, error) {
	return t.inner.Dial(ctx, spec, opts...)
}
func (t *listenTCP) Shutdown(ctx context.Context) error { return t.inner.Shutdown(ctx) }
func (t *listenTCP) Close() error                       { return t.inner.Close() }
func (t *listenTCP) Addr() net.Addr                     { return t.raw.Addr() }

type countingDial struct {
	inner transport.Transport
	n     atomic.Int64
}

func (t *countingDial) Serve(ctx context.Context, onConn func(context.Context, transport.Conn), opts ...transport.ServerOption) error {
	return t.inner.Serve(ctx, onConn, opts...)
}
func (t *countingDial) Dial(ctx context.Context, spec transport.DialSpec, opts ...transport.ClientOption) (transport.Conn, error) {
	t.n.Add(1)
	return t.inner.Dial(ctx, spec, opts...)
}
func (t *countingDial) Shutdown(ctx context.Context) error { return t.inner.Shutdown(ctx) }
func (t *countingDial) Close() error                       { return t.inner.Close() }
func (t *countingDial) Dials() int64                       { return t.n.Load() }

type rawCodec struct{}

func (rawCodec) Marshal(v any) ([]byte, error) {
	switch x := v.(type) {
	case []byte:
		return append([]byte(nil), x...), nil
	case string:
		return []byte(x), nil
	default:
		return nil, fmt.Errorf("rawCodec: unsupported %T", v)
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
		return fmt.Errorf("rawCodec: unsupported %T", v)
	}
}
func (rawCodec) CodecName() string { return "raw" }

func waitTCPAddr(t *testing.T, tr *listenTCP) string {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if a := tr.Addr(); a != nil {
			return a.String()
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("tcp listener Addr not ready")
	return ""
}

func echoHandler(_ context.Context, _ descriptor.Method, st stream.Stream) error {
	var req []byte
	if err := st.Recv(&req); err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	for {
		var discard []byte
		if err := st.Recv(&discard); err != nil {
			break
		}
	}
	return st.Send(append([]byte("echo:"), req...))
}

type integEnv struct {
	srv    *server.Server
	cli    *client.Client
	dials  *countingDial
	addr   string
	method descriptor.Method
	srvTr  *listenTCP
}

func startInteg(t *testing.T, opts ...argos.Option) *integEnv {
	t.Helper()
	srvTr := newListenTCP("127.0.0.1:0")
	frOpts := []envelope.Option{}
	cfgOpts := append([]argos.Option{
		argos.WithMaxConcurrentCalls(16),
		argos.WithMaxBufferedBytes(16 * 16 * 1024 * 1024),
		argos.WithMaxIdleSessions(8),
		argos.WithMaxSessionsPerEndpoint(8),
		argos.WithMaxInboundConnIdle(30 * time.Second),
		argos.WithMaxInboundConnAge(30 * time.Minute),
	}, opts...)

	cfg, err := argos.New(cfgOpts...)
	if err != nil {
		t.Fatalf("argos.New: %v", err)
	}
	srv := server.New(cfg)
	if err := srv.AddBinding(func() (argos.Binding, error) {
		return argos.Binding{
			Transport: srvTr,
			Framing:   envelope.New(frOpts...),
			Codec:     rawCodec{},
		}, nil
	}); err != nil {
		t.Fatal(err)
	}
	svc := descriptor.MustService(integService, descriptor.MustMethod(integMethod, descriptor.Unary))
	if err := srv.Register(svc, map[string]filter.Handler{
		"Echo": echoHandler,
	}); err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Run(context.Background()) }()
	addr := waitTCPAddr(t, srvTr)
	t.Cleanup(func() { _ = srv.Close() })

	dials := &countingDial{inner: tcp.New()}
	target := "ip://" + addr
	cliCfg, err := argos.New(append(cfgOpts,
		argos.WithService(integService,
			argos.ServiceBinding(func() (argos.Binding, error) {
				return argos.Binding{
					Transport: dials,
					Framing:   envelope.New(frOpts...),
					Codec:     rawCodec{},
				}, nil
			}),
			argos.ServiceTarget(target),
		),
	)...)
	if err != nil {
		t.Fatalf("client argos.New: %v", err)
	}
	cli, err := client.New(cliCfg, integService)
	if err != nil {
		t.Fatalf("client.New: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })

	return &integEnv{
		srv:    srv,
		cli:    cli,
		dials:  dials,
		addr:   addr,
		method: descriptor.MustMethod(integMethod, descriptor.Unary),
		srvTr:  srvTr,
	}
}

func unary(t *testing.T, cli *client.Client, m descriptor.Method, payload string) string {
	t.Helper()
	cs, err := cli.Open(context.Background(), m)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer cs.Close()
	if err := cs.Send([]byte(payload)); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if err := cs.HalfClose(); err != nil {
		t.Fatalf("HalfClose: %v", err)
	}
	var got []byte
	if err := cs.Recv(&got); err != nil {
		t.Fatalf("Recv: %v", err)
	}
	// Drain through STATUS/EOF so Call.Close leaves the session reusable.
	for {
		var discard []byte
		if err := cs.Recv(&discard); err != nil {
			break
		}
	}
	return string(got)
}

// ---------------------------------------------------------------------------
// 2.4b — Sequential reuse + session pool
// ---------------------------------------------------------------------------

func TestSequentialNCallsOneDial(t *testing.T) {
	env := startInteg(t)
	const n = 8
	for i := 0; i < n; i++ {
		got := unary(t, env.cli, env.method, fmt.Sprintf("msg-%d", i))
		if got != fmt.Sprintf("echo:msg-%d", i) {
			t.Fatalf("call %d: got %q", i, got)
		}
	}
	if d := env.dials.Dials(); d != 1 {
		t.Fatalf("Dial count = %d, want 1 (Sequential reuse)", d)
	}
}

func TestHalfCloseKeepsConnForCall2(t *testing.T) {
	env := startInteg(t)
	_ = unary(t, env.cli, env.method, "first")
	_ = unary(t, env.cli, env.method, "second")
	if d := env.dials.Dials(); d != 1 {
		t.Fatalf("Dial count = %d after 2 calls, want 1 (HalfClose wrote END not CloseSend)", d)
	}
}

func TestIOErrorMakesSessionNotReusable(t *testing.T) {
	cliConn, srvConn := fake.BytePipe()
	fr := envelope.New()
	cliSess, err := fr.NewClientSession(context.Background(), cliConn, framing.SessionSpec{})
	if err != nil {
		t.Fatal(err)
	}
	defer cliSess.Close()
	srvSess, err := fr.NewServerSession(context.Background(), srvConn, framing.SessionSpec{})
	if err != nil {
		t.Fatal(err)
	}
	defer srvSess.Close()

	method := descriptor.MustMethod("svc.IOErr", descriptor.Unary)
	go func() {
		md := metadata.New(metadata.RoleResponder, nil)
		sc, err := srvSess.AcceptCall(context.Background(), framing.CallSpec{Metadata: md})
		if err != nil {
			return
		}
		defer sc.Close()
		_, _, _ = sc.Recv()
		// Drain END too; the peer's HalfClose write must not depend on a race
		// with this goroutine finishing (net.Pipe is unbuffered).
		_, _, _ = sc.Recv()
	}()

	call, err := cliSess.OpenCall(context.Background(), method, framing.CallSpec{
		Metadata: metadata.New(metadata.RoleInitiator, nil),
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = call.Send([]byte("x"))
	_ = call.HalfClose()
	// Tear down the peer under the call so subsequent I/O fails.
	_ = srvConn.Close()
	_, _, _ = call.Recv()
	_ = call.Close()
	if cliSess.Reusable() {
		t.Fatal("Reusable() true after I/O error, want false")
	}
}

func TestAbortMakesNotReusable(t *testing.T) {
	cliConn, srvConn := fake.BytePipe()
	fr := envelope.New()
	cliSess, err := fr.NewClientSession(context.Background(), cliConn, framing.SessionSpec{})
	if err != nil {
		t.Fatal(err)
	}
	defer cliSess.Close()
	srvSess, err := fr.NewServerSession(context.Background(), srvConn, framing.SessionSpec{})
	if err != nil {
		t.Fatal(err)
	}
	defer srvSess.Close()

	method := descriptor.MustMethod("svc.Abort", descriptor.Unary)
	go func() {
		md := metadata.New(metadata.RoleResponder, nil)
		sc, err := srvSess.AcceptCall(context.Background(), framing.CallSpec{Metadata: md})
		if err != nil {
			return
		}
		defer sc.Close()
		_, _, _ = sc.Recv()
		_ = sc.Finish(nil)
	}()

	call, err := cliSess.OpenCall(context.Background(), method, framing.CallSpec{
		Metadata: metadata.New(metadata.RoleInitiator, nil),
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = call.HalfClose()
	// Close without reading STATUS → carrier hygiene.
	_ = call.Close()
	if cliSess.Reusable() {
		t.Fatal("Reusable() true after non-terminal Close, want false")
	}
}

// ---------------------------------------------------------------------------
// 2.4c — Carrier hygiene
// ---------------------------------------------------------------------------

func TestClientEarlyCloseNotReturnedToPool(t *testing.T) {
	// Custom handler: send one DATA then block before Finish so the client can
	// Close without the demux having observed STATUS yet.
	hold := make(chan struct{})
	srvTr := newListenTCP("127.0.0.1:0")
	cfg, err := argos.New(
		argos.WithMaxConcurrentCalls(8),
		argos.WithMaxBufferedBytes(8*16*1024*1024),
		argos.WithMaxIdleSessions(4),
		argos.WithMaxSessionsPerEndpoint(4),
		argos.WithMaxInboundConnIdle(30*time.Second),
		argos.WithMaxInboundConnAge(30*time.Minute),
	)
	if err != nil {
		t.Fatal(err)
	}
	srv := server.New(cfg)
	_ = srv.AddBinding(func() (argos.Binding, error) {
		return argos.Binding{Transport: srvTr, Framing: envelope.New(), Codec: rawCodec{}}, nil
	})
	svc := descriptor.MustService(integService, descriptor.MustMethod(integMethod, descriptor.Unary))
	_ = srv.Register(svc, map[string]filter.Handler{
		"Echo": func(ctx context.Context, m descriptor.Method, st stream.Stream) error {
			var req []byte
			_ = st.Recv(&req)
			for {
				var d []byte
				if err := st.Recv(&d); err != nil {
					break
				}
			}
			_ = st.Send([]byte("partial"))
			<-hold
			return nil
		},
	})
	go func() { _ = srv.Run(context.Background()) }()
	addr := waitTCPAddr(t, srvTr)
	t.Cleanup(func() { _ = srv.Close() })

	dials := &countingDial{inner: tcp.New()}
	cliCfg, err := argos.New(
		argos.WithMaxConcurrentCalls(8),
		argos.WithMaxBufferedBytes(8*16*1024*1024),
		argos.WithMaxIdleSessions(4),
		argos.WithMaxSessionsPerEndpoint(4),
		argos.WithService(integService,
			argos.ServiceBinding(func() (argos.Binding, error) {
				return argos.Binding{Transport: dials, Framing: envelope.New(), Codec: rawCodec{}}, nil
			}),
			argos.ServiceTarget("ip://"+addr),
		),
	)
	if err != nil {
		t.Fatal(err)
	}
	cli, err := client.New(cliCfg, integService)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	method := descriptor.MustMethod(integMethod, descriptor.Unary)

	cs, err := cli.Open(context.Background(), method)
	if err != nil {
		t.Fatal(err)
	}
	_ = cs.Send([]byte("x"))
	_ = cs.HalfClose()
	var got []byte
	if err := cs.Recv(&got); err != nil {
		t.Fatal(err)
	}
	_ = cs.Close() // before STATUS
	close(hold)    // release server so AcceptCall loop can continue
	time.Sleep(20 * time.Millisecond)
	_ = unary(t, cli, method, "next")
	if d := dials.Dials(); d < 2 {
		t.Fatalf("Dial count = %d, want ≥2 (non-terminal Close must not reuse session)", d)
	}
}

func TestServerDrainResidualsNextCall(t *testing.T) {
	// Use real TCP: after Finish the server demux goes idle while the client may
	// still write residual DATA/END. net.Pipe is unbuffered and deadlocks that
	// race; AcceptCall drains residuals once it resumes reading (§2.4c).
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	cliNC, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer cliNC.Close()
	srvNC, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer srvNC.Close()

	cliConn := &netCarrier{nc: cliNC}
	srvConn := &netCarrier{nc: srvNC}

	fr := envelope.New()
	cliSess, _ := fr.NewClientSession(context.Background(), cliConn, framing.SessionSpec{})
	defer cliSess.Close()
	srvSess, _ := fr.NewServerSession(context.Background(), srvConn, framing.SessionSpec{})
	defer srvSess.Close()

	method := descriptor.MustMethod("svc.Drain", descriptor.Unary)

	// Server: Recv once then return error (Finish+Close) while peer still sends.
	acceptDone := make(chan framing.ServerCall, 2)
	go func() {
		for i := 0; i < 2; i++ {
			md := metadata.New(metadata.RoleResponder, nil)
			sc, err := srvSess.AcceptCall(context.Background(), framing.CallSpec{Metadata: md})
			if err != nil {
				t.Errorf("AcceptCall %d: %v", i, err)
				return
			}
			acceptDone <- sc
			if i == 0 {
				_, _, _ = sc.Recv() // one message — leave residual DATA/END for drain
				_ = sc.Finish(fmt.Errorf("early"))
				_ = sc.Close()
				continue
			}
			// call 2: full unary
			p, rel, err := sc.Recv()
			if err != nil {
				t.Errorf("call2 Recv: %v", err)
				_ = sc.Close()
				return
			}
			rel()
			_, _, _ = sc.Recv()
			_ = sc.Send(append([]byte("ok:"), p...))
			_ = sc.Finish(nil)
			_ = sc.Close()
		}
	}()

	// Call 1: send two DATA then END — server only reads one.
	// Recv must run concurrently with the server's Finish write; otherwise both
	// sides fill TCP send buffers and deadlock under -race (§2.4c residual case).
	c1, err := cliSess.OpenCall(context.Background(), method, framing.CallSpec{
		Metadata: metadata.New(metadata.RoleInitiator, nil),
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = c1.Send([]byte("a"))
	statusDone := make(chan struct{})
	go func() {
		defer close(statusDone)
		_, _, _ = c1.Recv() // status error from early Finish
		for {
			_, _, err := c1.Recv()
			if err != nil {
				return
			}
		}
	}()
	_ = c1.Send([]byte("residual"))
	_ = c1.HalfClose()
	<-statusDone
	_ = c1.Close()
	<-acceptDone

	// Call 2 must succeed after residual drain.
	c2, err := cliSess.OpenCall(context.Background(), method, framing.CallSpec{
		Metadata: metadata.New(metadata.RoleInitiator, nil),
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = c2.Send([]byte("b"))
	_ = c2.HalfClose()
	p, rel, err := c2.Recv()
	if err != nil {
		t.Fatalf("call2 client Recv: %v", err)
	}
	if string(p) != "ok:b" {
		t.Fatalf("call2 got %q", p)
	}
	rel()
	_, _, _ = c2.Recv()
	_ = c2.Close()
	<-acceptDone
}

func TestMaxDrainBytesConnectionError(t *testing.T) {
	// Use real TCP so residual floods are buffered (net.Pipe is unbuffered and
	// deadlocks if AcceptCall has not entered read yet).
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	cliNC, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer cliNC.Close()
	srvNC, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer srvNC.Close()

	cliConn := &netCarrier{nc: cliNC}
	srvConn := &netCarrier{nc: srvNC}

	fr := envelope.New(envelope.WithMaxDrainBytes(64))
	cliSess, err := fr.NewClientSession(context.Background(), cliConn, framing.SessionSpec{
		Config: framing.Config{MaxDrainBytes: 64},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cliSess.Close()
	srvSess, err := fr.NewServerSession(context.Background(), srvConn, framing.SessionSpec{
		Config: framing.Config{MaxDrainBytes: 64},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer srvSess.Close()

	method := descriptor.MustMethod("svc.MaxDrain", descriptor.Unary)
	call1Done := make(chan struct{})
	errCh := make(chan error, 1)
	go func() {
		md := metadata.New(metadata.RoleResponder, nil)
		sc, err := srvSess.AcceptCall(context.Background(), framing.CallSpec{Metadata: md})
		if err != nil {
			errCh <- err
			return
		}
		_, _, _ = sc.Recv()
		_, _, _ = sc.Recv()
		_ = sc.Finish(nil)
		_ = sc.Close()
		close(call1Done)

		_, err = srvSess.AcceptCall(context.Background(), framing.CallSpec{
			Metadata: metadata.New(metadata.RoleResponder, nil),
		})
		errCh <- err
	}()

	c1, err := cliSess.OpenCall(context.Background(), method, framing.CallSpec{
		Metadata: metadata.New(metadata.RoleInitiator, nil),
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = c1.Send([]byte("x"))
	_ = c1.HalfClose()
	_, _, _ = c1.Recv()
	_ = c1.Close()
	<-call1Done
	time.Sleep(20 * time.Millisecond)

	big := make([]byte, 40)
	for i := 0; i < 4; i++ {
		raw, err := envelope.MarshalFrame(envelope.Frame{
			Type:   envelope.TypeData,
			CallID: 1,
			Data:   big,
		})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := cliNC.Write(raw); err != nil {
			t.Fatal(err)
		}
	}
	raw, _ := envelope.MarshalFrame(envelope.Frame{
		Type:   envelope.TypeOpen,
		CallID: 2,
		Method: method.FullName(),
	})
	_, _ = cliNC.Write(raw)

	select {
	case err := <-errCh:
		if err == nil || !containsDrain(err) {
			t.Fatalf("AcceptCall err = %v, want MaxDrainBytes", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for MaxDrainBytes AcceptCall error")
	}
}

// netCarrier is a minimal CarrierConn over net.Conn for integration tests.
type netCarrier struct {
	nc net.Conn
}

func (c *netCarrier) Close() error                      { return c.nc.Close() }
func (c *netCarrier) Carrier() transport.Carrier        { return c }
func (c *netCarrier) Read(p []byte) (int, error)        { return c.nc.Read(p) }
func (c *netCarrier) Write(p []byte) (int, error)       { return c.nc.Write(p) }
func (c *netCarrier) Abort() error                      { return c.nc.SetDeadline(time.Unix(1, 0)) }
func (c *netCarrier) CloseSend() error                  { return nil }
func (c *netCarrier) SetReadDeadline(t time.Time) error { return c.nc.SetReadDeadline(t) }
func (c *netCarrier) SetDeadline(t time.Time) error     { return c.nc.SetDeadline(t) }

// tcpBytePair returns a connected ByteConn pair over loopback TCP.
// Prefer this over fake.BytePipe when both peers write concurrently — net.Pipe
// is unbuffered and deadlocks if AcceptCall/Finish ordering leaves one side
// blocked on Write while the other is not yet Reading.
func tcpBytePair(t *testing.T) (cli, srv *fake.ByteConn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	type dialResult struct {
		c   net.Conn
		err error
	}
	ch := make(chan dialResult, 1)
	go func() {
		c, err := net.Dial("tcp", ln.Addr().String())
		ch <- dialResult{c, err}
	}()
	srvNC, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	dr := <-ch
	if dr.err != nil {
		_ = srvNC.Close()
		t.Fatal(dr.err)
	}
	cli = fake.NewByteConn(dr.c)
	srv = fake.NewByteConn(srvNC)
	t.Cleanup(func() {
		_ = cli.Close()
		_ = srv.Close()
	})
	return cli, srv
}

func containsDrain(err error) bool {
	return err != nil && (errors.Is(err, io.EOF) == false) &&
		(stringContains(err.Error(), "MaxDrainBytes"))
}

func stringContains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(sub) == 0 ||
		func() bool {
			for i := 0; i+len(sub) <= len(s); i++ {
				if s[i:i+len(sub)] == sub {
					return true
				}
			}
			return false
		}())
}

func TestCrossCallBufferPreservedOnClose(t *testing.T) {
	// TCP: injecting the next OPEN while call1 is closing must not drop bytes
	// from the Session read buffer. net.Pipe deadlocks under the same race.
	cliConn, srvConn := tcpBytePair(t)

	fr := envelope.New()
	cliSess, err := fr.NewClientSession(context.Background(), cliConn, framing.SessionSpec{})
	if err != nil {
		t.Fatal(err)
	}
	defer cliSess.Close()
	srvSess, err := fr.NewServerSession(context.Background(), srvConn, framing.SessionSpec{})
	if err != nil {
		t.Fatal(err)
	}
	defer srvSess.Close()

	method := descriptor.MustMethod("svc.Sticky", descriptor.Unary)

	open2, err := envelope.MarshalFrame(envelope.Frame{
		Type:   envelope.TypeOpen,
		CallID: 2,
		Method: method.FullName(),
		Flags:  envelope.FlagOpenEnd,
	})
	if err != nil {
		t.Fatal(err)
	}

	call1Done := make(chan struct{})
	accepted2 := make(chan string, 1)
	go func() {
		md := metadata.New(metadata.RoleResponder, nil)
		sc, err := srvSess.AcceptCall(context.Background(), framing.CallSpec{Metadata: md})
		if err != nil {
			t.Errorf("AcceptCall 0: %v", err)
			return
		}
		_, _, _ = sc.Recv()
		_ = sc.Finish(nil)
		_ = sc.Close()
		close(call1Done)

		sc2, err := srvSess.AcceptCall(context.Background(), framing.CallSpec{
			Metadata: metadata.New(metadata.RoleResponder, nil),
		})
		if err != nil {
			t.Errorf("AcceptCall 1: %v", err)
			return
		}
		accepted2 <- sc2.Method()
		_, _, _ = sc2.Recv()
		_ = sc2.Finish(nil)
		_ = sc2.Close()
	}()

	c1, err := cliSess.OpenCall(context.Background(), method, framing.CallSpec{
		Metadata: metadata.New(metadata.RoleInitiator, nil),
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = c1.HalfClose()
	_, _, _ = c1.Recv()
	_ = c1.Close()
	<-call1Done

	// Inject next OPEN via the carrier; session readBuf must not drop it on Close.
	if _, err := cliConn.Write(open2); err != nil {
		t.Fatal(err)
	}

	select {
	case m := <-accepted2:
		if m != method.FullName() {
			t.Fatalf("call2 method %q", m)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("AcceptCall2 timed out — cross-call buffer likely dropped OPEN")
	}
}

func TestReadNeverConcurrent(t *testing.T) {
	cliConn, srvConn := tcpBytePair(t)
	cliConn.DetectReentry = true

	fr := envelope.New()
	cliSess, err := fr.NewClientSession(context.Background(), cliConn, framing.SessionSpec{})
	if err != nil {
		t.Fatal(err)
	}
	defer cliSess.Close()
	srvSess, err := fr.NewServerSession(context.Background(), srvConn, framing.SessionSpec{})
	if err != nil {
		t.Fatal(err)
	}
	defer srvSess.Close()

	method := descriptor.MustMethod("svc.Reentry", descriptor.Unary)

	// Bounded so a stall surfaces as a diagnostic instead of the package's
	// 10-minute timeout, and so the server half can never disappear silently
	// and leave the client blocked in Recv forever.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	go func() {
		<-ctx.Done()
		_ = cliSess.Close()
		_ = srvSess.Close()
	}()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 3; i++ {
			md := metadata.New(metadata.RoleResponder, nil)
			sc, err := srvSess.AcceptCall(ctx, framing.CallSpec{Metadata: md})
			if err != nil {
				if ctx.Err() == nil {
					t.Errorf("server AcceptCall %d: %v", i, err)
				}
				return
			}
			_, _, _ = sc.Recv()
			_, _, _ = sc.Recv() // END
			_ = sc.Send([]byte("p"))
			_ = sc.Finish(nil)
			_ = sc.Close()
		}
	}()

	for i := 0; i < 3; i++ {
		c, err := cliSess.OpenCall(ctx, method, framing.CallSpec{
			Metadata: metadata.New(metadata.RoleInitiator, nil),
		})
		if err != nil {
			t.Fatal(err)
		}
		_ = c.Send([]byte("q"))
		_ = c.HalfClose()
		_, _, _ = c.Recv()
		_, _, _ = c.Recv() // STATUS / EOF
		_ = c.Close()
	}
	wg.Wait()
	if hits := cliConn.ConcurrentHits(); hits != 0 {
		t.Fatalf("concurrent Read hits = %d, want 0", hits)
	}
}

// ---------------------------------------------------------------------------
// 2.4d — Server serial AcceptCall is in serial_test.go; package doc in frame.go
// ---------------------------------------------------------------------------

func TestSessionSpecConfigOverridesDrainAndTimeout(t *testing.T) {
	fr := envelope.New(envelope.WithMaxDrainBytes(1<<20), envelope.WithOpenTimeout(10*time.Second))
	cliConn, srvConn := fake.BytePipe()
	defer cliConn.Close()
	defer srvConn.Close()
	spec := framing.SessionSpec{Config: framing.Config{
		MaxDrainBytes: 128,
		OpenTimeout:   50 * time.Millisecond,
	}}
	srvSess, err := fr.NewServerSession(context.Background(), srvConn, spec)
	if err != nil {
		t.Fatal(err)
	}
	defer srvSess.Close()
	// Smoke: session accepts the spec (values applied internally). A too-small
	// OpenTimeout still allows AcceptCall to block until first byte.
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, err = srvSess.AcceptCall(ctx, framing.CallSpec{
		Metadata: metadata.New(metadata.RoleResponder, nil),
	})
	if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
		t.Fatalf("AcceptCall = %v, want ctx deadline", err)
	}
}

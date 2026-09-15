package resp_test

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/argos-io/argos"
	"github.com/argos-io/argos/client"
	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/example/resp"
	"github.com/argos-io/argos/filter"
	"github.com/argos-io/argos/metadata"
	"github.com/argos-io/argos/server"
	"github.com/argos-io/argos/status"
	"github.com/argos-io/argos/stream"
	"github.com/argos-io/argos/transport"
	"github.com/argos-io/argos/transport/tcp"

	_ "github.com/argos-io/argos/resolver/ip"
)

// Task 7.2b: these tests document that example/resp required zero changes to
// public interfaces in transport/, framing/ (root), client/, or server/.
// Only example/resp was added.

const (
	svcName        = "redis.Redis"
	testListenAddr = "127.0.0.1:0"
)

type hasAddr interface {
	Addr() net.Addr
}

func baseOpts(extra ...argos.Option) []argos.Option {
	opts := []argos.Option{
		argos.WithMaxConcurrentCalls(16),
		argos.WithMaxBufferedBytes(16 * 16 * 1024 * 1024),
		argos.WithMaxIdleSessions(8),
		argos.WithMaxSessionsPerEndpoint(8),
		argos.WithHandshakeTimeout(5 * time.Second),
		argos.WithListenAddress(testListenAddr),
	}
	return append(opts, extra...)
}

func waitAddr(t *testing.T, a hasAddr) string {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if addr := a.Addr(); addr != nil {
			return addr.String()
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("listener Addr not ready")
	return ""
}

// dialCounter wraps a Transport to count Dial calls (TCP connections).
type dialCounter struct {
	transport.Transport
	dials *atomic.Int64
}

func (d *dialCounter) Dial(ctx context.Context, spec transport.DialSpec, opts ...transport.ClientOption) (transport.Conn, error) {
	d.dials.Add(1)
	return d.Transport.Dial(ctx, spec, opts...)
}

type harness struct {
	cli   *client.Client
	fr    *resp.Framing
	store *resp.Store
	dials *atomic.Int64
}

func startRESP(t *testing.T, register func(*server.Server, *resp.Store) error, frOpts ...resp.Option) *harness {
	t.Helper()
	if register == nil {
		register = resp.Register
	}

	store := resp.NewStore()
	var clientFr *resp.Framing
	var dials atomic.Int64
	var addrTr hasAddr
	bound := make(chan struct{})

	serverFn := func() (argos.Binding, error) {
		fr := resp.New(frOpts...)
		tr := tcp.New()
		addrTr = tr.(hasAddr)
		select {
		case <-bound:
		default:
			close(bound)
		}
		return argos.Binding{
			Transport: tr,
			Framing:   fr,
			Codec:     resp.NewBytesCodec(),
		}, nil
	}

	clientFn := func() (argos.Binding, error) {
		clientFr = resp.New(frOpts...)
		tr := tcp.New()
		return argos.Binding{
			Transport: &dialCounter{Transport: tr, dials: &dials},
			Framing:   clientFr,
			Codec:     resp.NewBytesCodec(),
		}, nil
	}

	cfg, err := argos.New(baseOpts()...)
	if err != nil {
		t.Fatalf("argos.New: %v", err)
	}
	srv := server.New(cfg)
	if err := srv.AddBinding(serverFn); err != nil {
		t.Fatal(err)
	}
	if err := register(srv, store); err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Run(context.Background()) }()
	select {
	case <-bound:
	case <-time.After(3 * time.Second):
		t.Fatal("server BindingFunc not invoked")
	}
	addr := waitAddr(t, addrTr)
	t.Cleanup(func() { _ = srv.Close() })

	cliCfg, err := argos.New(append(baseOpts(),
		argos.WithService(svcName,
			argos.ServiceBinding(clientFn),
			argos.ServiceTarget("ip://"+addr),
		),
	)...)
	if err != nil {
		t.Fatalf("client argos.New: %v", err)
	}
	cli, err := client.New(cliCfg, svcName)
	if err != nil {
		t.Fatalf("client.New: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })

	return &harness{cli: cli, fr: clientFr, store: store, dials: &dials}
}

func doCall(t *testing.T, h *harness, m descriptor.Method, args []byte) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	st, err := h.cli.Open(ctx, m)
	if err != nil {
		t.Fatalf("Open(%s): %v", m.Name(), err)
	}
	defer st.Close()

	if err := st.Send(args); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if err := st.HalfClose(); err != nil {
		t.Fatalf("HalfClose: %v", err)
	}
	var out []byte
	if err := st.Recv(&out); err != nil {
		t.Fatalf("Recv: %v", err)
	}
	return out
}

func TestHELLOOncePerSession(t *testing.T) {
	h := startRESP(t, nil)

	out := doCall(t, h, resp.MethodSET, resp.EncodeArgs("k", "v1"))
	if string(out) != string(resp.EncodeSimple("OK")) {
		t.Fatalf("SET reply = %q", out)
	}
	out = doCall(t, h, resp.MethodGET, resp.EncodeArgs("k"))
	if string(out) != string(resp.EncodeBulk("v1")) {
		t.Fatalf("GET reply = %q, want bulk v1", out)
	}

	if got := h.dials.Load(); got != 1 {
		t.Fatalf("TCP dials = %d, want 1 (Sequential reuse)", got)
	}
	if got := h.fr.ClientHellos(); got != 1 {
		t.Fatalf("HELLO count = %d, want 1 (once in NewClientSession)", got)
	}
}

func TestSetGetSameConnection(t *testing.T) {
	h := startRESP(t, nil)

	const key, val = "foo", "bar-value"
	if out := doCall(t, h, resp.MethodSET, resp.EncodeArgs(key, val)); string(out) != string(resp.EncodeSimple("OK")) {
		t.Fatalf("SET: %q", out)
	}
	if out := doCall(t, h, resp.MethodGET, resp.EncodeArgs(key)); string(out) != string(resp.EncodeBulk(val)) {
		t.Fatalf("GET: %q", out)
	}
	if v, ok := h.store.Get(key); !ok || v != val {
		t.Fatalf("store[%q]=%q ok=%v", key, v, ok)
	}
	if h.dials.Load() != 1 {
		t.Fatalf("dials=%d, want 1", h.dials.Load())
	}
}

func TestSendHeadersUnimplementedNextCallWorks(t *testing.T) {
	register := func(srv *server.Server, store *resp.Store) error {
		get := func(ctx context.Context, _ descriptor.Method, st stream.Stream) error {
			md, ok := metadata.FromContext(ctx)
			if !ok {
				t.Error("missing CallMetadata")
			} else {
				err := md.SendHeaders()
				if status.CodeOf(err) != status.Unimplemented {
					t.Errorf("SendHeaders CodeOf=%v err=%v, want Unimplemented", status.CodeOf(err), err)
				}
				// Must not freeze outgoing headers on Unimplemented.
				if addErr := md.AddOutgoingHeader("x-test", "1"); addErr != nil {
					t.Errorf("AddOutgoingHeader after Unimplemented SendHeaders: %v", addErr)
				}
			}
			var raw []byte
			if err := st.Recv(&raw); err != nil {
				return err
			}
			args, err := resp.DecodeArgs(raw)
			if err != nil {
				return err
			}
			if len(args) != 1 {
				return status.Error(status.InvalidArgument, "GET needs 1 arg")
			}
			if v, ok := store.Get(args[0]); ok {
				return st.Send(resp.EncodeBulk(v))
			}
			return st.Send(resp.EncodeBulkNull())
		}
		return srv.Register(descriptor.MustService(svcName,
			resp.MethodPING, resp.MethodGET, resp.MethodSET,
		), map[string]filter.Handler{
			"PING": func(ctx context.Context, m descriptor.Method, st stream.Stream) error {
				var raw []byte
				_ = st.Recv(&raw)
				return st.Send(resp.EncodeSimple("PONG"))
			},
			"GET": get,
			"SET": func(ctx context.Context, m descriptor.Method, st stream.Stream) error {
				var raw []byte
				if err := st.Recv(&raw); err != nil {
					return err
				}
				args, err := resp.DecodeArgs(raw)
				if err != nil {
					return err
				}
				if len(args) != 2 {
					return status.Error(status.InvalidArgument, "SET needs 2 args")
				}
				store.Set(args[0], args[1])
				return st.Send(resp.EncodeSimple("OK"))
			},
		})
	}

	h := startRESP(t, register)
	h.store.Set("x", "1")

	out := doCall(t, h, resp.MethodGET, resp.EncodeArgs("x"))
	if string(out) != string(resp.EncodeBulk("1")) {
		t.Fatalf("GET after SendHeaders: %q", out)
	}
	// Next call on the same Sequential session still works.
	out = doCall(t, h, resp.MethodSET, resp.EncodeArgs("y", "2"))
	if string(out) != string(resp.EncodeSimple("OK")) {
		t.Fatalf("SET after SendHeaders path: %q", out)
	}
	if h.dials.Load() != 1 {
		t.Fatalf("dials=%d, want 1", h.dials.Load())
	}
}

func TestSUBSCRIBEServerStreamingExclusive(t *testing.T) {
	h := startRESP(t, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	st, err := h.cli.Open(ctx, resp.MethodSUBSCRIBE)
	if err != nil {
		t.Fatalf("Open(SUBSCRIBE): %v", err)
	}

	if err := st.Send(resp.EncodeArgs("news")); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if err := st.HalfClose(); err != nil {
		t.Fatalf("HalfClose: %v", err)
	}

	var ack []byte
	if err := st.Recv(&ack); err != nil {
		t.Fatalf("Recv ack: %v", err)
	}
	wantAck := resp.EncodeSubscribeAck("news", 1)
	if string(ack) != string(wantAck) {
		t.Fatalf("ack = %q, want %q", ack, wantAck)
	}

	if got := h.dials.Load(); got != 1 {
		t.Fatalf("dials during SUBSCRIBE = %d, want 1", got)
	}
	if got := h.fr.ClientHellos(); got != 1 {
		t.Fatalf("HELLO during SUBSCRIBE = %d, want 1", got)
	}

	// Inject a push without opening another client call (avoids pooling a
	// second session that would mask the exclusive-close assertion).
	if n := h.store.Publish("news", "hello-push"); n != 1 {
		t.Fatalf("Publish recipients = %d, want 1", n)
	}

	var push []byte
	if err := st.Recv(&push); err != nil {
		t.Fatalf("Recv push: %v", err)
	}
	wantPush := resp.EncodePushMessage("news", "hello-push")
	if string(push) != string(wantPush) {
		t.Fatalf("push = %q, want %q", push, wantPush)
	}

	if err := st.Close(); err != nil {
		t.Fatalf("Close SUBSCRIBE: %v", err)
	}

	// After the exclusive call ends the connection is closed, not returned:
	// the next unary call must HELLO again on a fresh dial.
	out := doCall(t, h, resp.MethodPING, resp.EncodeArgs())
	if string(out) != string(resp.EncodeSimple("PONG")) {
		t.Fatalf("PING after SUBSCRIBE: %q", out)
	}
	if got := h.fr.ClientHellos(); got != 2 {
		t.Fatalf("HELLO after SUBSCRIBE end = %d, want 2 (connection not pooled)", got)
	}
	if got := h.dials.Load(); got != 2 {
		t.Fatalf("dials after SUBSCRIBE end = %d, want 2", got)
	}
}

func TestPING(t *testing.T) {
	h := startRESP(t, nil)
	out := doCall(t, h, resp.MethodPING, resp.EncodeArgs())
	if string(out) != string(resp.EncodeSimple("PONG")) {
		t.Fatalf("PING: %q", out)
	}
}

func TestWireRoundTrip(t *testing.T) {
	raw := resp.EncodeArgs("a", "b")
	args, err := resp.DecodeArgs(raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(args) != 2 || args[0] != "a" || args[1] != "b" {
		t.Fatalf("args=%v", args)
	}
}

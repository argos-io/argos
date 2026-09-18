package resp

import (
	"context"
	"net"
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

// baseOptions is the tuning both halves of a RESP test share. It is an Options
// rather than an option list because the knobs below are split across the two
// sides now — HandshakeTimeout is read by the server, ListenAddress is
// server-only — so no single option slice reaches both constructors, while one
// Options named by argos.WithClientOptions / WithServerOptions does.
//
// What is not here is the pool: an axis fixes it at construction and holds the
// only copy, so a test that wants particular pool numbers passes them to the
// axis instead.
func baseOptions() *argos.Options {
	return &argos.Options{
		MaxConcurrentCalls: 16,
		MaxBufferedBytes:   16 * 16 * 1024 * 1024,
		HandshakeTimeout:   5 * time.Second,
		ListenAddress:      testListenAddr,
	}
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

// The axis counts dials and HELLOs itself: it owns connectivity, so nothing
// outside it can wrap the transport to observe reuse.
type harness struct {
	cli   *client.Client
	ax    *Transport
	store *Store
}

func startRESP(t *testing.T, register func(*server.Server, *Store) error, frOpts ...Option) *harness {
	t.Helper()
	if register == nil {
		register = Register
	}

	cfg := baseOptions()
	store := NewStore()
	// A bare axis is what most of these tests want: the pool limits live on the
	// axis alone, so there is nothing for Options to agree with. A test that
	// cares about pool behaviour names the numbers in frOpts.
	srvAxis := New(frOpts...)
	cliAxis := New(frOpts...)
	srvTr := teststack.TransportName(t, srvAxis)
	cliTr := teststack.TransportName(t, cliAxis)

	srv := server.New(argos.WithServerOptions(cfg), argos.WithServerService(svcName,
		argos.ServiceTransport(srvTr), argos.ServiceCodec("raw"),
		argos.ServiceListenAddress(testListenAddr),
	))
	if err := register(srv, store); err != nil {
		t.Fatal(err)
	}
	// The ctx given to Run is the server's only stop signal: there is no
	// Server.Close, so canceling it is how the test stops the server.
	runCtx, stopServer := context.WithCancel(context.Background())
	go func() { _ = srv.Run(runCtx) }()
	addr := waitAddr(t, srvAxis)
	t.Cleanup(stopServer)

	cli, err := client.New(
		argos.WithClientOptions(cfg),
		argos.WithServiceName(svcName),
		argos.WithTransport(cliTr), argos.WithCodec("raw"),
		argos.WithTarget("ip://"+addr),
	)
	if err != nil {
		t.Fatalf("client.New: %v", err)
	}
	// The Client does not close the axis: an axis may be shared, so whoever
	// built it releases it. The Client itself owns nothing to release.
	t.Cleanup(func() { _ = cliAxis.Close() })

	return &harness{cli: cli, ax: cliAxis, store: store}
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

	out := doCall(t, h, MethodSET, EncodeArgs("k", "v1"))
	if string(out) != string(EncodeSimple("OK")) {
		t.Fatalf("SET reply = %q", out)
	}
	out = doCall(t, h, MethodGET, EncodeArgs("k"))
	if string(out) != string(EncodeBulk("v1")) {
		t.Fatalf("GET reply = %q, want bulk v1", out)
	}

	if got := h.ax.Dials(); got != 1 {
		t.Fatalf("TCP dials = %d, want 1 (Sequential reuse)", got)
	}
	if got := h.ax.ClientHellos(); got != 1 {
		t.Fatalf("HELLO count = %d, want 1 (once in NewClientSession)", got)
	}
}

func TestSetGetSameConnection(t *testing.T) {
	h := startRESP(t, nil)

	const key, val = "foo", "bar-value"
	if out := doCall(t, h, MethodSET, EncodeArgs(key, val)); string(out) != string(EncodeSimple("OK")) {
		t.Fatalf("SET: %q", out)
	}
	if out := doCall(t, h, MethodGET, EncodeArgs(key)); string(out) != string(EncodeBulk(val)) {
		t.Fatalf("GET: %q", out)
	}
	if v, ok := h.store.Get(key); !ok || v != val {
		t.Fatalf("store[%q]=%q ok=%v", key, v, ok)
	}
	if h.ax.Dials() != 1 {
		t.Fatalf("dials=%d, want 1", h.ax.Dials())
	}
}

func TestSendHeadersUnimplementedNextCallWorks(t *testing.T) {
	register := func(srv *server.Server, store *Store) error {
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
			args, err := DecodeArgs(raw)
			if err != nil {
				return err
			}
			if len(args) != 1 {
				return status.Error(status.InvalidArgument, "GET needs 1 arg")
			}
			if v, ok := store.Get(args[0]); ok {
				return st.Send(EncodeBulk(v))
			}
			return st.Send(EncodeBulkNull())
		}
		return srv.Register(descriptor.MustService(svcName,
			MethodPING, MethodGET, MethodSET,
		), map[string]filter.Handler{
			"PING": func(ctx context.Context, m descriptor.Method, st stream.Stream) error {
				var raw []byte
				_ = st.Recv(&raw)
				return st.Send(EncodeSimple("PONG"))
			},
			"GET": get,
			"SET": func(ctx context.Context, m descriptor.Method, st stream.Stream) error {
				var raw []byte
				if err := st.Recv(&raw); err != nil {
					return err
				}
				args, err := DecodeArgs(raw)
				if err != nil {
					return err
				}
				if len(args) != 2 {
					return status.Error(status.InvalidArgument, "SET needs 2 args")
				}
				store.Set(args[0], args[1])
				return st.Send(EncodeSimple("OK"))
			},
		})
	}

	h := startRESP(t, register)
	h.store.Set("x", "1")

	out := doCall(t, h, MethodGET, EncodeArgs("x"))
	if string(out) != string(EncodeBulk("1")) {
		t.Fatalf("GET after SendHeaders: %q", out)
	}
	// Next call on the same Sequential session still works.
	out = doCall(t, h, MethodSET, EncodeArgs("y", "2"))
	if string(out) != string(EncodeSimple("OK")) {
		t.Fatalf("SET after SendHeaders path: %q", out)
	}
	if h.ax.Dials() != 1 {
		t.Fatalf("dials=%d, want 1", h.ax.Dials())
	}
}

func TestSUBSCRIBEServerStreamingExclusive(t *testing.T) {
	h := startRESP(t, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	st, err := h.cli.Open(ctx, MethodSUBSCRIBE)
	if err != nil {
		t.Fatalf("Open(SUBSCRIBE): %v", err)
	}

	if err := st.Send(EncodeArgs("news")); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if err := st.HalfClose(); err != nil {
		t.Fatalf("HalfClose: %v", err)
	}

	var ack []byte
	if err := st.Recv(&ack); err != nil {
		t.Fatalf("Recv ack: %v", err)
	}
	wantAck := EncodeSubscribeAck("news", 1)
	if string(ack) != string(wantAck) {
		t.Fatalf("ack = %q, want %q", ack, wantAck)
	}

	if got := h.ax.Dials(); got != 1 {
		t.Fatalf("dials during SUBSCRIBE = %d, want 1", got)
	}
	if got := h.ax.ClientHellos(); got != 1 {
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
	wantPush := EncodePushMessage("news", "hello-push")
	if string(push) != string(wantPush) {
		t.Fatalf("push = %q, want %q", push, wantPush)
	}

	if err := st.Close(); err != nil {
		t.Fatalf("Close SUBSCRIBE: %v", err)
	}

	// After the exclusive call ends the connection is closed, not returned:
	// the next unary call must HELLO again on a fresh dial.
	out := doCall(t, h, MethodPING, EncodeArgs())
	if string(out) != string(EncodeSimple("PONG")) {
		t.Fatalf("PING after SUBSCRIBE: %q", out)
	}
	if got := h.ax.ClientHellos(); got != 2 {
		t.Fatalf("HELLO after SUBSCRIBE end = %d, want 2 (connection not pooled)", got)
	}
	if got := h.ax.Dials(); got != 2 {
		t.Fatalf("dials after SUBSCRIBE end = %d, want 2", got)
	}
}

func TestPING(t *testing.T) {
	h := startRESP(t, nil)
	out := doCall(t, h, MethodPING, EncodeArgs())
	if string(out) != string(EncodeSimple("PONG")) {
		t.Fatalf("PING: %q", out)
	}
}

func TestWireRoundTrip(t *testing.T) {
	raw := EncodeArgs("a", "b")
	args, err := DecodeArgs(raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(args) != 2 || args[0] != "a" || args[1] != "b" {
		t.Fatalf("args=%v", args)
	}
}

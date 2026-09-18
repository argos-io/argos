package client

import (
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/argos-io/argos"
	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/filter"
	"github.com/argos-io/argos/internal/fake"
	"github.com/argos-io/argos/internal/session"
	"github.com/argos-io/argos/metadata"
	"github.com/argos-io/argos/status"
	"github.com/argos-io/argos/stream"
	"github.com/argos-io/argos/transport"

	_ "github.com/argos-io/argos/resolver/ip"
)

const (
	testService = "echo.v1.Echo"
	testTarget  = "ip://127.0.0.1:9"
)

func testMethod(t *testing.T) descriptor.Method {
	t.Helper()
	return descriptor.MustMethod(testService+".Echo", descriptor.Unary)
}

type bytesCodec struct{}

func (bytesCodec) Marshal(v any) ([]byte, error) {
	return append([]byte(nil), v.([]byte)...), nil
}

func (bytesCodec) Unmarshal(b []byte, v any) error {
	dst := v.(*[]byte)
	*dst = append([]byte(nil), b...)
	return nil
}

// loopTransport dials via a callback; Serve is unused on the client path.
type loopTransport struct {
	dial   func(ctx context.Context, endpoint string) (transport.Conn, error)
	dials  atomic.Int64
	closed atomic.Bool
	closeN atomic.Int64
}

func (t *loopTransport) Dial(ctx context.Context, spec transport.DialSpec, _ ...transport.ClientOption) (transport.Conn, error) {
	t.dials.Add(1)
	return t.dial(ctx, spec.Endpoint)
}

func (t *loopTransport) Serve(context.Context, func(context.Context, transport.Conn), ...transport.ServerOption) error {
	return errors.New("loopTransport: Serve not used")
}

func (t *loopTransport) Shutdown(context.Context) error { return nil }

func (t *loopTransport) Close() error {
	if t.closed.Swap(true) {
		return nil
	}
	t.closeN.Add(1)
	return nil
}

func sequentialLoopback(t *testing.T, dials *atomic.Int64) *fake.Transport {
	return echoLoopback(t, dials)
}

func runEchoServer(t *testing.T, f *fake.Framing, conn transport.Conn) {
	t.Helper()
	sess, err := f.NewServerSession(context.Background(), conn, session.SessionSpec{})
	if err != nil {
		_ = conn.Close()
		return
	}
	defer sess.Close()

	for {
		md := metadata.New(metadata.RoleResponder, nil)
		sc, err := sess.AcceptCall(context.Background(), session.CallSpec{Metadata: md})
		if err != nil {
			return
		}
		// Sequential: finish the call before the next AcceptCall, or the
		// session returns ErrSessionBusy and we would Close the Conn.
		handleEchoCall(sc)
	}
}

func handleEchoCall(sc session.ServerCall) {
	defer sc.Close()
	var payloads [][]byte
	for {
		p, release, err := sc.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			_ = sc.Finish(err)
			return
		}
		payloads = append(payloads, append([]byte(nil), p...))
		release()
	}
	for _, p := range payloads {
		if err := sc.Send(p); err != nil {
			_ = sc.Finish(err)
			return
		}
	}
	_ = sc.Finish(nil)
}

// echoRoundTrip runs one full unary exchange over cli on ctx, closes the call,
// and checks the payload came back.
func echoRoundTrip(t *testing.T, cli *Client, ctx context.Context) {
	t.Helper()
	cs, err := cli.Open(ctx, testMethod(t))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := cs.Send([]byte("ping")); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if err := cs.HalfClose(); err != nil {
		t.Fatalf("HalfClose: %v", err)
	}
	var got []byte
	if err := cs.Recv(&got); err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if string(got) != "ping" {
		t.Fatalf("Recv = %q, want ping", got)
	}
	if err := cs.Recv(&got); !errors.Is(err, io.EOF) {
		t.Fatalf("Recv terminal: %v, want EOF", err)
	}
	if err := cs.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func newTestClient(t *testing.T, opts ...argos.ClientOption) *Client {
	t.Helper()
	// opts come last so a test can retune any of these.
	base := []argos.ClientOption{
		argos.WithServiceName(testService),
		argos.WithMaxConcurrentCalls(8),
		argos.WithMaxBufferedBytes(8 * 16 * 1024 * 1024), // 8 × default perCall
		argos.WithTarget(testTarget),
	}
	cli, err := newClientLoopback(t, sequentialLoopback(t, nil), append(base, opts...)...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return cli
}

func TestUnaryOpenSendHalfCloseRecvClose(t *testing.T) {
	t.Parallel()
	cli := newTestClient(t)
	m := testMethod(t)

	cs, err := cli.Open(context.Background(), m)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer cs.Close()

	req := []byte("hello")
	if err := cs.Send(req); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if err := cs.HalfClose(); err != nil {
		t.Fatalf("HalfClose: %v", err)
	}
	var got []byte
	if err := cs.Recv(&got); err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if string(got) != "hello" {
		t.Fatalf("got %q, want hello", got)
	}
	if err := cs.Recv(&got); !errors.Is(err, io.EOF) {
		t.Fatalf("Recv terminal: %v, want EOF", err)
	}
	if err := cs.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestAdmissionExhausted(t *testing.T) {
	t.Parallel()
	cli := newTestClient(t, argos.WithMaxConcurrentCalls(1))
	m := testMethod(t)

	cs1, err := cli.Open(context.Background(), m)
	if err != nil {
		t.Fatalf("Open #1: %v", err)
	}
	defer cs1.Close()

	_, err = cli.Open(context.Background(), m)
	if !errors.Is(err, status.ErrCallsExhausted) {
		t.Fatalf("Open #2: %v, want ErrCallsExhausted", err)
	}
	if status.CodeOf(err) != status.ResourceExhausted {
		t.Fatalf("CodeOf = %v, want ResourceExhausted", status.CodeOf(err))
	}
}

// A Client needs a service name to select its Services entry and to check the
// method of every Open against it, so New refuses to build one without it
// rather than returning a Client that can never open a call. An empty
// WithServiceName is the same thing as never passing the option.
func TestNewWithoutServiceNameFails(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		opts []argos.ClientOption
	}{
		{name: "option_absent"},
		{name: "empty_name", opts: []argos.ClientOption{argos.WithServiceName("")}},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			opts := append(tc.opts, argos.WithTarget(testTarget))
			_, err := newClientLoopback(t, sequentialLoopback(t, nil), opts...)
			if err == nil {
				t.Fatal("New built a Client that has no service to open calls for")
			}
			if !strings.Contains(err.Error(), "missing service name") {
				t.Fatalf("error %q, want a missing service name report", err)
			}
		})
	}
}

func TestNewFailsWhenConcurrentTimesPerCallExceedsBuffered(t *testing.T) {
	t.Parallel()
	// Force conflict via options: 128 × 16MiB >> 1GiB default buffered after
	// raising concurrent without raising MaxBufferedBytes.
	_, err := newClientLoopback(t, sequentialLoopback(t, nil),
		argos.WithServiceName(testService),
		argos.WithMaxConcurrentCalls(128),
		argos.WithTarget(testTarget),
	)
	if err == nil {
		t.Fatal("expected options validation error")
	}
	msg := err.Error()
	// The cross-check is the root package's now, so the message says "argos:";
	// New no longer keeps a copy of it.
	if !strings.HasPrefix(msg, "argos:") {
		t.Fatalf("error %q, want the root-package validation message", msg)
	}
	for _, field := range []string{
		"MaxConcurrentCalls", "MaxFrameSize", "MaxMessageSize", "ReadAheadMessages", "MaxBufferedBytes",
	} {
		if !strings.Contains(msg, field) {
			t.Fatalf("error %q missing field %q", msg, field)
		}
	}
}

func TestSessionReusableAfterCall(t *testing.T) {
	t.Parallel()
	var dials atomic.Int64
	cli, err := newClientLoopback(t, sequentialLoopback(t, &dials),
		argos.WithServiceName(testService),
		argos.WithMaxConcurrentCalls(4),
		argos.WithMaxBufferedBytes(4*16*1024*1024),
		argos.WithHandshakeTimeout(50*time.Millisecond),
		argos.WithTarget(testTarget),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	m := testMethod(t)

	doCall := func() {
		cs, err := cli.Open(context.Background(), m)
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		if err := cs.Send([]byte("x")); err != nil {
			t.Fatalf("Send: %v", err)
		}
		if err := cs.HalfClose(); err != nil {
			t.Fatalf("HalfClose: %v", err)
		}
		var got []byte
		if err := cs.Recv(&got); err != nil {
			t.Fatalf("Recv: %v", err)
		}
		if err := cs.Recv(&got); !errors.Is(err, io.EOF) {
			t.Fatalf("Recv EOF: %v", err)
		}
		if err := cs.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}

	doCall()
	// Sleep past HandshakeTimeout: conn ctx must not carry that deadline.
	time.Sleep(80 * time.Millisecond)
	doCall()

	if got := dials.Load(); got != 1 {
		t.Fatalf("dial count = %d, want 1 (session reused; conn ctx not child of call)", got)
	}
}

func TestCallerCancelAbortsCall(t *testing.T) {
	t.Parallel()
	f := fake.NewFraming(session.Sequential)
	holdRecv := make(chan struct{})
	tr := &loopTransport{
		dial: func(ctx context.Context, endpoint string) (transport.Conn, error) {
			cli, srv := fake.BytePipe()
			go func() {
				sess, err := f.NewServerSession(context.Background(), srv, session.SessionSpec{})
				if err != nil {
					_ = srv.Close()
					return
				}
				defer sess.Close()
				md := metadata.New(metadata.RoleResponder, nil)
				sc, err := sess.AcceptCall(context.Background(), session.CallSpec{Metadata: md})
				if err != nil {
					return
				}
				defer sc.Close()
				// Block until test cancels; do not respond.
				<-holdRecv
				_ = sc.Finish(nil)
			}()
			return cli, nil
		},
	}
	cli, err := newClientLoopback(t, fixedLoopback(tr, f),
		argos.WithServiceName(testService),
		argos.WithMaxConcurrentCalls(4),
		argos.WithMaxBufferedBytes(4*16*1024*1024),
		argos.WithTarget(testTarget),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer close(holdRecv)

	ctx, cancel := context.WithCancel(context.Background())
	cs, err := cli.Open(ctx, testMethod(t))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer cs.Close()

	if err := cs.Send([]byte("ping")); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if err := cs.HalfClose(); err != nil {
		t.Fatalf("HalfClose: %v", err)
	}

	errCh := make(chan error, 1)
	go func() {
		var got []byte
		errCh <- cs.Recv(&got)
	}()

	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("Recv succeeded; want cancel error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Recv not aborted by caller cancel")
	}
}

func TestOpenFilterShortCircuitNeverDials(t *testing.T) {
	t.Parallel()
	var dials atomic.Int64
	want := errors.New("auth denied")
	cli, err := newClientLoopback(t, sequentialLoopback(t, &dials),
		argos.WithServiceName(testService),
		argos.WithMaxConcurrentCalls(4),
		argos.WithMaxBufferedBytes(4*16*1024*1024),
		argos.WithOpenFilter(func(ctx context.Context, m descriptor.Method, next filter.OpenFunc) (stream.Stream, error) {
			return nil, want
		}),
		argos.WithTarget(testTarget),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	_, err = cli.Open(context.Background(), testMethod(t))
	if !errors.Is(err, want) {
		t.Fatalf("Open: %v, want %v", err, want)
	}
	if got := dials.Load(); got != 0 {
		t.Fatalf("dials = %d, want 0", got)
	}
}

func TestDialFailureMapsUnavailable(t *testing.T) {
	t.Parallel()
	root := &net.OpError{Op: "dial", Err: errors.New("connection refused")}
	f := fake.NewFraming(session.Sequential)
	tr := &loopTransport{
		dial: func(ctx context.Context, endpoint string) (transport.Conn, error) {
			return nil, root
		},
	}
	cli, err := newClientLoopback(t, fixedLoopback(tr, f),
		argos.WithServiceName(testService),
		argos.WithMaxConcurrentCalls(2),
		argos.WithMaxBufferedBytes(2*16*1024*1024),
		argos.WithTarget(testTarget),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	_, err = cli.Open(context.Background(), testMethod(t))
	if status.CodeOf(err) != status.Unavailable {
		t.Fatalf("CodeOf = %v, want Unavailable; err=%v", status.CodeOf(err), err)
	}
	var op *net.OpError
	if !errors.As(err, &op) {
		t.Fatalf("errors.As net.OpError failed: %v", err)
	}
}

func TestHandshakeTimeoutMapsDeadlineExceeded(t *testing.T) {
	t.Parallel()
	f := fake.NewFraming(session.Sequential)
	f.Handshake = func(ctx context.Context, c transport.Conn) error {
		<-ctx.Done()
		return ctx.Err()
	}
	tr := &loopTransport{
		dial: func(ctx context.Context, endpoint string) (transport.Conn, error) {
			cli, srv := fake.BytePipe()
			t.Cleanup(func() { _ = srv.Close() })
			return cli, nil
		},
	}
	cli, err := newClientLoopback(t, fixedLoopback(tr, f),
		argos.WithServiceName(testService),
		argos.WithMaxConcurrentCalls(2),
		argos.WithMaxBufferedBytes(2*16*1024*1024),
		argos.WithHandshakeTimeout(30*time.Millisecond),
		argos.WithTarget(testTarget),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	_, err = cli.Open(context.Background(), testMethod(t))
	if status.CodeOf(err) != status.DeadlineExceeded {
		t.Fatalf("CodeOf = %v, want DeadlineExceeded; err=%v", status.CodeOf(err), err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("errors.Is DeadlineExceeded failed: %v", err)
	}
}

// TestHandshakeTimeoutBeforeOpenFilterNext: slow NewClientSession handshake
// fails with DeadlineExceeded and must not count as a successful next() open
// (filter post-next / stream-wrap path never runs).
func TestHandshakeTimeoutBeforeOpenFilterNext(t *testing.T) {
	t.Parallel()
	var nextOK atomic.Int64
	f := fake.NewFraming(session.Sequential)
	f.Handshake = func(ctx context.Context, c transport.Conn) error {
		<-ctx.Done()
		return ctx.Err()
	}
	tr := &loopTransport{
		dial: func(ctx context.Context, endpoint string) (transport.Conn, error) {
			cli, srv := fake.BytePipe()
			t.Cleanup(func() { _ = srv.Close() })
			return cli, nil
		},
	}
	cli, err := newClientLoopback(t, fixedLoopback(tr, f),
		argos.WithServiceName(testService),
		argos.WithMaxConcurrentCalls(2),
		argos.WithMaxBufferedBytes(2*16*1024*1024),
		argos.WithHandshakeTimeout(30*time.Millisecond),
		argos.WithOpenFilter(func(ctx context.Context, m descriptor.Method, next filter.OpenFunc) (stream.Stream, error) {
			st, err := next(ctx, m)
			if err != nil {
				return nil, err
			}
			// Count only when next yields a stream (call actually opened).
			nextOK.Add(1)
			return st, nil
		}),
		argos.WithTarget(testTarget),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	_, err = cli.Open(context.Background(), testMethod(t))
	code := status.CodeOf(err)
	if code != status.DeadlineExceeded && code != status.Unavailable {
		t.Fatalf("CodeOf = %v, want DeadlineExceeded or Unavailable; err=%v", code, err)
	}
	if got := nextOK.Load(); got != 0 {
		t.Fatalf("OpenFilter next success count = %d, want 0 (handshake timed out)", got)
	}
}

func TestNarrowInterfaceAssertStaysSetupError(t *testing.T) {
	t.Parallel()
	// Concurrent framing requires StreamConn; ByteConn is CarrierConn only.
	f := fake.NewFraming(session.Concurrent)
	tr := &loopTransport{
		dial: func(ctx context.Context, endpoint string) (transport.Conn, error) {
			cli, srv := fake.BytePipe()
			t.Cleanup(func() { _ = srv.Close() })
			return cli, nil
		},
	}
	cli, err := newClientLoopback(t, fixedLoopback(tr, f),
		argos.WithServiceName(testService),
		argos.WithMaxConcurrentCalls(2),
		argos.WithMaxBufferedBytes(2*16*1024*1024),
		argos.WithTarget(testTarget),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	_, err = cli.Open(context.Background(), testMethod(t))
	if err == nil {
		t.Fatal("expected narrow-interface setup error")
	}
	if status.CodeOf(err) != status.Unknown {
		t.Fatalf("CodeOf = %v, want Unknown (unmapped setup error); err=%v", status.CodeOf(err), err)
	}
	if !strings.Contains(err.Error(), "requires") {
		t.Fatalf("error = %v, want narrow-interface message", err)
	}
}

// A Client has no Close, so the caller's ctx is the only thing that can refuse
// an Open. A call opened with an already-done ctx could never be governed by
// that ctx, so Open refuses it up front instead of handing back a stream whose
// lifetime has already ended.
func TestOpenRefusesDoneContext(t *testing.T) {
	t.Parallel()
	var dials atomic.Int64
	cli, err := newClientLoopback(t, sequentialLoopback(t, &dials),
		argos.WithServiceName(testService),
		argos.WithMaxConcurrentCalls(1),
		argos.WithMaxBufferedBytes(16*1024*1024), // 1 × default perCall
		argos.WithTarget(testTarget),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := cli.Open(ctx, testMethod(t)); !errors.Is(err, context.Canceled) {
		t.Fatalf("Open with a done ctx = %v, want context.Canceled", err)
	}
	if got := dials.Load(); got != 0 {
		t.Fatalf("dials = %d, want 0: a refused Open must not reach the axis", got)
	}

	// The refusal must not have consumed admission either: this Client has one
	// slot, so a leaked reservation shows up here.
	cs, err := cli.Open(context.Background(), testMethod(t))
	if err != nil {
		t.Fatalf("Open after a refused Open: %v; the refusal held an admission slot", err)
	}
	_ = cs.Close()
}

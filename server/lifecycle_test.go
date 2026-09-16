package server_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/argos-io/argos"
	"github.com/argos-io/argos/codec"
	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/filter"
	"github.com/argos-io/argos/framing"
	"github.com/argos-io/argos/internal/fake"
	"github.com/argos-io/argos/server"
	"github.com/argos-io/argos/stream"
	"github.com/argos-io/argos/transport"
)

func TestProtocolAssembleOncePerServerStart(t *testing.T) {
	var calls atomic.Int64
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

	srv := server.New(argos.WithService(svcName,
		argos.JoinService(
			argos.ServiceTransport(func() (transport.Transport, error) {
				calls.Add(1)
				return tr, nil
			}),
			argos.ServiceFraming(func() (framing.Framing, error) { return fr, nil }),
			argos.ServiceCodec(func() (codec.Codec, error) { return rawCodec{}, nil }),
		),
		argos.ServiceListenAddress("127.0.0.1:0"),
	))
	if err := srv.Register(echoService(), map[string]filter.Handler{methodEcho: h}); err != nil {
		t.Fatal(err)
	}

	go func() { _ = srv.Run(context.Background()) }()
	time.Sleep(15 * time.Millisecond)
	t.Cleanup(func() { _ = srv.Close() })

	if got := calls.Load(); got != 1 {
		t.Fatalf("protocol assemble calls after Run = %d, want 1", got)
	}

	for i := 0; i < 3; i++ {
		client, serverConn := fake.BytePipe()
		tr.Offer(serverConn)
		method := descriptor.MustMethod(fullMethod, descriptor.Unary)
		call := openClientCall(t, client, fr, method)
		_ = unaryRoundTrip(t, call, "x")
		_ = call.Close()
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("protocol assemble calls after accepts = %d, want 1", got)
	}
}

func TestShutdownDeadlineInterruptsInFlight(t *testing.T) {
	tr := newTestTransport()
	fr := fake.NewFraming(framing.Sequential)
	entered := make(chan struct{})
	handlerDone := make(chan error, 1)
	h := func(ctx context.Context, m descriptor.Method, st stream.Stream) error {
		var req []byte
		if err := st.Recv(&req); err != nil {
			return err
		}
		drainRecv(st)
		close(entered)
		select {
		case <-ctx.Done():
			err := ctx.Err()
			handlerDone <- err
			return err
		case <-time.After(10 * time.Second):
			handlerDone <- errors.New("handler timed out without interrupt")
			return errors.New("not interrupted")
		}
	}
	srv := startServer(t, tr, fr, h)

	client, serverConn := fake.BytePipe()
	tr.Offer(serverConn)

	method := descriptor.MustMethod(fullMethod, descriptor.Unary)
	call := openClientCall(t, client, fr, method)
	if err := call.Send([]byte("hold")); err != nil {
		t.Fatal(err)
	}
	_ = call.HalfClose()

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not enter")
	}

	shCtx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	err := srv.Shutdown(shCtx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown: %v, want context.DeadlineExceeded", err)
	}

	select {
	case herr := <-handlerDone:
		if herr == nil {
			t.Fatal("handler returned nil; want interrupt error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("in-flight call was not interrupted by Shutdown deadline")
	}
	_ = call.Close()
}

func TestCloseIdempotent(t *testing.T) {
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
	srv := startServer(t, tr, fr, h)
	if err := srv.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := srv.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	if err := srv.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown after Close: %v", err)
	}
}

func TestFactoryIsolationAcrossListeners(t *testing.T) {
	var calls atomic.Int64
	tr1, tr2 := newTestTransport(), newTestTransport()
	fr1, fr2 := fake.NewFraming(framing.Sequential), fake.NewFraming(framing.Sequential)

	listenerAxes := func(tr transport.Transport, fr framing.Framing) argos.ServiceOption {
		return argos.JoinService(
			argos.ServiceTransport(func() (transport.Transport, error) {
				calls.Add(1)
				return tr, nil
			}),
			argos.ServiceFraming(func() (framing.Framing, error) { return fr, nil }),
			argos.ServiceCodec(func() (codec.Codec, error) { return rawCodec{}, nil }),
		)
	}
	srv := server.New(argos.WithService(svcName,
		argos.ServiceListener("127.0.0.1:1", listenerAxes(tr1, fr1)),
		argos.ServiceListener("127.0.0.1:2", listenerAxes(tr2, fr2)),
	))
	h := func(ctx context.Context, m descriptor.Method, st stream.Stream) error {
		return nil
	}
	if err := srv.Register(echoService(), map[string]filter.Handler{methodEcho: h}); err != nil {
		t.Fatal(err)
	}

	go func() { _ = srv.Run(context.Background()) }()
	time.Sleep(15 * time.Millisecond)
	t.Cleanup(func() { _ = srv.Close() })

	if got := calls.Load(); got != 2 {
		t.Fatalf("protocol assemble calls = %d, want 2 (once per listener)", got)
	}
}

var errAxisFactory = errors.New("transport factory refused")

// newAbortingServer returns a Server whose second listen surface cannot
// assemble, so Run aborts after the first surface is already built, plus that
// first surface's Transport.
func newAbortingServer(t *testing.T) (*server.Server, *testTransport) {
	t.Helper()
	tr := newTestTransport()
	fr := fake.NewFraming(framing.Sequential)
	srv := server.New(argos.WithService(svcName,
		argos.ServiceListener("127.0.0.1:1", testServiceAxes(tr, fr)),
		argos.ServiceListener("127.0.0.1:2",
			argos.ServiceTransport(func() (transport.Transport, error) { return nil, errAxisFactory }),
			argos.ServiceFraming(func() (framing.Framing, error) { return fr, nil }),
			argos.ServiceCodec(func() (codec.Codec, error) { return rawCodec{}, nil }),
		),
	))
	t.Cleanup(func() { _ = srv.Close() })
	if err := srv.Register(echoService(), map[string]filter.Handler{
		methodEcho: func(context.Context, descriptor.Method, stream.Stream) error { return nil },
	}); err != nil {
		t.Fatal(err)
	}
	return srv, tr
}

func TestStartFailureClosesAssembledTransport(t *testing.T) {
	srv, tr := newAbortingServer(t)

	if err := srv.Run(context.Background()); !errors.Is(err, errAxisFactory) {
		t.Fatalf("Run: %v, want %v", err, errAxisFactory)
	}
	if !tr.isClosed() {
		t.Error("aborted Run left the Transport it had assembled open")
	}
}

func TestStartFailureLeavesServerRestartable(t *testing.T) {
	srv, _ := newAbortingServer(t)

	if err := srv.Run(context.Background()); !errors.Is(err, errAxisFactory) {
		t.Fatalf("first Run: %v, want %v", err, errAxisFactory)
	}
	// No listen surface ever went live, so the second Run must report why the
	// start failed rather than reject the call as a restart.
	if err := srv.Run(context.Background()); !errors.Is(err, errAxisFactory) {
		t.Fatalf("second Run: %v, want %v", err, errAxisFactory)
	}
	other := descriptor.MustService("test.v1.Other",
		descriptor.MustMethod("test.v1.Other."+methodEcho, descriptor.Unary))
	if err := srv.Register(other, map[string]filter.Handler{
		methodEcho: func(context.Context, descriptor.Method, stream.Stream) error { return nil },
	}); err != nil {
		t.Fatalf("Register after a failed start: %v", err)
	}
}

func TestCloseDuringStartClosesAssembledTransports(t *testing.T) {
	tr1, tr2 := newTestTransport(), newTestTransport()
	fr := fake.NewFraming(framing.Sequential)
	assembling := make(chan struct{})
	resume := make(chan struct{})

	srv := server.New(argos.WithService(svcName,
		argos.ServiceListener("127.0.0.1:1", testServiceAxes(tr1, fr)),
		// Holds the start inside the assembly loop until the test has closed
		// the Server, so Close snapshots the live surfaces while there are none.
		argos.ServiceListener("127.0.0.1:2",
			argos.ServiceTransport(func() (transport.Transport, error) {
				close(assembling)
				<-resume
				return tr2, nil
			}),
			argos.ServiceFraming(func() (framing.Framing, error) { return fr, nil }),
			argos.ServiceCodec(func() (codec.Codec, error) { return rawCodec{}, nil }),
		),
	))
	t.Cleanup(func() { _ = srv.Close() })
	if err := srv.Register(echoService(), map[string]filter.Handler{
		methodEcho: func(context.Context, descriptor.Method, stream.Stream) error { return nil },
	}); err != nil {
		t.Fatal(err)
	}

	runErr := make(chan error, 1)
	go func() { runErr <- srv.Run(context.Background()) }()

	<-assembling
	if err := srv.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	close(resume)

	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("Run: %v, want nil after Close", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after Close during start")
	}
	if !tr1.isClosed() || !tr2.isClosed() {
		t.Errorf("Close during start left Transports open: first=%v second=%v",
			tr1.isClosed(), tr2.isClosed())
	}
}

func TestRejectedOptionsSurfaceFromRun(t *testing.T) {
	tr := newTestTransport()
	fr := fake.NewFraming(framing.Sequential)
	srv := server.New(
		argos.WithMaxConcurrentCalls(-5),
		argos.WithService(svcName, testServiceAxes(tr, fr), argos.ServiceListenAddress("127.0.0.1:0")),
	)
	t.Cleanup(func() { _ = srv.Close() })

	if err := srv.Register(echoService(), map[string]filter.Handler{methodEcho: func(context.Context, descriptor.Method, stream.Stream) error {
		return nil
	}}); err != nil {
		t.Fatal(err)
	}
	runErr := srv.Run(context.Background())
	if runErr == nil {
		t.Fatal("Run succeeded on a Server built from a rejected option set")
	}
	if !strings.Contains(runErr.Error(), "MaxConcurrentCalls") {
		t.Fatalf("Run: %v, want the error to name MaxConcurrentCalls", runErr)
	}
}

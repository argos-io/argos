package server_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/argos-io/argos"
	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/filter"
	"github.com/argos-io/argos/framing"
	"github.com/argos-io/argos/internal/fake"
	"github.com/argos-io/argos/server"
	"github.com/argos-io/argos/stream"
	"github.com/argos-io/argos/transport"
)

func TestBindingFuncOncePerServerStart(t *testing.T) {
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

	cfg, err := argos.New()
	if err != nil {
		t.Fatal(err)
	}
	srv := server.New(cfg)
	err = srv.AddBinding(func() (argos.Binding, error) {
		calls.Add(1)
		return argos.Binding{Transport: tr, Framing: fr, Codec: rawCodec{}}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Register(echoService(), map[string]filter.Handler{methodEcho: h}); err != nil {
		t.Fatal(err)
	}

	go func() { _ = srv.Run(context.Background()) }()
	time.Sleep(15 * time.Millisecond)
	t.Cleanup(func() { _ = srv.Close() })

	if got := calls.Load(); got != 1 {
		t.Fatalf("BindingFunc calls after Run = %d, want 1", got)
	}

	// Multiple accepted conns must not re-invoke the factory.
	for i := 0; i < 3; i++ {
		client, serverConn := fake.BytePipe()
		tr.Offer(serverConn)
		method := descriptor.MustMethod(fullMethod, descriptor.Unary)
		call := openClientCall(t, client, fr, method)
		_ = unaryRoundTrip(t, call, "x")
		_ = call.Close()
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("BindingFunc calls after accepts = %d, want 1", got)
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
	// Shutdown after Close is a no-op.
	if err := srv.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown after Close: %v", err)
	}
}

func TestFactoryIsolationAcrossBindings(t *testing.T) {
	var calls atomic.Int64
	tr1, tr2 := newTestTransport(), newTestTransport()
	fr1, fr2 := fake.NewFraming(framing.Sequential), fake.NewFraming(framing.Sequential)

	cfg, err := argos.New()
	if err != nil {
		t.Fatal(err)
	}
	srv := server.New(cfg)

	mk := func(tr transport.Transport, fr framing.Framing) argos.BindingFunc {
		return func() (argos.Binding, error) {
			calls.Add(1)
			return argos.Binding{Transport: tr, Framing: fr, Codec: rawCodec{}}, nil
		}
	}
	if err := srv.AddBinding(mk(tr1, fr1)); err != nil {
		t.Fatal(err)
	}
	if err := srv.AddBinding(mk(tr2, fr2)); err != nil {
		t.Fatal(err)
	}
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
		t.Fatalf("BindingFunc calls = %d, want 2 (once per binding)", got)
	}
}

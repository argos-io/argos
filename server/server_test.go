package server

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/argos-io/argos"
	"github.com/argos-io/argos/codec/protobuf"
	"github.com/argos-io/argos/transport"
	"github.com/argos-io/argos/transport/http1"
)

type noopTransport struct{}

func (noopTransport) ListenAndServe(
	_ context.Context,
	_ func(context.Context, string, transport.Framer) error,
	_ ...transport.ServerOption,
) error {
	return nil
}

func (noopTransport) Open(_ context.Context, _ string, _ ...transport.ClientOption) (transport.Framer, error) {
	return nil, nil
}

func TestRunErrorsWithoutCodec(t *testing.T) {
	s := New()
	s.NewService(argos.WithTransport(noopTransport{}))
	if err := s.Run(context.Background()); err == nil {
		t.Fatal("expected error without codec")
	}
}

func TestRunEmptyServer(t *testing.T) {
	s := New()
	if err := s.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func TestRunRejectsNilContext(t *testing.T) {
	var ctx context.Context
	if err := New().Run(ctx); err == nil {
		t.Fatal("Run accepted nil context")
	}
}

func TestRunRejectsPreCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := New().Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run error = %v, want context.Canceled", err)
	}
}

func TestRunErrorsWithoutTransport(t *testing.T) {
	s := New()
	s.NewService(argos.WithCodec(nopCodec{}))
	if err := s.Run(context.Background()); err == nil {
		t.Fatal("expected error without transport")
	}
}

func TestRunRejectsIncompatibleTransportCodec(t *testing.T) {
	s := New()
	s.NewService(argos.WithTransport(http1.New()), argos.WithCodec(protobuf.New()))
	err := s.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), `transport "http1" is incompatible`) {
		t.Fatalf("error = %v, want http1/codec incompatibility", err)
	}
}

type blockingTransport struct {
	started chan struct{}
}

func (t *blockingTransport) ListenAndServe(ctx context.Context, _ func(context.Context, string, transport.Framer) error, _ ...transport.ServerOption) error {
	close(t.started)
	<-ctx.Done()
	return ctx.Err()
}

func (t *blockingTransport) Open(context.Context, string, ...transport.ClientOption) (transport.Framer, error) {
	return nil, nil
}

type errTransport struct {
	err error
}

func (e errTransport) ListenAndServe(context.Context, func(context.Context, string, transport.Framer) error, ...transport.ServerOption) error {
	return e.err
}

func (errTransport) Open(context.Context, string, ...transport.ClientOption) (transport.Framer, error) {
	return nil, nil
}

type nopCodec struct{}

func (nopCodec) Marshal(_ io.Writer, _ any) error   { return nil }
func (nopCodec) Unmarshal(_ io.Reader, _ any) error { return nil }

func TestRunMultiServiceAllStart(t *testing.T) {
	block1 := &blockingTransport{started: make(chan struct{})}
	block2 := &blockingTransport{started: make(chan struct{})}
	s := New()
	s.NewService(argos.WithTransport(block1), argos.WithCodec(nopCodec{}))
	s.NewService(argos.WithTransport(block2), argos.WithCodec(nopCodec{}))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()

	<-block1.started
	<-block2.started
	cancel()
	if err := <-done; err == nil {
		t.Fatal("expected ctx.Err() from cancelled Run")
	}
}

func TestRunFirstListenerErrorCancelsOthers(t *testing.T) {
	listenErr := errors.New("listen failed")
	block := &blockingTransport{started: make(chan struct{})}
	s := New()
	s.NewService(argos.WithTransport(errTransport{err: listenErr}), argos.WithCodec(nopCodec{}))
	s.NewService(argos.WithTransport(block), argos.WithCodec(nopCodec{}))

	ctx := context.Background()
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()

	<-block.started
	if err := <-done; !errors.Is(err, listenErr) {
		t.Fatalf("Run error = %v, want %v", err, listenErr)
	}
}

func TestRunFirstListenerErrorOmitsInternalCancellation(t *testing.T) {
	listenErr := errors.New("listen failed")
	block := &blockingTransport{started: make(chan struct{})}
	s := New()
	s.NewService(argos.WithTransport(errTransport{err: listenErr}), argos.WithCodec(nopCodec{}))
	s.NewService(argos.WithTransport(block), argos.WithCodec(nopCodec{}))

	done := make(chan error, 1)
	go func() { done <- s.Run(context.Background()) }()

	<-block.started
	err := <-done
	if !errors.Is(err, listenErr) {
		t.Fatalf("Run error = %v, want %v", err, listenErr)
	}
	if strings.Contains(err.Error(), context.Canceled.Error()) {
		t.Fatalf("Run error contains internal cancellation: %v", err)
	}
}

func TestRunNormalListenerStopCancelsOthers(t *testing.T) {
	block := &blockingTransport{started: make(chan struct{})}
	s := New()
	s.NewService(argos.WithTransport(noopTransport{}), argos.WithCodec(nopCodec{}))
	s.NewService(argos.WithTransport(block), argos.WithCodec(nopCodec{}))

	done := make(chan error, 1)
	go func() { done <- s.Run(context.Background()) }()

	select {
	case <-block.started:
	case <-time.After(time.Second):
		t.Fatal("blocking listener did not start")
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run did not stop after a listener returned normally")
	}
}

func TestRunResolvesAllServicesBeforeStartingListeners(t *testing.T) {
	first := &blockingTransport{started: make(chan struct{})}
	s := New()
	s.NewService(argos.WithTransport(first), argos.WithCodec(nopCodec{}))
	s.NewService(argos.WithTransportNamed("test-missing-late-transport"), argos.WithCodec(nopCodec{}))

	if err := s.Run(context.Background()); err == nil {
		t.Fatal("Run succeeded with an unresolved later service")
	}
	select {
	case <-first.started:
		t.Fatal("an earlier listener started before all services were resolved")
	default:
	}
}

func TestRunJoinsMultipleListenerErrors(t *testing.T) {
	err1 := errors.New("listen failed one")
	err2 := errors.New("listen failed two")
	s := New()
	s.NewService(argos.WithTransport(errTransport{err: err1}), argos.WithCodec(nopCodec{}))
	s.NewService(argos.WithTransport(errTransport{err: err2}), argos.WithCodec(nopCodec{}))

	err := s.Run(context.Background())
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, err1) || !errors.Is(err, err2) {
		t.Fatalf("Run error = %v, want both %v and %v", err, err1, err2)
	}
}

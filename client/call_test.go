package client

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/argos-io/argos"
	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/filter"
	"github.com/argos-io/argos/stream"
)

type recvFailStream struct {
	stream.Stream
	recvErr error
}

func (s *recvFailStream) Recv(v any) error {
	return s.recvErr
}

func TestHeaderWaitsAfterNonStatusRecvError(t *testing.T) {
	t.Parallel()
	recvErr := errors.New("client_test: wire broken")

	wrapRecv := filter.OpenFilter(func(ctx context.Context, m descriptor.Method, next filter.OpenFunc) (stream.Stream, error) {
		st, err := next(ctx, m)
		if err != nil {
			return nil, err
		}
		return &recvFailStream{Stream: st, recvErr: recvErr}, nil
	})
	cli, err := New(
		argos.WithServiceName(testService),
		argos.WithTarget(testTarget),
		sequentialLoopback(t, nil),
		argos.WithOpenFilter(wrapRecv),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer cli.Close()

	cs, err := cli.Open(context.Background(), testMethod(t))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	headerDone := make(chan error, 1)
	go func() {
		_, err := cs.Header()
		headerDone <- err
	}()

	time.Sleep(30 * time.Millisecond)
	select {
	case err := <-headerDone:
		t.Fatalf("Header returned early: %v", err)
	default:
	}

	if err := cs.Recv(new([]byte)); !errors.Is(err, recvErr) {
		t.Fatalf("Recv = %v, want %v", err, recvErr)
	}

	time.Sleep(30 * time.Millisecond)
	select {
	case err := <-headerDone:
		t.Fatalf("Header returned after non-status Recv: %v", err)
	default:
	}

	if err := cs.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case err := <-headerDone:
		if !errors.Is(err, ErrCallClosed) {
			t.Fatalf("Header after Close = %v, want ErrCallClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Header did not return after Close")
	}
}

func TestHeaderAfterCloseReturnsErrCallClosed(t *testing.T) {
	t.Parallel()
	cli, err := New(
		argos.WithServiceName(testService),
		argos.WithTarget(testTarget),
		sequentialLoopback(t, nil),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	defer cli.Close()

	cs, err := cli.Open(context.Background(), testMethod(t))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := cs.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if _, err := cs.Header(); !errors.Is(err, ErrCallClosed) {
		t.Fatalf("Header after Close = %v, want ErrCallClosed", err)
	}
	if cs.Trailer() != nil {
		t.Fatalf("Trailer after Close = %v, want nil", cs.Trailer())
	}
}

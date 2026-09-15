package probe

import (
	"context"
	"errors"
	"io"
	"testing"
)

type pMethod struct{ full string }

type pStream interface {
	Recv(v any) error
	Send(v any) error
}

type openFunc func(ctx context.Context, m pMethod) (pStream, error)
type openFilter func(ctx context.Context, m pMethod, next openFunc) (pStream, error)

func chain(term openFunc, fs ...openFilter) openFunc {
	for i := len(fs) - 1; i >= 0; i-- {
		f, next := fs[i], term
		term = func(ctx context.Context, m pMethod) (pStream, error) {
			var nextCalled bool
			wrappedNext := func(ctx context.Context, m pMethod) (pStream, error) {
				nextCalled = true
				return next(ctx, m)
			}
			st, err := f(ctx, m, wrappedNext)
			if !nextCalled && st == nil && err == nil {
				return nil, errOpenFilterMisuse
			}
			return st, err
		}
	}
	return term
}

var errOpenFilterMisuse = errors.New("openfilter: must call next or return non-nil error")

type stubStream struct {
	send func(v any) error
	recv func(v any) error
}

func (s *stubStream) Send(v any) error { return s.send(v) }
func (s *stubStream) Recv(v any) error { return s.recv(v) }

type wrapStream struct {
	inner      pStream
	name       string
	sendOrder  *[]string
	recvOrder  *[]string
	recvMutate func(error) error
}

func (w *wrapStream) Send(v any) error {
	*w.sendOrder = append(*w.sendOrder, w.name+" enter send")
	err := w.inner.Send(v)
	*w.sendOrder = append(*w.sendOrder, w.name+" exit send")
	return err
}

func (w *wrapStream) Recv(v any) error {
	*w.recvOrder = append(*w.recvOrder, w.name+" enter recv")
	err := w.inner.Recv(v)
	if w.recvMutate != nil {
		err = w.recvMutate(err)
	}
	*w.recvOrder = append(*w.recvOrder, w.name+" exit recv")
	return err
}

func wrapFilter(name string, sendOrder, recvOrder *[]string, recvMutate func(error) error) openFilter {
	return func(ctx context.Context, m pMethod, next openFunc) (pStream, error) {
		st, err := next(ctx, m)
		if err != nil {
			return nil, err
		}
		return &wrapStream{
			inner:      st,
			name:       name,
			sendOrder:  sendOrder,
			recvOrder:  recvOrder,
			recvMutate: recvMutate,
		}, nil
	}
}

func TestOpenFilterShortCircuit(t *testing.T) {
	var dialCount int
	term := func(ctx context.Context, m pMethod) (pStream, error) {
		dialCount++
		return &stubStream{}, nil
	}

	want := errors.New("auth denied")
	short := func(ctx context.Context, m pMethod, next openFunc) (pStream, error) {
		return nil, want
	}

	_, err := chain(term, short)(context.Background(), pMethod{"svc/m"})
	if !errors.Is(err, want) {
		t.Fatalf("error = %v, want %v", err, want)
	}
	if dialCount != 0 {
		t.Fatalf("dialCount = %d, want 0 (short-circuit must not reach terminal)", dialCount)
	}
}

func TestOpenFilterWrapOrder(t *testing.T) {
	var sendOrder, recvOrder []string

	term := func(ctx context.Context, m pMethod) (pStream, error) {
		return &stubStream{
			send: func(v any) error { return nil },
			recv: func(v any) error { return io.EOF },
		}, nil
	}

	open := chain(term,
		wrapFilter("outer", &sendOrder, &recvOrder, nil),
		wrapFilter("inner", &sendOrder, &recvOrder, nil),
	)

	st, err := open(context.Background(), pMethod{"svc/m"})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := st.Send(nil); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if err := st.Recv(nil); !errors.Is(err, io.EOF) {
		t.Fatalf("Recv: %v, want EOF", err)
	}

	wantSend := []string{
		"outer enter send", "inner enter send",
		"inner exit send", "outer exit send",
	}
	if len(sendOrder) != len(wantSend) {
		t.Fatalf("Send order = %v, want %v", sendOrder, wantSend)
	}
	for i, got := range sendOrder {
		if got != wantSend[i] {
			t.Fatalf("Send order[%d] = %q, want %q (full: %v)", i, got, wantSend[i], sendOrder)
		}
	}

	wantRecv := []string{
		"outer enter recv", "inner enter recv",
		"inner exit recv", "outer exit recv",
	}
	if len(recvOrder) != len(wantRecv) {
		t.Fatalf("Recv order = %v, want %v", recvOrder, wantRecv)
	}
	for i, got := range recvOrder {
		if got != wantRecv[i] {
			t.Fatalf("Recv order[%d] = %q, want %q (full: %v)", i, got, wantRecv[i], recvOrder)
		}
	}
}

func TestOpenFilterAppendFailure(t *testing.T) {
	remoteErr := errors.New("validation failed")

	term := func(ctx context.Context, m pMethod) (pStream, error) {
		return &stubStream{
			send: func(v any) error { return nil },
			recv: func(v any) error { return io.EOF },
		}, nil
	}

	inner := wrapFilter("inner", new([]string), new([]string), func(err error) error {
		if errors.Is(err, io.EOF) {
			return remoteErr
		}
		return err
	})
	outer := wrapFilter("outer", new([]string), new([]string), nil)

	st, err := chain(term, outer, inner)(context.Background(), pMethod{"svc/m"})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := st.Recv(nil); !errors.Is(err, remoteErr) {
		t.Fatalf("Recv = %v, want %v", err, remoteErr)
	}
}

func TestOpenFilterMustNotClearFailure(t *testing.T) {
	remoteErr := errors.New("remote unavailable")

	term := func(ctx context.Context, m pMethod) (pStream, error) {
		return &stubStream{
			send: func(v any) error { return nil },
			recv: func(v any) error { return remoteErr },
		}, nil
	}

	// Inner returns (st, nil) after next; outer returns (st, nil). On Recv the inner
	// layer must pass remoteErr through unchanged — outer must not clear it.
	inner := wrapFilter("inner", new([]string), new([]string), func(err error) error {
		return err
	})
	outer := wrapFilter("outer", new([]string), new([]string), func(err error) error {
		return err
	})

	st, err := chain(term, outer, inner)(context.Background(), pMethod{"svc/m"})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := st.Recv(nil); !errors.Is(err, remoteErr) {
		t.Fatalf("Recv = %v, want %v", err, remoteErr)
	}
}

func TestOpenFilterNilNilWithoutNext(t *testing.T) {
	bad := func(ctx context.Context, m pMethod, next openFunc) (pStream, error) {
		return nil, nil
	}

	_, err := chain(func(ctx context.Context, m pMethod) (pStream, error) {
		return &stubStream{}, nil
	}, bad)(context.Background(), pMethod{"svc/m"})

	if !errors.Is(err, errOpenFilterMisuse) {
		t.Fatalf("error = %v, want %v", err, errOpenFilterMisuse)
	}
}

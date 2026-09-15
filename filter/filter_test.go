package filter

import (
	"context"
	"errors"
	"io"
	"testing"

	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/stream"
)

var testMethod = descriptor.MustMethod("svc.m", descriptor.Unary)

type stubStream struct {
	send func(v any) error
	recv func(v any) error
}

func (s *stubStream) Send(v any) error {
	if s.send != nil {
		return s.send(v)
	}
	return nil
}

func (s *stubStream) Recv(v any) error {
	if s.recv != nil {
		return s.recv(v)
	}
	return io.EOF
}

func (s *stubStream) HalfClose() error { return nil }

type wrapStream struct {
	inner      stream.Stream
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

func (w *wrapStream) HalfClose() error { return w.inner.HalfClose() }

func wrapFilter(name string, sendOrder, recvOrder *[]string, recvMutate func(error) error) OpenFilter {
	return func(ctx context.Context, m descriptor.Method, next OpenFunc) (stream.Stream, error) {
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

// sessionPoolStub is an in-memory stand-in for the client session pool.
// Terminus borrows a session (idle → in-use) before OpenCall; short-circuit
// must leave idle/inUse unchanged.
type sessionPoolStub struct {
	idle        int
	inUse       int
	borrowCount int
}

func (p *sessionPoolStub) borrow() {
	p.borrowCount++
	if p.idle > 0 {
		p.idle--
	}
	p.inUse++
}

func TestChainShortCircuit(t *testing.T) {
	var endCount int
	end := func(ctx context.Context, m descriptor.Method, st stream.Stream) error {
		endCount++
		return nil
	}
	want := errors.New("denied")
	short := func(ctx context.Context, m descriptor.Method, st stream.Stream, next Handler) error {
		return want
	}

	err := Chain([]Filter{short}, end)(context.Background(), testMethod, &stubStream{})
	if !errors.Is(err, want) {
		t.Fatalf("error = %v, want %v", err, want)
	}
	if endCount != 0 {
		t.Fatalf("endCount = %d, want 0 (short-circuit must not reach terminal)", endCount)
	}
}

func TestOpenFilterShortCircuit(t *testing.T) {
	var dialCount int
	term := func(ctx context.Context, m descriptor.Method) (stream.Stream, error) {
		dialCount++
		return &stubStream{}, nil
	}

	want := errors.New("auth denied")
	short := func(ctx context.Context, m descriptor.Method, next OpenFunc) (stream.Stream, error) {
		return nil, want
	}

	_, err := ChainOpen([]OpenFilter{short}, term)(context.Background(), testMethod)
	if !errors.Is(err, want) {
		t.Fatalf("error = %v, want %v", err, want)
	}
	if dialCount != 0 {
		t.Fatalf("dialCount = %d, want 0 (short-circuit must not reach terminal)", dialCount)
	}
}

// TestOpenFilterShortCircuitDoesNotBorrowSession asserts Task 0.9 re-run:
// chain terminus is borrow session → OpenCall; short-circuit must not produce
// network resources AND must not occupy an idle session from the pool.
func TestOpenFilterShortCircuitDoesNotBorrowSession(t *testing.T) {
	pool := &sessionPoolStub{idle: 2}
	idleBefore, inUseBefore := pool.idle, pool.inUse

	var openCallCount int
	term := func(ctx context.Context, m descriptor.Method) (stream.Stream, error) {
		pool.borrow()
		openCallCount++
		return &stubStream{}, nil
	}

	want := errors.New("auth denied")
	short := func(ctx context.Context, m descriptor.Method, next OpenFunc) (stream.Stream, error) {
		return nil, want
	}

	_, err := ChainOpen([]OpenFilter{short}, term)(context.Background(), testMethod)
	if !errors.Is(err, want) {
		t.Fatalf("error = %v, want %v", err, want)
	}
	if openCallCount != 0 {
		t.Fatalf("openCallCount = %d, want 0 (short-circuit must not reach terminus)", openCallCount)
	}
	if pool.borrowCount != 0 {
		t.Fatalf("borrowCount = %d, want 0 (short-circuit must not borrow a session)", pool.borrowCount)
	}
	if pool.idle != idleBefore || pool.inUse != inUseBefore {
		t.Fatalf("pool idle/inUse = %d/%d, want %d/%d (idle sessions must stay unoccupied)",
			pool.idle, pool.inUse, idleBefore, inUseBefore)
	}
}

func TestOpenFilterWrapOrder(t *testing.T) {
	var sendOrder, recvOrder []string

	term := func(ctx context.Context, m descriptor.Method) (stream.Stream, error) {
		return &stubStream{
			send: func(v any) error { return nil },
			recv: func(v any) error { return io.EOF },
		}, nil
	}

	open := ChainOpen([]OpenFilter{
		wrapFilter("outer", &sendOrder, &recvOrder, nil),
		wrapFilter("inner", &sendOrder, &recvOrder, nil),
	}, term)

	st, err := open(context.Background(), testMethod)
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

	term := func(ctx context.Context, m descriptor.Method) (stream.Stream, error) {
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

	st, err := ChainOpen([]OpenFilter{outer, inner}, term)(context.Background(), testMethod)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := st.Recv(nil); !errors.Is(err, remoteErr) {
		t.Fatalf("Recv = %v, want %v", err, remoteErr)
	}
}

func TestOpenFilterMustNotClearFailure(t *testing.T) {
	remoteErr := errors.New("remote unavailable")

	term := func(ctx context.Context, m descriptor.Method) (stream.Stream, error) {
		return &stubStream{
			send: func(v any) error { return nil },
			recv: func(v any) error { return remoteErr },
		}, nil
	}

	// OpenFilters return (st, nil) after next. On Recv the wrap layers must
	// pass remoteErr through unchanged — outer must not clear it.
	inner := wrapFilter("inner", new([]string), new([]string), func(err error) error {
		return err
	})
	outer := wrapFilter("outer", new([]string), new([]string), func(err error) error {
		return err
	})

	st, err := ChainOpen([]OpenFilter{outer, inner}, term)(context.Background(), testMethod)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := st.Recv(nil); !errors.Is(err, remoteErr) {
		t.Fatalf("Recv = %v, want %v", err, remoteErr)
	}
}

func TestOpenFilterNilNilWithoutNext(t *testing.T) {
	bad := func(ctx context.Context, m descriptor.Method, next OpenFunc) (stream.Stream, error) {
		return nil, nil
	}

	_, err := ChainOpen([]OpenFilter{bad}, func(ctx context.Context, m descriptor.Method) (stream.Stream, error) {
		return &stubStream{}, nil
	})(context.Background(), testMethod)

	if !errors.Is(err, ErrOpenFilterMisuse) {
		t.Fatalf("error = %v, want %v", err, ErrOpenFilterMisuse)
	}
}

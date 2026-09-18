package budget

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/argos-io/argos/status"
)

func asSlice(t *testing.T, b Budget) SliceBudget {
	t.Helper()
	sb, ok := b.(SliceBudget)
	if !ok {
		t.Fatalf("New must return SliceBudget, got %T", b)
	}
	return sb
}

func TestTryAcquireRelease(t *testing.T) {
	b := New(100)
	r1, err := b.TryAcquire(40)
	if err != nil {
		t.Fatalf("TryAcquire(40): %v", err)
	}
	r2, err := b.TryAcquire(60)
	if err != nil {
		t.Fatalf("TryAcquire(60): %v", err)
	}
	_, err = b.TryAcquire(1)
	if !errors.Is(err, status.ErrCallsExhausted) {
		t.Fatalf("expected ErrCallsExhausted, got %v", err)
	}
	if status.CodeOf(err) != status.ResourceExhausted {
		t.Fatalf("CodeOf = %v, want ResourceExhausted", status.CodeOf(err))
	}
	r1()
	r1() // idempotent
	r3, err := b.TryAcquire(40)
	if err != nil {
		t.Fatalf("after release TryAcquire(40): %v", err)
	}
	r2()
	r3()
	_, err = b.TryAcquire(100)
	if err != nil {
		t.Fatalf("full capacity after all releases: %v", err)
	}
}

func TestTryAcquireNonPositive(t *testing.T) {
	b := New(10)
	for _, n := range []int64{0, -1, -100} {
		r, err := b.TryAcquire(n)
		if err != nil {
			t.Fatalf("TryAcquire(%d): %v", n, err)
		}
		r()
		r()
	}
	// capacity untouched
	if _, err := b.TryAcquire(10); err != nil {
		t.Fatalf("capacity should be intact: %v", err)
	}
}

func TestTryAcquireExhaust(t *testing.T) {
	b := New(8)
	r, err := b.TryAcquire(8)
	if err != nil {
		t.Fatal(err)
	}
	_, err = b.TryAcquire(1)
	if !errors.Is(err, status.ErrCallsExhausted) {
		t.Fatalf("got %v, want ErrCallsExhausted", err)
	}
	if status.CodeOf(err) != status.ResourceExhausted {
		t.Fatalf("CodeOf = %v", status.CodeOf(err))
	}
	r()
}

func TestSliceAliasNoDoubleCharge(t *testing.T) {
	sb := asSlice(t, New(1024))
	a := make([]byte, 0, 1024)
	r1, err := sb.TryAcquireSlice(a)
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	alias := a[:0]
	r2, err := sb.TryAcquireSlice(alias)
	if err != nil {
		t.Fatalf("alias must not double-charge: %v", err)
	}
	// capacity fully used by the single charge
	if _, err := sb.TryAcquire(1); !errors.Is(err, status.ErrCallsExhausted) {
		t.Fatalf("expected exhausted after one charge, got %v", err)
	}
	r1()
	// still held via alias
	if _, err := sb.TryAcquire(1); !errors.Is(err, status.ErrCallsExhausted) {
		t.Fatalf("expected still held after one release, got %v", err)
	}
	r2()
	r2() // idempotent
	if _, err := sb.TryAcquire(1024); err != nil {
		t.Fatalf("after both releases: %v", err)
	}
}

func TestTryAcquireSliceNonPositive(t *testing.T) {
	sb := asSlice(t, New(10))
	r, err := sb.TryAcquireSlice(nil)
	if err != nil {
		t.Fatal(err)
	}
	r()
	r, err = sb.TryAcquireSlice([]byte{})
	if err != nil {
		t.Fatal(err)
	}
	r()
	if _, err := sb.TryAcquire(10); err != nil {
		t.Fatalf("capacity intact: %v", err)
	}
}

func TestFromContext(t *testing.T) {
	ctx := context.Background()
	if _, ok := FromContext(ctx); ok {
		t.Fatal("empty ctx should miss")
	}
	b := New(1)
	ctx = ContextWith(ctx, b)
	got, ok := FromContext(ctx)
	if !ok || got != b {
		t.Fatalf("FromContext = (%v, %v), want (%v, true)", got, ok, b)
	}
}

func TestConcurrentAcquireRelease(t *testing.T) {
	const (
		capacity = 64
		workers  = 32
		iters    = 200
	)
	b := New(capacity)
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < iters; j++ {
				r, err := b.TryAcquire(1)
				if err != nil {
					if !errors.Is(err, status.ErrCallsExhausted) {
						t.Errorf("unexpected err: %v", err)
					}
					continue
				}
				r()
				r() // idempotent under race
			}
		}()
	}
	wg.Wait()
	// all returned
	r, err := b.TryAcquire(capacity)
	if err != nil {
		t.Fatalf("capacity not fully returned: %v", err)
	}
	r()
}

func TestConcurrentSliceAlias(t *testing.T) {
	sb := asSlice(t, New(1024))
	a := make([]byte, 0, 1024)
	var wg sync.WaitGroup
	const workers = 16
	wg.Add(workers)
	releases := make([]func(), workers)
	errs := make([]error, workers)
	for i := 0; i < workers; i++ {
		i := i
		go func() {
			defer wg.Done()
			p := a[:0]
			releases[i], errs[i] = sb.TryAcquireSlice(p)
		}()
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("worker %d: %v", i, err)
		}
	}
	if _, err := sb.TryAcquire(1); !errors.Is(err, status.ErrCallsExhausted) {
		t.Fatalf("expected single charge, got %v", err)
	}
	for _, r := range releases {
		r()
	}
	if _, err := sb.TryAcquire(1024); err != nil {
		t.Fatalf("after releases: %v", err)
	}
}

// Package budget provides a per-call byte budget.
//
// A Budget is carved out once at call admission (perCall). Within a call,
// TryAcquire never waits: exceeding the remaining capacity returns
// status.ErrCallsExhausted. Successful acquires return an idempotent release.
//
// Today only framing/envelope reads the Budget from the call context; other
// Framing stacks may adopt it later. The client still reserves perCall bytes
// at admission for every stack.
//
// Byte slices are charged by underlying array capacity. Aliases that share
// the same (*byte, cap) key do not double-charge; see SliceBudget.
package budget

import (
	"context"
	"sync"
	"unsafe"

	"github.com/argos-io/argos/status"
)

// Budget is a per-call buffer quota. TryAcquire does not wait.
type Budget interface {
	// TryAcquire reserves bytes from the remaining capacity.
	// On success it returns an idempotent release that returns the bytes.
	// n <= 0 is a no-op success with an idempotent noop release.
	TryAcquire(bytes int64) (release func(), err error)
}

// SliceBudget extends Budget with capacity charging keyed by underlying array.
// TryAcquireSlice charges int64(cap(p)) once per (*byte, cap) identity;
// aliases of the same array do not double-bill while any acquire is held.
type SliceBudget interface {
	Budget
	TryAcquireSlice(p []byte) (release func(), err error)
}

type contextKey struct{}

// FromContext returns the Budget stored in ctx, if any.
func FromContext(ctx context.Context) (Budget, bool) {
	b, ok := ctx.Value(contextKey{}).(Budget)
	return b, ok
}

// ContextWith returns a child of ctx carrying b.
func ContextWith(ctx context.Context, b Budget) context.Context {
	return context.WithValue(ctx, contextKey{}, b)
}

// New creates a Budget with the given capacity (perCall bytes).
// The concrete type also implements SliceBudget.
func New(capacity int64) Budget {
	if capacity < 0 {
		capacity = 0
	}
	return &budget{
		remaining: capacity,
		slices:    make(map[sliceKey]*sliceHold),
	}
}

type sliceKey struct {
	ptr *byte
	cap int
}

type sliceHold struct {
	bytes int64
	refs  int
}

type budget struct {
	mu        sync.Mutex
	remaining int64
	slices    map[sliceKey]*sliceHold
}

func noop() {}

func (b *budget) TryAcquire(n int64) (func(), error) {
	if n <= 0 {
		return noop, nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if n > b.remaining {
		return nil, status.ErrCallsExhausted
	}
	b.remaining -= n
	var once sync.Once
	return func() {
		once.Do(func() {
			b.mu.Lock()
			b.remaining += n
			b.mu.Unlock()
		})
	}, nil
}

func (b *budget) TryAcquireSlice(p []byte) (func(), error) {
	n := int64(cap(p))
	if n <= 0 {
		return noop, nil
	}
	key := sliceKey{ptr: unsafe.SliceData(p), cap: cap(p)}

	b.mu.Lock()
	defer b.mu.Unlock()

	if h, ok := b.slices[key]; ok {
		h.refs++
		return b.sliceRelease(key), nil
	}
	if n > b.remaining {
		return nil, status.ErrCallsExhausted
	}
	b.remaining -= n
	b.slices[key] = &sliceHold{bytes: n, refs: 1}
	return b.sliceRelease(key), nil
}

func (b *budget) sliceRelease(key sliceKey) func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			b.mu.Lock()
			defer b.mu.Unlock()
			h, ok := b.slices[key]
			if !ok {
				return
			}
			h.refs--
			if h.refs <= 0 {
				b.remaining += h.bytes
				delete(b.slices, key)
			}
		})
	}
}

// Compile-time assertion: *budget implements SliceBudget.
var _ SliceBudget = (*budget)(nil)

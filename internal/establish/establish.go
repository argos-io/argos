// Package establish carries a one-shot dial/handshake time budget on a context
// without making that deadline the parent of call-scoped work. sessionpool
// reads it in dialNew; the composition layer sets it from Options.HandshakeTimeout.
package establish

import (
	"context"
	"time"
)

type timeoutKey struct{}

// WithTimeout returns a copy of ctx that carries d as the establishment budget.
// Zero or negative d is ignored (returns ctx unchanged).
func WithTimeout(ctx context.Context, d time.Duration) context.Context {
	if d <= 0 {
		return ctx
	}
	return context.WithValue(ctx, timeoutKey{}, d)
}

// Timeout returns the establishment budget on ctx, or zero if unset.
func Timeout(ctx context.Context) time.Duration {
	if ctx == nil {
		return 0
	}
	d, _ := ctx.Value(timeoutKey{}).(time.Duration)
	return d
}

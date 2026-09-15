// Package filter provides server Filter/Handler chains and client OpenFilter/OpenFunc chains.
//
// Server filters wrap an already-open call; client open-filters wrap the act of
// opening a call. The two are intentionally different shapes (see design §2.3).
package filter

import (
	"context"
	"errors"

	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/stream"
)

// Handler is the terminal (or next) handler for a server-side Filter chain.
type Handler func(ctx context.Context, m descriptor.Method, st stream.Stream) error

// Filter wraps a server handler. Call next at most once; short-circuit by
// returning without calling next.
type Filter func(ctx context.Context, m descriptor.Method, st stream.Stream, next Handler) error

// OpenFunc is the terminal (or next) opener for a client-side OpenFilter chain.
type OpenFunc func(ctx context.Context, m descriptor.Method) (stream.Stream, error)

// OpenFilter wraps the act of opening a call. Modify metadata or derive ctx
// before next; wrap the returned Stream after next. Call next at most once;
// short-circuit by returning an error without calling next.
type OpenFilter func(ctx context.Context, m descriptor.Method, next OpenFunc) (stream.Stream, error)

// ErrOpenFilterMisuse is returned when an OpenFilter returns (nil, nil) without
// calling next, or when next is called more than once.
var ErrOpenFilterMisuse = errors.New("openfilter: must call next or return non-nil error")

// Chain builds an outside-in Handler chain: filters[0] is outermost.
func Chain(filters []Filter, end Handler) Handler {
	h := end
	for i := len(filters) - 1; i >= 0; i-- {
		f, next := filters[i], h
		h = func(ctx context.Context, m descriptor.Method, st stream.Stream) error {
			return f(ctx, m, st, next)
		}
	}
	return h
}

// ChainOpen builds an outside-in OpenFunc chain: filters[0] is outermost.
// Each layer wraps next so that returning (nil, nil) without calling next, or
// calling next more than once, yields ErrOpenFilterMisuse.
func ChainOpen(filters []OpenFilter, end OpenFunc) OpenFunc {
	term := end
	for i := len(filters) - 1; i >= 0; i-- {
		f, next := filters[i], term
		term = func(ctx context.Context, m descriptor.Method) (stream.Stream, error) {
			var nextCalled bool
			wrappedNext := func(ctx context.Context, m descriptor.Method) (stream.Stream, error) {
				if nextCalled {
					return nil, ErrOpenFilterMisuse
				}
				nextCalled = true
				return next(ctx, m)
			}
			st, err := f(ctx, m, wrappedNext)
			if !nextCalled && st == nil && err == nil {
				return nil, ErrOpenFilterMisuse
			}
			return st, err
		}
	}
	return term
}

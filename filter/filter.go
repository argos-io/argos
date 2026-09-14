// Package filter wraps calls with cross-cutting handlers.
package filter

import (
	"context"
	"errors"

	"github.com/argos-io/argos/stream"
)

// Handler is one call after filters: dispatch on the server, or the generated call on the client.
type Handler func(ctx context.Context, method string, st stream.Stream) error

// Filter wraps a Handler. Same type on server and client.
type Filter func(ctx context.Context, method string, st stream.Stream, next Handler) error

// Chain builds a filter chain ending at end.
func Chain(filters []Filter, end Handler) Handler {
	if end == nil {
		return func(context.Context, string, stream.Stream) error {
			return errors.New("filter: nil terminal handler")
		}
	}
	next := end
	for i := len(filters) - 1; i >= 0; i-- {
		filter := filters[i]
		if filter == nil {
			return func(context.Context, string, stream.Stream) error {
				return errors.New("filter: nil filter")
			}
		}
		downstream := next
		next = func(ctx context.Context, method string, st stream.Stream) error {
			return filter(ctx, method, st, downstream)
		}
	}
	return next
}

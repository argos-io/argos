package echov1

import (
	"context"

	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/filter"
	"github.com/argos-io/argos/metadata"
	"github.com/argos-io/argos/status"
	"github.com/argos-io/argos/stream"
)

// ServerAuth rejects calls without authorization metadata.
func ServerAuth(
	ctx context.Context,
	m descriptor.Method,
	st stream.Stream,
	next filter.Handler,
) error {
	md, ok := metadata.FromContext(ctx)
	if !ok || len(md.IncomingHeaders()["authorization"]) == 0 {
		return status.Error(status.Unauthenticated, "missing token")
	}
	return next(ctx, m, st)
}

// ClientAuth attaches a bearer token before the call proceeds.
func ClientAuth(
	ctx context.Context,
	m descriptor.Method,
	next filter.OpenFunc,
) (stream.Stream, error) {
	if md, ok := metadata.FromContext(ctx); ok {
		_ = md.AddOutgoingHeader("authorization", "Bearer x")
	}
	return next(ctx, m)
}

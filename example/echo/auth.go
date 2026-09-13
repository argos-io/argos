package echov1

import (
	"context"

	"github.com/argos-io/argos/errs"
	"github.com/argos-io/argos/filter"
	"github.com/argos-io/argos/metadata"
	"github.com/argos-io/argos/stream"
)

// ServerAuth rejects calls without authorization metadata.
func ServerAuth(
	ctx context.Context,
	method string,
	st stream.Stream,
	next filter.Handler,
) error {
	if metadata.FromContext(ctx)["authorization"] == nil {
		return errs.Error(errs.Unauthenticated, "missing token")
	}
	return next(ctx, method, st)
}

// ClientAuth attaches a bearer token before the call proceeds.
func ClientAuth(
	ctx context.Context,
	method string,
	st stream.Stream,
	next filter.Handler,
) error {
	metadata.FromContext(ctx)["authorization"] = []string{"Bearer x"}
	return next(ctx, method, st)
}

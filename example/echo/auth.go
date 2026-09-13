package echov1

import (
	"context"

	"github.com/argos-io/argos"
)

// ServerAuth rejects calls without authorization metadata.
func ServerAuth(
	ctx context.Context,
	method string,
	st argos.Stream,
	next argos.Handler,
) error {
	if argos.MetadataFromContext(ctx)["authorization"] == nil {
		return argos.Error(argos.Unauthenticated, "missing token")
	}
	return next(ctx, method, st)
}

// ClientAuth attaches a bearer token before the call proceeds.
func ClientAuth(
	ctx context.Context,
	method string,
	st argos.Stream,
	next argos.Handler,
) error {
	argos.MetadataFromContext(ctx)["authorization"] = []string{"Bearer x"}
	return next(ctx, method, st)
}

package server

import (
	"context"
	"errors"

	"github.com/argos-io/argos/status"
)

// statusFromContext maps a canceled/deadline call context to a status error.
// Server-side causes ErrServerShutdown / ErrPeerGone / ErrSessionExpired become
// Unavailable (not Canceled). Plain cancel → Canceled; deadline → DeadlineExceeded.
func statusFromContext(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	err := ctx.Err()
	if err == nil {
		return nil
	}
	cause := context.Cause(ctx)
	switch {
	case errors.Is(cause, ErrServerShutdown),
		errors.Is(cause, ErrPeerGone),
		errors.Is(cause, ErrSessionExpired):
		return status.Error(status.Unavailable, cause.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(status.DeadlineExceeded, err.Error())
	case errors.Is(err, context.Canceled):
		return status.Error(status.Canceled, err.Error())
	default:
		return status.Error(status.Unknown, err.Error())
	}
}

// finishError prefers an explicit handler error; otherwise maps ctx if done.
func finishError(ctx context.Context, handlerErr error) error {
	if handlerErr != nil {
		// If the handler returned a raw context error, remap causes.
		if errors.Is(handlerErr, context.Canceled) || errors.Is(handlerErr, context.DeadlineExceeded) {
			if mapped := statusFromContext(ctx); mapped != nil {
				return mapped
			}
		}
		return handlerErr
	}
	return statusFromContext(ctx)
}

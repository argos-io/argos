package server

import (
	"context"
	"errors"
	"testing"

	"github.com/argos-io/argos/status"
)

func TestStatusFromContextCauses(t *testing.T) {
	cases := []struct {
		name  string
		setup func() context.Context
		code  status.Code
	}{
		{
			name: "shutdown",
			setup: func() context.Context {
				ctx, cancel := context.WithCancelCause(context.Background())
				cancel(ErrServerShutdown)
				return ctx
			},
			code: status.Unavailable,
		},
		{
			name: "peer",
			setup: func() context.Context {
				ctx, cancel := context.WithCancelCause(context.Background())
				cancel(ErrPeerGone)
				return ctx
			},
			code: status.Unavailable,
		},
		{
			name: "expired",
			setup: func() context.Context {
				ctx, cancel := context.WithCancelCause(context.Background())
				cancel(ErrSessionExpired)
				return ctx
			},
			code: status.Unavailable,
		},
		{
			name: "plain cancel",
			setup: func() context.Context {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				return ctx
			},
			code: status.Canceled,
		},
		{
			name: "deadline",
			setup: func() context.Context {
				ctx, cancel := context.WithTimeout(context.Background(), 0)
				defer cancel()
				<-ctx.Done()
				return ctx
			},
			code: status.DeadlineExceeded,
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ctx := tc.setup()
			err := statusFromContext(ctx)
			if status.CodeOf(err) != tc.code {
				t.Fatalf("CodeOf = %v want %v (err=%v cause=%v)", status.CodeOf(err), tc.code, err, context.Cause(ctx))
			}
		})
	}
}

func TestFinishErrorRemapsContextError(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	cancel(ErrPeerGone)
	err := finishError(ctx, context.Canceled)
	if status.CodeOf(err) != status.Unavailable {
		t.Fatalf("got %v want Unavailable", status.CodeOf(err))
	}
	if !errors.Is(context.Cause(ctx), ErrPeerGone) {
		t.Fatal("cause lost")
	}
}

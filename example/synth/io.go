package synth

import (
	"context"
	"io"
	"time"
)

// runIO runs fn in a goroutine and aborts via cancelConn when ctx ends first.
// Used for handshake I/O bounded by HandshakeTimeout.
func runIO(ctx context.Context, cancelConn func(), fn func() error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	errCh := make(chan error, 1)
	go func() { errCh <- fn() }()
	select {
	case <-ctx.Done():
		if cancelConn != nil {
			cancelConn()
		}
		return ctx.Err()
	case err := <-errCh:
		return err
	}
}

func writeAll(ctx context.Context, w io.Writer, closeOnCancel func(), write func(io.Writer) error) error {
	return runIO(ctx, closeOnCancel, func() error { return write(w) })
}

func readAll(ctx context.Context, r io.Reader, closeOnCancel func(), read func(io.Reader) error) error {
	return runIO(ctx, closeOnCancel, func() error { return read(r) })
}

type readDeadliner interface {
	SetReadDeadline(time.Time) error
}

func setReadDeadline(c any, t time.Time) {
	if d, ok := c.(readDeadliner); ok {
		_ = d.SetReadDeadline(t)
	}
}

func clearReadDeadline(c any) {
	setReadDeadline(c, time.Time{})
}

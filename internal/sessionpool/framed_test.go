package sessionpool_test

import (
	"context"
	"errors"
	"github.com/argos-io/argos/internal/sessionpool"
	"testing"

	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/internal/fake"
	"github.com/argos-io/argos/internal/session"
	"github.com/argos-io/argos/transport"
)

// These helpers keep the pool's tests pointed at the fake protocol now that the
// pool itself only knows transport.ClientConn: dial plus handshake, and the
// protocol's reuse model, are the caller's business. They go away with the
// framing package.

func newPool(f session.Framing, dial func(context.Context, string) (transport.Conn, error), cfg sessionpool.Options) *sessionpool.Pool {
	cfg.MaxCallsPerConn = callsPerConn(f.Reuse())
	return sessionpool.New(dialFramed(f, dial), cfg)
}

func callsPerConn(model session.ReuseModel) int {
	if model == session.Concurrent {
		return -1
	}
	return 1
}

func dialFramed(f session.Framing, dial func(context.Context, string) (transport.Conn, error)) sessionpool.DialFunc {
	return func(ctx context.Context, endpoint string) (transport.ClientConn, error) {
		c, err := dial(ctx, endpoint)
		if err != nil {
			return nil, err
		}
		sess, err := f.NewClientSession(ctx, c, session.SessionSpec{})
		if err != nil {
			_ = c.Close()
			return nil, err
		}
		return framedConn{sess: sess}, nil
	}
}

type framedConn struct{ sess session.ClientSession }

func (c framedConn) OpenCall(ctx context.Context, m descriptor.Method, spec transport.CallSpec) (transport.Call, error) {
	call, err := c.sess.OpenCall(ctx, m, session.CallSpec{Metadata: spec.Metadata})
	switch {
	case err == nil:
		return call, nil
	case errors.Is(err, session.ErrSessionBusy):
		return nil, transport.ErrConnBusy
	case errors.Is(err, session.ErrSessionSpent):
		return nil, transport.ErrConnSpent
	default:
		return nil, err
	}
}

func (c framedConn) Reusable() bool { return c.sess.Reusable() }

func (c framedConn) Close() error { return c.sess.Close() }

// fakeSession reaches the fake protocol session inside a pooled connection.
func fakeSession(t *testing.T, conn transport.ClientConn) *fake.ClientSession {
	t.Helper()
	if fs, ok := fake.ClientSessionFromConn(conn); ok {
		return fs
	}
	fc, ok := conn.(framedConn)
	if !ok {
		t.Fatalf("pooled conn is %T, want framedConn or fake clientConn", conn)
	}
	fs, ok := fc.sess.(*fake.ClientSession)
	if !ok {
		t.Fatalf("pooled session is %T, want *fake.ClientSession", fc.sess)
	}
	return fs
}

// fakeSessionOK is fakeSession without the fatal, for probes inside loops.
func fakeSessionOK(conn transport.ClientConn) (*fake.ClientSession, bool) {
	if fs, ok := fake.ClientSessionFromConn(conn); ok {
		return fs, true
	}
	fc, ok := conn.(framedConn)
	if !ok {
		return nil, false
	}
	fs, ok := fc.sess.(*fake.ClientSession)
	return fs, ok
}

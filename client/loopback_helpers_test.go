package client

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/argos-io/argos"
	"github.com/argos-io/argos/codec"
	"github.com/argos-io/argos/internal/fake"
	"github.com/argos-io/argos/internal/session"
	"github.com/argos-io/argos/internal/sessionpool"
	"github.com/argos-io/argos/internal/teststack"
	"github.com/argos-io/argos/transport"
)

// loopbackCodec is the codec every loopback Client uses: it carries no name, so
// the axis/codec name check has nothing to compare (the fake axis names none
// either). A test that wants a mismatch passes its own argos.WithCodec, which
// wins because newClientLoopback appends the test's options last.
const loopbackCodecName = "argos-client-test-loopback"

func init() {
	codec.Register(loopbackCodecName, func() (codec.Codec, error) { return bytesCodec{}, nil })
}

// attachFakePool binds the client-side pool an axis needs before OpenCall. It
// belongs to whoever constructs the axis, not to the Client.
func attachFakePool(ax *fake.Transport, pc sessionpool.Options) {
	ax.AttachPool(sessionpool.New(ax.DialClientConn(), pc))
}

// fakePoolOptions is the pool options a test axis starts from: the built-in
// session-pool baseline in the fake's reuse model. The axis owns its pool
// limits, so nothing here is derived from argos.Options; a test that wants
// another cap edits the returned options before attaching it.
func fakePoolOptions() sessionpool.Options {
	pc := sessionpool.DefaultOptions()
	pc.MaxCallsPerConn = fake.MaxCallsPerConn(session.Sequential)
	return pc
}

func buildLoopbackTransport(t *testing.T, dials *atomic.Int64, dialed *[]*fake.ByteConn, dialMu *sync.Mutex, opts ...fake.Option) *fake.Transport {
	t.Helper()
	f := fake.NewFraming(session.Sequential)
	ax := fake.New(session.Sequential, append([]fake.Option{fake.WithFraming(f), fake.WithDial(func(ctx context.Context, endpoint string) (transport.Conn, error) {
		cli, srv := fake.BytePipe()
		if dialMu != nil && dialed != nil {
			dialMu.Lock()
			*dialed = append(*dialed, cli)
			dialMu.Unlock()
		}
		if dials != nil {
			dials.Add(1)
		}
		go runEchoServer(t, f, srv)
		return cli, nil
	})}, opts...)...)
	return ax
}

// newClientLoopback builds a Client over ax.
//
// The axis is a caller-owned instance: the Client neither closes it nor writes
// to it, and its pool carries the axis' own limits rather than anything read
// off the Client's options. The test owns the axis and closes it after the test
// body returns.
func newClientLoopback(t *testing.T, ax *fake.Transport, opts ...argos.ClientOption) (*Client, error) {
	t.Helper()
	t.Cleanup(func() { _ = ax.Close() })
	all := append([]argos.ClientOption{
		argos.WithTransport(teststack.TransportName(t, ax)),
		argos.WithCodec(loopbackCodecName),
	}, opts...)
	attachFakePool(ax, fakePoolOptions())
	return New(all...)
}

func echoLoopback(t *testing.T, dials *atomic.Int64) *fake.Transport {
	t.Helper()
	return buildLoopbackTransport(t, dials, nil, nil)
}

func fixedLoopback(tr transport.Pipe, fr *fake.Framing) *fake.Transport {
	ax := fake.New(session.Sequential, fake.WithFraming(fr), fake.WithDial(func(ctx context.Context, endpoint string) (transport.Conn, error) {
		return tr.Dial(ctx, transport.DialSpec{Endpoint: endpoint})
	}))
	return ax
}

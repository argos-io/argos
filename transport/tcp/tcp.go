// Package tcp implements a byte-stream Transport over TCP.
//
// Conn is a CarrierConn whose Carrier is itself: ByteStreamCarrier +
// SendCloser. This package moves bytes only; it does not import framing,
// descriptor, codec, or status.
package tcp

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/argos-io/argos/transport"
)

// Compile-time interface checks.
var (
	_ transport.Transport       = (*Transport)(nil)
	_ transport.Conn            = (*Conn)(nil)
	_ transport.CarrierConn     = (*Conn)(nil)
	_ transport.ByteStreamCarrier = (*Conn)(nil)
	_ transport.SendCloser      = (*Conn)(nil)
)

// Transport is a TCP Transport producing CarrierConn connections.
type Transport struct {
	mu sync.Mutex

	listener net.Listener
	conns    map[*Conn]struct{}

	serving  bool
	shutdown bool
	closed   bool

	// onConnWG counts in-flight onConn callbacks.
	onConnWG sync.WaitGroup

	// serveDone is closed when the Accept loop exits.
	serveDone chan struct{}
}

// New returns a TCP Transport.
func New() transport.Transport {
	return &Transport{
		conns: make(map[*Conn]struct{}),
	}
}

// Addr returns the listener address after Serve has bound, or nil.
func (t *Transport) Addr() net.Addr {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.listener == nil {
		return nil
	}
	return t.listener.Addr()
}

// Serve listens on the address from WithListenAddress and accepts connections.
// Each accepted connection is passed to onConn in its own goroutine.
func (t *Transport) Serve(ctx context.Context, onConn func(context.Context, transport.Conn), opts ...transport.ServerOption) error {
	settings := transport.ApplyServerOptions(opts...)
	if settings.ListenAddress == "" {
		return errors.New("tcp: listen address required (use transport.WithListenAddress)")
	}

	ln, err := net.Listen("tcp", settings.ListenAddress)
	if err != nil {
		return fmt.Errorf("tcp: listen %s: %w", settings.ListenAddress, err)
	}

	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		_ = ln.Close()
		return errors.New("tcp: transport closed")
	}
	if t.serving {
		t.mu.Unlock()
		_ = ln.Close()
		return errors.New("tcp: already serving")
	}
	t.listener = ln
	t.serving = true
	t.shutdown = false
	t.serveDone = make(chan struct{})
	serveDone := t.serveDone
	t.mu.Unlock()

	defer func() {
		t.mu.Lock()
		t.serving = false
		if t.listener == ln {
			t.listener = nil
		}
		close(serveDone)
		t.mu.Unlock()
		_ = ln.Close()
	}()

	// Close the listener when ctx is cancelled so Accept unblocks.
	stopAccept := context.AfterFunc(ctx, func() { _ = ln.Close() })
	defer stopAccept()

	for {
		nc, err := ln.Accept()
		if err != nil {
			t.mu.Lock()
			shuttingDown := t.shutdown || t.closed
			t.mu.Unlock()
			if shuttingDown || ctx.Err() != nil {
				if ctx.Err() != nil && !shuttingDown {
					return ctx.Err()
				}
				return nil
			}
			return fmt.Errorf("tcp: accept: %w", err)
		}

		t.mu.Lock()
		if t.shutdown || t.closed {
			t.mu.Unlock()
			_ = nc.Close()
			continue
		}
		c := newConn(nc)
		t.conns[c] = struct{}{}
		t.onConnWG.Add(1)
		t.mu.Unlock()

		go func(c *Conn) {
			defer t.onConnWG.Done()
			defer t.untrack(c)
			onConn(ctx, c)
		}(c)
	}
}

// Dial connects to spec.Endpoint and returns a Conn wrapping the net.Conn.
func (t *Transport) Dial(ctx context.Context, spec transport.DialSpec, _ ...transport.ClientOption) (transport.Conn, error) {
	if spec.Endpoint == "" {
		return nil, errors.New("tcp: empty DialSpec.Endpoint")
	}
	t.mu.Lock()
	closed := t.closed
	t.mu.Unlock()
	if closed {
		return nil, errors.New("tcp: transport closed")
	}

	var d net.Dialer
	nc, err := d.DialContext(ctx, "tcp", spec.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("tcp: dial %s: %w", spec.Endpoint, err)
	}
	c := newConn(nc)

	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		_ = c.Close()
		return nil, errors.New("tcp: transport closed")
	}
	t.conns[c] = struct{}{}
	t.mu.Unlock()
	return c, nil
}

// Shutdown stops accepting new connections and waits for in-flight onConn
// callbacks. If ctx expires before they finish, remaining connections are
// closed to unblock them.
func (t *Transport) Shutdown(ctx context.Context) error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.shutdown = true
	ln := t.listener
	serveDone := t.serveDone
	t.mu.Unlock()

	if ln != nil {
		_ = ln.Close()
	}

	done := make(chan struct{})
	go func() {
		t.onConnWG.Wait()
		close(done)
	}()

	select {
	case <-done:
		if serveDone != nil {
			select {
			case <-serveDone:
			case <-ctx.Done():
				// Accept loop may still be exiting; connections are idle.
			}
		}
		return nil
	case <-ctx.Done():
		t.closeAllConns()
		<-done
		return ctx.Err()
	}
}

// Close closes the listener and all tracked connections. Idempotent.
func (t *Transport) Close() error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.closed = true
	t.shutdown = true
	ln := t.listener
	t.mu.Unlock()

	if ln != nil {
		_ = ln.Close()
	}
	t.closeAllConns()
	return nil
}

func (t *Transport) untrack(c *Conn) {
	t.mu.Lock()
	delete(t.conns, c)
	t.mu.Unlock()
}

func (t *Transport) closeAllConns() {
	t.mu.Lock()
	conns := make([]*Conn, 0, len(t.conns))
	for c := range t.conns {
		conns = append(conns, c)
	}
	t.mu.Unlock()
	for _, c := range conns {
		_ = c.Close()
	}
}

// Conn wraps a net.Conn as a CarrierConn / ByteStreamCarrier / SendCloser.
type Conn struct {
	nc     net.Conn
	closed atomic.Bool
}

func newConn(nc net.Conn) *Conn {
	return &Conn{nc: nc}
}

// Carrier returns the Conn itself as the exchange carrier.
func (c *Conn) Carrier() transport.Carrier { return c }

// Addr returns the local address of the underlying connection.
func (c *Conn) Addr() net.Addr { return c.nc.LocalAddr() }

// Read implements io.Reader.
func (c *Conn) Read(p []byte) (int, error) { return c.nc.Read(p) }

// Write implements io.Writer.
func (c *Conn) Write(p []byte) (int, error) { return c.nc.Write(p) }

// CloseSend half-closes the write side (TCP FIN).
func (c *Conn) CloseSend() error {
	if tc, ok := c.nc.(*net.TCPConn); ok {
		return tc.CloseWrite()
	}
	type closeWriter interface {
		CloseWrite() error
	}
	if cw, ok := c.nc.(closeWriter); ok {
		return cw.CloseWrite()
	}
	return errors.New("tcp: CloseWrite not supported")
}

// Abort poisons the connection by setting deadlines in the past and closing.
func (c *Conn) Abort() error {
	_ = c.nc.SetDeadline(time.Unix(1, 0))
	return c.Close()
}

// Close closes the underlying connection. Idempotent.
func (c *Conn) Close() error {
	if c.closed.Swap(true) {
		return nil
	}
	return c.nc.Close()
}

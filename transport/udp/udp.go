// Package udp implements a datagram Transport over UDP.
//
// Conn is a CarrierConn whose Carrier is itself: DatagramCarrier.
// Each SendDatagram/RecvDatagram moves one UDP datagram. The practical IPv4
// maximum payload is MaxDatagramSize (65507); larger sends fail without a
// write. This package only moves datagrams; framing OneCallPerConn is
// enforced above. A server association (keyed by remote address) may carry
// sequential exchanges until the Conn is closed.
package udp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/argos-io/argos/transport"
)

// MaxDatagramSize is the practical maximum UDP payload for IPv4
// (65535 − 8 UDP − 20 IP). Binding/framing layers use this for startup
// size checks; SendDatagram rejects larger payloads at runtime.
const MaxDatagramSize = 65507

// Compile-time interface checks.
var (
	_ transport.Transport       = (*Transport)(nil)
	_ transport.Conn            = (*Conn)(nil)
	_ transport.CarrierConn     = (*Conn)(nil)
	_ transport.DatagramCarrier = (*Conn)(nil)
)

// Transport is a UDP Transport producing CarrierConn connections.
type Transport struct {
	mu sync.Mutex

	pc    net.PacketConn
	conns map[*Conn]struct{}
	// assocs demuxes inbound datagrams by remote address while serving.
	assocs map[string]*Conn

	serving  bool
	shutdown bool
	closed   bool

	onConnWG sync.WaitGroup

	serveDone chan struct{}
}

// New returns a UDP Transport.
func New() transport.Transport {
	return &Transport{
		conns:  make(map[*Conn]struct{}),
		assocs: make(map[string]*Conn),
	}
}

// Addr returns the listener address after Serve has bound, or nil.
func (t *Transport) Addr() net.Addr {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.pc == nil {
		return nil
	}
	return t.pc.LocalAddr()
}

// Serve listens on the address from WithListenAddress and accepts associations.
// The first datagram from a remote address creates a Conn; further datagrams
// from that address are delivered on the same Conn until it is closed.
func (t *Transport) Serve(ctx context.Context, onConn func(context.Context, transport.Conn), opts ...transport.ServerOption) error {
	settings := transport.ApplyServerOptions(opts...)
	if settings.ListenAddress == "" {
		return errors.New("udp: listen address required (use transport.WithListenAddress)")
	}

	pc, err := net.ListenPacket("udp", settings.ListenAddress)
	if err != nil {
		return fmt.Errorf("udp: listen %s: %w", settings.ListenAddress, err)
	}

	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		_ = pc.Close()
		return errors.New("udp: transport closed")
	}
	if t.serving {
		t.mu.Unlock()
		_ = pc.Close()
		return errors.New("udp: already serving")
	}
	t.pc = pc
	t.serving = true
	t.shutdown = false
	t.serveDone = make(chan struct{})
	serveDone := t.serveDone
	t.mu.Unlock()

	defer func() {
		t.mu.Lock()
		t.serving = false
		if t.pc == pc {
			t.pc = nil
		}
		close(serveDone)
		t.mu.Unlock()
		_ = pc.Close()
	}()

	stopAccept := context.AfterFunc(ctx, func() { _ = pc.Close() })
	defer stopAccept()

	buf := make([]byte, MaxDatagramSize)
	for {
		n, addr, err := pc.ReadFrom(buf)
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
			return fmt.Errorf("udp: read: %w", err)
		}

		payload := append([]byte(nil), buf[:n]...)
		key := addr.String()

		t.mu.Lock()
		if t.shutdown || t.closed {
			t.mu.Unlock()
			continue
		}
		if c, ok := t.assocs[key]; ok {
			if !c.closed.Load() {
				t.mu.Unlock()
				c.deliver(payload)
				continue
			}
			// Association closed but onConn may still be exiting; replace it.
			delete(t.assocs, key)
			delete(t.conns, c)
		}

		c := newServerConn(pc, addr)
		t.assocs[key] = c
		t.conns[c] = struct{}{}
		t.onConnWG.Add(1)
		t.mu.Unlock()

		c.deliver(payload)

		go func(c *Conn, key string) {
			defer t.onConnWG.Done()
			defer t.untrackAssoc(c, key)
			onConn(ctx, c)
		}(c, key)
	}
}

// Dial opens a UDP association to spec.Endpoint and returns a Conn.
func (t *Transport) Dial(ctx context.Context, spec transport.DialSpec, _ ...transport.ClientOption) (transport.Conn, error) {
	if spec.Endpoint == "" {
		return nil, errors.New("udp: empty DialSpec.Endpoint")
	}
	t.mu.Lock()
	closed := t.closed
	t.mu.Unlock()
	if closed {
		return nil, errors.New("udp: transport closed")
	}

	var d net.Dialer
	nc, err := d.DialContext(ctx, "udp", spec.Endpoint)
	if err != nil {
		return nil, fmt.Errorf("udp: dial %s: %w", spec.Endpoint, err)
	}
	uc, ok := nc.(*net.UDPConn)
	if !ok {
		_ = nc.Close()
		return nil, fmt.Errorf("udp: dial %s: unexpected conn type %T", spec.Endpoint, nc)
	}
	c := newClientConn(uc)
	// A dialed association is tracked only while it is open: Session recycles
	// connections on idle/lifetime timeouts, and the transport must not retain
	// closed ones until Close.
	c.detach = func() { t.untrack(c) }

	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		_ = c.Close()
		return nil, errors.New("udp: transport closed")
	}
	t.conns[c] = struct{}{}
	t.mu.Unlock()
	return c, nil
}

// Shutdown stops accepting new associations and waits for in-flight onConn
// callbacks. If ctx expires before they finish, remaining connections are
// closed to unblock them.
func (t *Transport) Shutdown(ctx context.Context) error {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return nil
	}
	t.shutdown = true
	pc := t.pc
	serveDone := t.serveDone
	t.mu.Unlock()

	if pc != nil {
		_ = pc.Close()
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
			}
		}
		return nil
	case <-ctx.Done():
		t.closeAllConns()
		// A callback that ignores Conn.Close must not hold Shutdown open past
		// its deadline.
		select {
		case <-done:
		case <-ctx.Done():
		}
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
	pc := t.pc
	t.mu.Unlock()

	if pc != nil {
		_ = pc.Close()
	}
	t.closeAllConns()
	return nil
}

func (t *Transport) untrack(c *Conn) {
	t.mu.Lock()
	delete(t.conns, c)
	t.mu.Unlock()
}

func (t *Transport) untrackAssoc(c *Conn, key string) {
	_ = c.Close()
	t.mu.Lock()
	delete(t.conns, c)
	if t.assocs[key] == c {
		delete(t.assocs, key)
	}
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

// Conn wraps a UDP association as a CarrierConn / DatagramCarrier.
type Conn struct {
	// client path: connected UDPConn owned by this Conn.
	uc *net.UDPConn
	// server path: shared PacketConn + fixed remote.
	pc     net.PacketConn
	remote net.Addr

	recv   chan []byte
	done   chan struct{}
	closed atomic.Bool

	// closeOnce guards closing done.
	closeOnce sync.Once

	// detach removes this Conn from the Transport's tracking map when it
	// closes, so the map never retains a closed association.
	detach func()

	// recvMu serialises client reads and owns recvBuf, a reusable
	// MaxDatagramSize landing area. Returning a slice of a freshly allocated
	// 64 KiB buffer instead kept the whole array alive for as long as the
	// caller held the payload.
	recvMu  sync.Mutex
	recvBuf []byte
}

func newClientConn(uc *net.UDPConn) *Conn {
	return &Conn{
		uc:   uc,
		done: make(chan struct{}),
	}
}

func newServerConn(pc net.PacketConn, remote net.Addr) *Conn {
	return &Conn{
		pc:     pc,
		remote: remote,
		recv:   make(chan []byte, 32),
		done:   make(chan struct{}),
	}
}

// Carrier returns the Conn itself as the exchange carrier.
func (c *Conn) Carrier() transport.Carrier { return c }

// Addr returns the local address of the underlying socket, or nil.
func (c *Conn) Addr() net.Addr {
	if c.uc != nil {
		return c.uc.LocalAddr()
	}
	if c.pc != nil {
		return c.pc.LocalAddr()
	}
	return nil
}

// RemoteAddr returns the peer address for this association, or nil.
func (c *Conn) RemoteAddr() net.Addr {
	if c.remote != nil {
		return c.remote
	}
	if c.uc != nil {
		return c.uc.RemoteAddr()
	}
	return nil
}

func (c *Conn) deliver(p []byte) {
	select {
	case <-c.done:
		return
	case c.recv <- p:
	default:
		// Drop when the inbox is full; UDP is unreliable.
	}
}

// RecvDatagram reads one UDP datagram.
func (c *Conn) RecvDatagram() ([]byte, error) {
	if c.recv != nil {
		select {
		case <-c.done:
			return nil, net.ErrClosed
		case p, ok := <-c.recv:
			if !ok {
				return nil, net.ErrClosed
			}
			return p, nil
		}
	}

	if c.uc == nil {
		return nil, net.ErrClosed
	}
	c.recvMu.Lock()
	defer c.recvMu.Unlock()
	if c.recvBuf == nil {
		c.recvBuf = make([]byte, MaxDatagramSize)
	}
	n, err := c.uc.Read(c.recvBuf)
	if err != nil {
		if c.closed.Load() {
			return nil, net.ErrClosed
		}
		return nil, err
	}
	// Copy out: recvBuf is reused by the next read.
	return append([]byte(nil), c.recvBuf[:n]...), nil
}

// SendDatagram writes one UDP datagram. Payloads larger than MaxDatagramSize
// fail without writing.
//
// Every I/O failure here is terminal for the exchange: UDP carries one datagram
// per call, so a datagram that never left (or left truncated) can never be
// answered. ReceiveOpen is therefore false — unlike byte-stream transports,
// where a failed write may still be followed by a readable response.
func (c *Conn) SendDatagram(p []byte) error {
	if c.closed.Load() {
		return transport.WrapSendError(net.ErrClosed, false)
	}
	if len(p) > MaxDatagramSize {
		return transport.WrapSendError(
			fmt.Errorf("udp: datagram is %d bytes; maximum is %d", len(p), MaxDatagramSize),
			false,
		)
	}

	var (
		n   int
		err error
	)
	switch {
	case c.uc != nil:
		n, err = c.uc.Write(p)
	case c.pc != nil && c.remote != nil:
		n, err = c.pc.WriteTo(p, c.remote)
	default:
		return transport.WrapSendError(net.ErrClosed, false)
	}
	if err != nil {
		return transport.WrapSendError(err, false)
	}
	if n < len(p) {
		return transport.WrapSendError(io.ErrShortWrite, false)
	}
	return nil
}

// Abort interrupts in-flight I/O and closes the association.
func (c *Conn) Abort() error {
	if c.uc != nil {
		_ = c.uc.SetDeadline(time.Unix(1, 0))
	}
	return c.Close()
}

// Close closes the association. Idempotent. On the server path it does not
// close the shared PacketConn.
func (c *Conn) Close() error {
	if c.closed.Swap(true) {
		return nil
	}
	c.closeOnce.Do(func() { close(c.done) })
	var err error
	if c.uc != nil {
		err = c.uc.Close()
	}
	if c.detach != nil {
		c.detach()
	}
	return err
}

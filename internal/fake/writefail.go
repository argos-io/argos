package fake

import (
	"errors"
	"net"
	"sync"

	"github.com/argos-io/argos/transport"
)

// WriteFailConn is a net.Conn whose write direction can be broken on demand
// while reads keep working.
//
// A real transport cannot be made to fail a write at a chosen moment: the
// failure arrives when the kernel or the peer decides, which makes the case
// "the send direction failed but the exchange is still readable" (the case
// transport.SendError exists for) untestable without a seam. This Conn is that
// seam: it wraps the wire, not the Transport, so the Transport's own Frame
// handling stays in the path.
//
// Write returns a transport.SendError, the same classification
// transport.WrapSendError produces, so a fake Conn stands in for a Transport's
// verdict about the exchange. ReceiveOpen false models a dead exchange.
type WriteFailConn struct {
	net.Conn

	mu          sync.Mutex
	err         error
	receiveOpen bool
	after       int // writes allowed through before failing
	wire        int
	failures    int
}

// BreakWrites wraps nc so that its writes can be made to fail on demand.
func BreakWrites(nc net.Conn) *WriteFailConn {
	return &WriteFailConn{Conn: nc, receiveOpen: true}
}

// Fail makes every subsequent Write fail with err, classified with
// SendError.ReceiveOpen == receiveOpen. Nil err becomes a plain wire failure.
func (c *WriteFailConn) Fail(err error, receiveOpen bool) {
	c.FailAfter(0, err, receiveOpen)
}

// FailAfter makes Write fail with err once n further writes have gone through,
// which lets a caller complete part of its upload before the send direction
// breaks.
func (c *WriteFailConn) FailAfter(n int, err error, receiveOpen bool) {
	if err == nil {
		err = errors.New("fake: write failed")
	}
	c.mu.Lock()
	c.err, c.receiveOpen, c.after = err, receiveOpen, n
	c.mu.Unlock()
}

// FailedWrites returns how many writes have been failed.
func (c *WriteFailConn) FailedWrites() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.failures
}

// WireWrites returns how many writes reached the underlying Conn.
func (c *WriteFailConn) WireWrites() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.wire
}

// Write implements io.Writer: it fails once Fail or FailAfter armed the Conn,
// and passes through to the wire otherwise.
func (c *WriteFailConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	if c.err == nil || c.after > 0 {
		if c.err != nil {
			c.after--
		}
		c.wire++
		c.mu.Unlock()
		return c.Conn.Write(p)
	}
	err, open := c.err, c.receiveOpen
	c.failures++
	c.mu.Unlock()
	return 0, transport.WrapSendError(err, open)
}

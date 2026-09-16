package synth

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/status"
)

type recvItem struct {
	payload []byte
	err     error
}

type call struct {
	session *session
	method  string
	client  bool
	shape   descriptor.Shape

	ctx    context.Context
	cancel context.CancelFunc

	recvCh chan recvItem
	done   chan struct{} // recvLoop exited

	mu sync.Mutex

	peerHalfClosed bool
	sendClosed     bool
	finished       bool
	closed         bool
	gotTerminal    bool // STATUS or io.EOF on recv
	recvErr        error
}

type serverCall struct{ *call }

func (c *call) Method() string { return c.method }

func (c *call) Deadline() (time.Time, bool) { return time.Time{}, false }

func (c *call) SendHeaders() error {
	return status.Error(status.Unimplemented, "synth: no metadata channel")
}

func (c *call) Recv() (payload []byte, release func(), err error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, nil, status.Error(status.Canceled, "synth: call closed")
	}
	c.mu.Unlock()

	// Buffered items win over the completion signal. recvLoop pushes the final
	// item and only then closes done, so a select over both would pick at
	// random and silently drop an already-delivered message.
	select {
	case it, ok := <-c.recvCh:
		return c.recvItem(it, ok)
	default:
	}

	select {
	case <-c.ctx.Done():
		return nil, nil, c.ctx.Err()
	case it, ok := <-c.recvCh:
		return c.recvItem(it, ok)
	case <-c.done:
		// The check above is not enough: recvLoop can push its last items and
		// close done in the gap between that check and this select, and then
		// both cases are ready and Go picks at random. Drain the inbox before
		// reporting the end — an item pushed before done was closed is in the
		// channel buffer by the time the close is observable.
		select {
		case it, ok := <-c.recvCh:
			return c.recvItem(it, ok)
		default:
		}
		if err := c.ctx.Err(); err != nil {
			return nil, nil, err
		}
		c.mu.Lock()
		err := c.recvErr
		c.mu.Unlock()
		if err == nil {
			err = io.EOF
		}
		return nil, nil, err
	}
}

// recvItem does not touch gotTerminal. Setting it on any io.EOF made a client
// that consumed the peer's half-close look like it had read the STATUS, so a
// session with an unread terminal went back to the pool (§4.4). recvLoop sets
// the flag when it actually reads STATUS, which is the fact the pool needs.
func (c *call) recvItem(it recvItem, ok bool) ([]byte, func(), error) {
	if !ok {
		return nil, nil, io.EOF
	}
	if it.err != nil {
		return nil, nil, it.err
	}
	return it.payload, func() {}, nil
}

func (c *call) Send(payload []byte) error {
	c.mu.Lock()
	if c.closed || c.sendClosed {
		c.mu.Unlock()
		return status.Error(status.Unavailable, "synth: send closed")
	}
	if c.session.maxMessage > 0 && int64(len(payload)) > c.session.maxMessage {
		c.mu.Unlock()
		return status.Error(status.ResourceExhausted, "synth: message too large")
	}
	c.mu.Unlock()

	c.session.writeMu.Lock()
	err := writeFrame(c.session.carrier, frame{typ: typeData, data: payload})
	c.session.writeMu.Unlock()
	if err != nil {
		c.session.markBad()
		return err
	}
	return nil
}

func (c *call) HalfClose() error {
	if !c.client {
		return status.Error(status.Unimplemented, "synth: HalfClose is initiator-only")
	}
	c.mu.Lock()
	if c.sendClosed {
		c.mu.Unlock()
		return nil
	}
	c.sendClosed = true
	c.mu.Unlock()

	c.session.writeMu.Lock()
	err := writeFrame(c.session.carrier, frame{typ: typeEnd})
	c.session.writeMu.Unlock()
	if err != nil {
		c.session.markBad()
		return err
	}
	return nil
}

func (c *call) Finish(err error) error {
	if c.client {
		return status.Error(status.Unimplemented, "synth: Finish is responder-only")
	}
	c.mu.Lock()
	if c.finished {
		c.mu.Unlock()
		return status.Error(status.Internal, "synth: Finish called twice")
	}
	c.finished = true
	c.mu.Unlock()

	code := uint32(status.OK)
	msg := ""
	if err != nil {
		code = uint32(status.CodeOf(err))
		msg = err.Error()
	}
	c.session.writeMu.Lock()
	werr := writeFrame(c.session.carrier, frame{typ: typeStatus, code: code, message: msg})
	c.session.writeMu.Unlock()
	if werr != nil {
		c.session.markBad()
		return werr
	}
	c.mu.Lock()
	c.gotTerminal = true
	c.mu.Unlock()
	return nil
}

func (c *call) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	gotTerminal := c.gotTerminal
	// atEnd reports that recvLoop has read the peer's end of the stream and is
	// therefore on its way out: STATUS for an initiator, END for a responder.
	// Both flags are set by recvLoop before its last hand-off, and every path
	// after them returns, so waiting for done is bounded.
	atEnd := gotTerminal
	if !c.client {
		atEnd = c.peerHalfClosed
	}
	c.mu.Unlock()

	c.cancel()
	poison := c.client && !gotTerminal
	if atEnd {
		// Wait it out. Aborting in this window killed a healthy connection the
		// pool was about to reuse, and the next OpenCall on it failed with
		// EPIPE — recvLoop had delivered the peer's end but had not reached
		// close(done) yet.
		<-c.done
	} else {
		select {
		case <-c.done:
			// recvLoop already exited (clean terminal or prior error).
		default:
			// The peer still owes us bytes, so recvLoop is parked in a socket
			// read that only Abort can interrupt. Abort poisons Sequential
			// reuse — correct when we close without a protocol terminal.
			poison = true
			_ = c.session.carrier.Abort()
			<-c.done
		}
	}

	c.session.detach(c, poison)
	return nil
}

func (c *serverCall) Accept(m descriptor.Method) error {
	_ = m
	return nil
}

func (c *call) recvLoop() {
	defer close(c.done)
	for {
		f, err := readFrame(c.session.carrier, c.session.maxFrame)
		if err != nil {
			c.mu.Lock()
			if c.closed {
				c.mu.Unlock()
				return
			}
			c.recvErr = err
			c.mu.Unlock()
			if err != io.EOF {
				c.session.markBad()
			}
			select {
			case c.recvCh <- recvItem{err: err}:
			default:
			}
			return
		}
		switch f.typ {
		case typeData:
			select {
			case <-c.ctx.Done():
				return
			case c.recvCh <- recvItem{payload: f.data}:
			}
		case typeEnd:
			c.mu.Lock()
			c.peerHalfClosed = true
			c.mu.Unlock()
			select {
			case <-c.ctx.Done():
				return
			case c.recvCh <- recvItem{err: io.EOF}:
			}
			// Client continues reading until STATUS.
			if c.client {
				continue
			}
			return
		case typeStatus:
			c.mu.Lock()
			c.gotTerminal = true
			c.mu.Unlock()
			var term error = io.EOF
			if f.code != uint32(status.OK) {
				term = status.Error(status.Code(f.code), f.message)
			}
			select {
			case <-c.ctx.Done():
				return
			case c.recvCh <- recvItem{err: term}:
			}
			return
		case typeOpen:
			c.session.markBad()
			select {
			case c.recvCh <- recvItem{err: status.Error(status.Internal, "synth: unexpected OPEN")}:
			default:
			}
			return
		default:
			c.session.markBad()
			select {
			case c.recvCh <- recvItem{err: errUnknownType}:
			default:
			}
			return
		}
	}
}

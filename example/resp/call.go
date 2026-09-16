package resp

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/status"
)

var (
	errCallClosed   = errors.New("resp: call closed")
	errSendFinished = errors.New("resp: send already finished")
)

type call struct {
	sess      *session
	method    string // full name
	cmd       string // Redis command (PING/GET/SET/SUBSCRIBE/…)
	initiator bool
	streaming bool // server-streaming (SUBSCRIBE): multiple Recv/Send

	ctx    context.Context
	cancel context.CancelFunc

	mu sync.Mutex

	args       []string // server: parsed args awaiting Recv
	argsSent   bool     // server Recv delivered
	reqSent    bool     // client Write done
	halfClosed bool
	replySent  bool // server wrote at least one reply via Send
	finished   bool
	closed     bool
	terminal   bool // client saw stream end (unary reply or cancel/EOF)

	sending atomic.Bool
	recving atomic.Bool
}

// newCall builds one call's state. parent becomes the parent of the call ctx and
// therefore the authority for its cancellation and deadline.
func newCall(s *session, fullMethod, cmd string, initiator bool, parent context.Context) *call {
	ctx, cancel := context.WithCancel(parent)
	streaming := strings.EqualFold(cmd, "SUBSCRIBE")
	return &call{
		sess:      s,
		method:    fullMethod,
		cmd:       cmd,
		initiator: initiator,
		streaming: streaming,
		ctx:       ctx,
		cancel:    cancel,
	}
}

func (c *call) Method() string { return c.method }

// Deadline reports the deadline the call observes. For an initiator that is the
// deadline of the ctx passed to OpenCall — the caller's own deadline, which
// bounds its Recv and is what the composition layer derives the call ctx from.
// A responder has no inbound deadline channel in RESP2 and reports none.
func (c *call) Deadline() (time.Time, bool) { return c.ctx.Deadline() }

func (c *call) SendHeaders() error {
	// RESP has no metadata channel — returning Unimplemented must not require
	// a kernel hole, and must not break subsequent calls on this session.
	return status.Error(status.Unimplemented, "resp: SendHeaders unsupported (no metadata channel)")
}

func (c *call) Recv() (payload []byte, release func(), err error) {
	if !c.recving.CompareAndSwap(false, true) {
		return nil, nil, errors.New("resp: concurrent Recv")
	}
	defer c.recving.Store(false)

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, nil, errCallClosed
	}
	c.mu.Unlock()

	if c.initiator {
		return c.recvClient()
	}
	return c.recvServer()
}

func (c *call) recvServer() ([]byte, func(), error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.argsSent {
		return nil, nil, io.EOF
	}
	c.argsSent = true
	payload := EncodeArgs(c.args...)
	return payload, func() {}, nil
}

func (c *call) recvClient() ([]byte, func(), error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, nil, errCallClosed
	}
	if c.terminal {
		c.mu.Unlock()
		return nil, nil, io.EOF
	}
	streaming := c.streaming
	c.mu.Unlock()

	v, err := c.sess.readValue(c.ctx)
	if err != nil {
		c.mu.Lock()
		closed := c.closed
		c.terminal = true
		c.mu.Unlock()
		if closed {
			return nil, nil, errCallClosed
		}
		c.sess.markBad()
		return nil, nil, err
	}
	if !streaming {
		c.mu.Lock()
		c.terminal = true
		c.mu.Unlock()
	}
	if v.Type == '-' {
		return nil, nil, status.Error(status.Unknown, v.Str)
	}
	return MarshalValue(v), func() {}, nil
}

func (c *call) Send(payload []byte) error {
	if !c.sending.CompareAndSwap(false, true) {
		return errors.New("resp: concurrent Send")
	}
	defer c.sending.Store(false)

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return errCallClosed
	}
	if c.initiator && (c.reqSent || c.halfClosed) {
		c.mu.Unlock()
		return errSendFinished
	}
	if !c.initiator && !c.streaming && (c.replySent || c.finished) {
		c.mu.Unlock()
		return errSendFinished
	}
	if !c.initiator && c.streaming && c.finished {
		c.mu.Unlock()
		return errSendFinished
	}
	c.mu.Unlock()

	if c.initiator {
		return c.sendClient(payload)
	}
	return c.sendServer(payload)
}

func (c *call) sendClient(payload []byte) error {
	var args []string
	if len(payload) > 0 {
		var err error
		args, err = DecodeArgs(payload)
		if err != nil {
			return err
		}
	}
	elems := make([]string, 0, 1+len(args))
	elems = append(elems, c.cmd)
	elems = append(elems, args...)
	if err := c.sess.writeRaw(EncodeArray(elems...)); err != nil {
		return err
	}
	c.mu.Lock()
	c.reqSent = true
	c.mu.Unlock()
	return nil
}

func (c *call) sendServer(payload []byte) error {
	data := payload
	if data == nil {
		data = []byte{}
	}
	if err := c.sess.writeRaw(append([]byte(nil), data...)); err != nil {
		return err
	}
	c.mu.Lock()
	c.replySent = true
	c.mu.Unlock()
	return nil
}

func (c *call) HalfClose() error {
	if !c.initiator {
		return status.Error(status.Unimplemented, "resp: HalfClose unsupported for responder")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return errCallClosed
	}
	c.halfClosed = true
	// Wire: request is already a complete command packet on Send; nothing to write.
	return nil
}

func (c *call) Finish(err error) error {
	if c.initiator {
		return status.Error(status.Unimplemented, "resp: Finish unsupported for initiator")
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return errCallClosed
	}
	if c.finished {
		c.mu.Unlock()
		return errSendFinished
	}
	sent := c.replySent
	c.finished = true
	c.mu.Unlock()

	if sent {
		// Success: response itself is terminal; Finish(nil) writes nothing.
		return nil
	}
	if err == nil {
		return nil
	}
	msg := err.Error()
	var se *status.StatusError
	if errors.As(err, &se) && se.Message() != "" {
		msg = se.Message()
	}
	if werr := c.sess.writeRaw(EncodeError("ERR " + msg)); werr != nil {
		return werr
	}
	c.mu.Lock()
	c.replySent = true
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
	poison := false
	if c.initiator {
		if c.streaming {
			// Exclusive long-lived call: never return the connection to the pool.
			poison = true
			c.terminal = true
		} else {
			poison = !c.terminal
		}
	} else {
		poison = !c.replySent && !c.finished
		if c.streaming {
			poison = true
		}
	}
	c.mu.Unlock()
	c.cancel()
	c.sess.endCall(poison)
	return nil
}

type serverCall struct {
	*call
}

func (c *serverCall) Accept(m descriptor.Method) error {
	want := descriptor.Unary
	if c.streaming {
		want = descriptor.ServerStreaming
	}
	if m.Shape() != want {
		return status.Error(status.Unimplemented,
			"resp: shape "+shapeName(m.Shape())+" unsupported (Framing=resp); need "+shapeName(want))
	}
	if m.FullName() != c.method {
		return status.Error(status.Internal, "resp: Accept method mismatch")
	}
	return nil
}

func shapeName(s descriptor.Shape) string {
	switch s {
	case descriptor.Unary:
		return "Unary"
	case descriptor.ServerStreaming:
		return "ServerStreaming"
	case descriptor.ClientStreaming:
		return "ClientStreaming"
	case descriptor.BidiStreaming:
		return "BidiStreaming"
	default:
		return "unknown"
	}
}

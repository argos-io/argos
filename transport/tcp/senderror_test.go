package tcp

import (
	"errors"
	"io"
	"net"
	"testing"

	"github.com/argos-io/argos/transport"
)

// A TCP write failure does not imply the peer stopped sending: the response may
// still sit in the receive buffer. Send must therefore be classifiable as a
// transport.SendError whose receive direction is open, so stream can
// report the send direction finished (stream.ErrSendClosed) instead of tearing
// the exchange down and losing the remote status.
func TestWriteFailureOnLiveConnKeepsReceiveOpen(t *testing.T) {
	a, b := net.Pipe()
	t.Cleanup(func() { _ = a.Close() })
	c := newConn(a)

	// Peer gone: writes fail, but this Conn is not closed and has not read EOF.
	_ = b.Close()

	_, err := c.Write([]byte("ping"))
	if err == nil {
		t.Fatal("Write succeeded after the peer hung up")
	}
	var se transport.SendError
	if !errors.As(err, &se) {
		t.Fatalf("Write error %v (%T) is not a transport.SendError", err, err)
	}
	if !se.ReceiveOpen() {
		t.Fatal("ReceiveOpen() = false; a write failure alone does not prove the receive direction is dead")
	}
}

// Once this Conn is closed the receive direction is known to be unrecoverable,
// so the ambiguity above must resolve to ReceiveOpen() == false.
func TestWriteFailureAfterCloseReportsReceiveClosed(t *testing.T) {
	a, b := net.Pipe()
	t.Cleanup(func() { _ = b.Close() })
	c := newConn(a)
	_ = c.Close()

	_, err := c.Write([]byte("ping"))
	if err == nil {
		t.Fatal("Write succeeded on a closed Conn")
	}
	var se transport.SendError
	if !errors.As(err, &se) {
		t.Fatalf("Write error %v (%T) is not a transport.SendError", err, err)
	}
	if se.ReceiveOpen() {
		t.Fatal("ReceiveOpen() = true on a closed Conn")
	}
}

// A carrier without a half-close capability is a capability error, not a send
// failure: framing must report it as-is (Unimplemented), never as a finished
// send direction.
func TestCloseSendUnsupportedIsNotASendError(t *testing.T) {
	a, b := net.Pipe()
	t.Cleanup(func() { _ = a.Close() })
	defer b.Close()
	c := newConn(a)

	err := c.CloseSend()
	if err == nil {
		t.Fatal("net.Pipe has no CloseWrite; CloseSend must report unsupported")
	}
	if !errors.Is(err, io.ErrClosedPipe) {
		var se transport.SendError
		if errors.As(err, &se) {
			t.Fatalf("CloseSend capability error %v must not be a transport.SendError", err)
		}
	}
}

// closeWriterConn is a net.Conn whose CloseWrite fails on demand: the FIN could
// not be delivered, which is a send-direction failure.
type closeWriterConn struct {
	net.Conn
	err error
}

func (c *closeWriterConn) CloseWrite() error { return c.err }

func TestCloseSendIOFailureKeepsReceiveOpen(t *testing.T) {
	a, b := net.Pipe()
	t.Cleanup(func() { _ = a.Close() })
	defer b.Close()

	want := errors.New("finite: no FIN")
	c := newConn(&closeWriterConn{Conn: a, err: want})

	err := c.CloseSend()
	if !errors.Is(err, want) {
		t.Fatalf("CloseSend = %v, want %v", err, want)
	}
	var se transport.SendError
	if !errors.As(err, &se) {
		t.Fatalf("CloseSend error %v (%T) is not a transport.SendError", err, err)
	}
	if !se.ReceiveOpen() {
		t.Fatal("ReceiveOpen() = false; a failed FIN does not close the receive direction")
	}
}

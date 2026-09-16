package stream_test

import (
	"bytes"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/argos-io/argos/framing"
	"github.com/argos-io/argos/stream"
	"github.com/argos-io/argos/transport"
)

func TestErrSendClosedIsEOF(t *testing.T) {
	t.Parallel()
	if !errors.Is(stream.ErrSendClosed, io.EOF) {
		t.Fatal("errors.Is(ErrSendClosed, io.EOF) want true")
	}
	if errors.Is(io.EOF, stream.ErrSendClosed) {
		// One-way Is is enough; reverse may or may not hold depending on
		// implementation. Document expected Send/HalfClose-only usage.
	}
}

type bytesCodec struct{}

func (bytesCodec) Marshal(v any) ([]byte, error) {
	return append([]byte(nil), v.([]byte)...), nil
}

func (bytesCodec) Unmarshal(b []byte, v any) error {
	dst := v.(*[]byte)
	*dst = append([]byte(nil), b...)
	return nil
}

type sendErr struct {
	msg         string
	receiveOpen bool
}

func (e *sendErr) Error() string     { return e.msg }
func (e *sendErr) ReceiveOpen() bool { return e.receiveOpen }

var _ transport.SendError = (*sendErr)(nil)

type fakeCall struct {
	recvPayload  []byte
	recvErr      error
	sendErr      error
	halfCloseErr error
	sent         bytes.Buffer
	released     bool
	closed       bool
	halfClosed   bool
}

func (c *fakeCall) Method() string              { return "svc.Method" }
func (c *fakeCall) Deadline() (time.Time, bool) { return time.Time{}, false }
func (c *fakeCall) SendHeaders() error          { return nil }
func (c *fakeCall) Finish(error) error          { return nil }

func (c *fakeCall) Recv() ([]byte, func(), error) {
	if c.recvErr != nil {
		return nil, nil, c.recvErr
	}
	payload := append([]byte(nil), c.recvPayload...)
	return payload, func() { c.released = true }, nil
}

func (c *fakeCall) Send(payload []byte) error {
	if c.sendErr != nil {
		return c.sendErr
	}
	c.sent.Write(payload)
	return nil
}

func (c *fakeCall) HalfClose() error {
	c.halfClosed = true
	return c.halfCloseErr
}

func (c *fakeCall) Close() error {
	c.closed = true
	return nil
}

var _ framing.Call = (*fakeCall)(nil)

func TestWrapSendRecv(t *testing.T) {
	t.Parallel()
	call := &fakeCall{recvPayload: []byte("in")}
	st := stream.Wrap(call, bytesCodec{})

	var out []byte
	if err := st.Recv(&out); err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if string(out) != "in" {
		t.Fatalf("Recv = %q, want in", out)
	}
	if !call.released {
		t.Fatal("Recv did not release payload")
	}
	if err := st.Send([]byte("out")); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if got := call.sent.String(); got != "out" {
		t.Fatalf("sent = %q, want out", got)
	}
	if err := st.HalfClose(); err != nil {
		t.Fatalf("HalfClose: %v", err)
	}
	if !call.halfClosed {
		t.Fatal("HalfClose did not reach call")
	}
}

func TestSendMapsReceiveOpenSendErrorToErrSendClosed(t *testing.T) {
	t.Parallel()
	call := &fakeCall{sendErr: &sendErr{msg: "peer reset send", receiveOpen: true}}
	st := stream.Wrap(call, bytesCodec{})
	err := st.Send([]byte("x"))
	if !errors.Is(err, stream.ErrSendClosed) {
		t.Fatalf("Send error = %v, want ErrSendClosed", err)
	}
	if !errors.Is(err, io.EOF) {
		t.Fatalf("ErrSendClosed must satisfy errors.Is(_, io.EOF); got %v", err)
	}
}

func TestHalfCloseMapsReceiveOpenSendErrorToErrSendClosed(t *testing.T) {
	t.Parallel()
	call := &fakeCall{halfCloseErr: &sendErr{msg: "peer reset send", receiveOpen: true}}
	st := stream.Wrap(call, bytesCodec{})
	err := st.HalfClose()
	if !errors.Is(err, stream.ErrSendClosed) {
		t.Fatalf("HalfClose error = %v, want ErrSendClosed", err)
	}
}

func TestSendPassesThroughReceiveClosedSendError(t *testing.T) {
	t.Parallel()
	want := &sendErr{msg: "both directions dead", receiveOpen: false}
	call := &fakeCall{sendErr: want}
	st := stream.Wrap(call, bytesCodec{})
	err := st.Send([]byte("x"))
	if !errors.Is(err, want) {
		t.Fatalf("Send error = %v, want original SendError", err)
	}
	if errors.Is(err, stream.ErrSendClosed) {
		t.Fatal("ReceiveOpen=false must not map to ErrSendClosed")
	}
}

func TestRecvNeverReturnsErrSendClosed(t *testing.T) {
	t.Parallel()
	// Even if Call.Recv somehow returns a ReceiveOpen SendError, Wrap must
	// not rewrite it to ErrSendClosed (that sentinel is Send/HalfClose-only).
	recvSE := &sendErr{msg: "unexpected on recv", receiveOpen: true}
	call := &fakeCall{recvErr: recvSE}
	st := stream.Wrap(call, bytesCodec{})
	var out []byte
	err := st.Recv(&out)
	if !errors.Is(err, recvSE) {
		t.Fatalf("Recv error = %v, want original", err)
	}
	if errors.Is(err, stream.ErrSendClosed) {
		t.Fatal("Recv must never yield ErrSendClosed")
	}
}

func TestRecvPropagatesEOF(t *testing.T) {
	t.Parallel()
	st := stream.Wrap(&fakeCall{recvErr: io.EOF}, bytesCodec{})
	var out []byte
	err := st.Recv(&out)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("Recv error = %v, want io.EOF", err)
	}
	// Bare io.EOF from Recv must not be confused with ErrSendClosed:
	// only ErrSendClosed.Is(io.EOF) is true, not the reverse.
	if errors.Is(err, stream.ErrSendClosed) {
		t.Fatal("Recv io.EOF must not satisfy errors.Is(_, ErrSendClosed)")
	}
}

func TestCloserClose(t *testing.T) {
	t.Parallel()
	call := &fakeCall{}
	st := stream.Wrap(call, bytesCodec{})
	cl, ok := st.(stream.Closer)
	if !ok {
		t.Fatal("Wrap result must implement Closer")
	}
	if err := cl.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !call.closed {
		t.Fatal("Close did not reach call")
	}
}

func TestSendEncodeFailureNeverReachesCall(t *testing.T) {
	t.Parallel()
	call := &fakeCall{}
	st := stream.Wrap(call, failCodec{})
	if err := st.Send([]byte("x")); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("Send error = %v", err)
	}
	if call.sent.Len() != 0 {
		t.Fatalf("call received %d bytes after a failed encode", call.sent.Len())
	}
}

type failCodec struct{}

func (failCodec) Marshal(any) ([]byte, error) { return nil, io.ErrClosedPipe }
func (failCodec) Unmarshal([]byte, any) error { return nil }

func TestWrappedStreamIsACloser(t *testing.T) {
	t.Parallel()
	var _ stream.Closer = stream.Wrap(&fakeCall{}, bytesCodec{}).(stream.Closer)
}

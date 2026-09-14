package stream

import (
	"bytes"
	"errors"
	"io"
	"testing"

	"github.com/argos-io/argos/internal/limits"
)

type bytesCodec struct{}

func (bytesCodec) Marshal(w io.Writer, v any) error {
	_, err := io.Copy(w, bytes.NewReader(v.([]byte)))
	return err
}

func (bytesCodec) Unmarshal(r io.Reader, v any) error {
	buf := v.(*[]byte)
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	*buf = b
	return nil
}

type partialCodec struct{}

func (partialCodec) Marshal(io.Writer, any) error { return nil }
func (partialCodec) Unmarshal(io.Reader, any) error {
	// Deliberately leave the message unread to verify that the stream wrapper
	// enforces the limit independently of codec behavior.
	return nil
}

type memoryFramer struct {
	recv   []byte
	sent   bytes.Buffer
	closed bool
}

func (f *memoryFramer) Recv() (io.Reader, error) {
	return bytes.NewReader(f.recv), nil
}

func (f *memoryFramer) Send() (io.WriteCloser, error) {
	return &bufferWriteCloser{buf: &f.sent}, nil
}

func (f *memoryFramer) CloseSend() error {
	f.closed = true
	return nil
}

func (f *memoryFramer) Close() error {
	f.closed = true
	return nil
}

type bufferWriteCloser struct {
	buf *bytes.Buffer
}

func (b *bufferWriteCloser) Write(p []byte) (int, error) { return b.buf.Write(p) }
func (b *bufferWriteCloser) Close() error                { return nil }

func TestWrapSendRecv(t *testing.T) {
	framer := &memoryFramer{recv: []byte("in")}
	st := Wrap(framer, bytesCodec{})

	var out []byte
	if err := st.Recv(&out); err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if string(out) != "in" {
		t.Fatalf("Recv = %q, want in", out)
	}
	if err := st.Send([]byte("out")); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if got := framer.sent.String(); got != "out" {
		t.Fatalf("sent = %q, want out", got)
	}
	if err := st.CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}
	if !framer.closed {
		t.Fatal("CloseSend did not reach framer")
	}
}

type failCodec struct {
	failOnMarshal bool
}

func (c failCodec) Marshal(w io.Writer, v any) error {
	if c.failOnMarshal {
		return io.ErrClosedPipe
	}
	_, err := io.Copy(w, bytes.NewReader(v.([]byte)))
	return err
}

func (failCodec) Unmarshal(r io.Reader, v any) error {
	buf := v.(*[]byte)
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	*buf = b
	return nil
}

type failRecvFramer struct {
	recvErr error
}

func (f *failRecvFramer) Recv() (io.Reader, error) {
	return nil, f.recvErr
}

func (f *failRecvFramer) Send() (io.WriteCloser, error) {
	return &bufferWriteCloser{buf: &bytes.Buffer{}}, nil
}

func (f *failRecvFramer) CloseSend() error { return nil }
func (f *failRecvFramer) Close() error     { return nil }

type nilResultFramer struct {
	nilReader bool
	nilWriter bool
}

func (f *nilResultFramer) Recv() (io.Reader, error) {
	if f.nilReader {
		return nil, nil
	}
	return bytes.NewReader(nil), nil
}

func (f *nilResultFramer) Send() (io.WriteCloser, error) {
	if f.nilWriter {
		return nil, nil
	}
	return &bufferWriteCloser{buf: &bytes.Buffer{}}, nil
}

func (f *nilResultFramer) CloseSend() error { return nil }
func (f *nilResultFramer) Close() error     { return nil }

func TestSendMarshalFailureClosesWriter(t *testing.T) {
	framer := &memoryFramer{}
	st := Wrap(framer, failCodec{failOnMarshal: true})
	if err := st.Send([]byte("x")); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("Send error = %v", err)
	}
}

func TestRecvPropagatesFramerError(t *testing.T) {
	want := io.EOF
	st := Wrap(&failRecvFramer{recvErr: want}, bytesCodec{})
	var out []byte
	if err := st.Recv(&out); !errors.Is(err, want) {
		t.Fatalf("Recv error = %v", err)
	}
}

func TestWrapRejectsNilFramerResults(t *testing.T) {
	var out []byte
	if err := Wrap(&nilResultFramer{nilReader: true}, bytesCodec{}).Recv(&out); err == nil {
		t.Fatal("Recv accepted a nil reader")
	}
	if err := Wrap(&nilResultFramer{nilWriter: true}, bytesCodec{}).Send([]byte("x")); err == nil {
		t.Fatal("Send accepted a nil writer")
	}
}

func TestWrapRejectsTypedNilDependencies(t *testing.T) {
	var framer *memoryFramer
	var codec *bytesCodec
	var out []byte
	if err := Wrap(framer, bytesCodec{}).Recv(&out); err == nil {
		t.Fatal("Wrap accepted a typed-nil framer")
	}
	if err := Wrap(&memoryFramer{}, codec).Recv(&out); err == nil {
		t.Fatal("Wrap accepted a typed-nil codec")
	}
}

func TestWrapWithLimitBoundsSendAndRecv(t *testing.T) {
	framer := &memoryFramer{recv: []byte("abc")}
	st := WrapWithLimit(framer, bytesCodec{}, 2)

	if err := st.Send([]byte("abc")); !errors.Is(err, limits.ErrTooLarge) {
		t.Fatalf("Send error = %v, want message-size error", err)
	}
	if framer.sent.Len() != 0 {
		t.Fatalf("transport received %d bytes after oversized encode", framer.sent.Len())
	}

	var out []byte
	if err := st.Recv(&out); !errors.Is(err, limits.ErrTooLarge) {
		t.Fatalf("Recv error = %v, want message-size error", err)
	}
}

func TestWrapWithLimitRejectsOversizePartialCodec(t *testing.T) {
	framer := &memoryFramer{recv: []byte("abc")}
	st := WrapWithLimit(framer, partialCodec{}, 2)

	if err := st.Recv(new(struct{})); !errors.Is(err, limits.ErrTooLarge) {
		t.Fatalf("Recv error = %v, want message-size error", err)
	}
}

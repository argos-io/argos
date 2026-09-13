package stream

import (
	"bytes"
	"errors"
	"io"
	"testing"
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

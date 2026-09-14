package protobuf

import (
	"bytes"
	"io"
	"testing"

	"github.com/argos-io/argos/errs"
	"github.com/argos-io/argos/internal/testdata"
	"google.golang.org/protobuf/proto"
)

func TestRoundTrip(t *testing.T) {
	c := New()
	in := &testdata.Num{N: 123, S: "x"}
	var buf bytes.Buffer
	if err := c.Marshal(&buf, in); err != nil {
		t.Fatal(err)
	}
	wire, err := proto.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(buf.Bytes(), wire) {
		t.Fatalf("want binary protobuf %q, got %q", wire, buf.Bytes())
	}
	out := new(testdata.Num)
	if err := c.Unmarshal(&buf, out); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(in, out) {
		t.Fatalf("got %+v want %+v", out, in)
	}
}

func TestRejectNonProto(t *testing.T) {
	c := New()
	err := c.Marshal(new(bytes.Buffer), "not a proto")
	if errs.CodeOf(err) != errs.Internal {
		t.Fatalf("Marshal: want Internal, got %v (%v)", errs.CodeOf(err), err)
	}
	err = c.Unmarshal(bytes.NewReader(nil), 1)
	if errs.CodeOf(err) != errs.Internal {
		t.Fatalf("Unmarshal: want Internal, got %v (%v)", errs.CodeOf(err), err)
	}
}

func TestMarshalRejectsShortWrite(t *testing.T) {
	err := New().Marshal(shortWriter{}, &testdata.Num{N: 1, S: "x"})
	if err != io.ErrShortWrite {
		t.Fatalf("Marshal error = %v, want %v", err, io.ErrShortWrite)
	}
}

type shortWriter struct{}

func (shortWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	return len(p) - 1, nil
}

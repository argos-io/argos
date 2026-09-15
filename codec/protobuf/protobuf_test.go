package protobuf_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/argos-io/argos/codec/protobuf"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestRoundTrip(t *testing.T) {
	t.Parallel()
	c := protobuf.New()
	in := wrapperspb.String("hello")
	b, err := c.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	want, err := proto.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(b, want) {
		t.Fatalf("want binary protobuf %q, got %q", want, b)
	}
	out := new(wrapperspb.StringValue)
	if err := c.Unmarshal(b, out); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(in, out) {
		t.Fatalf("got %+v want %+v", out, in)
	}
}

func TestRejectNonProto(t *testing.T) {
	t.Parallel()
	c := protobuf.New()
	if _, err := c.Marshal("not a proto"); err == nil {
		t.Fatal("Marshal accepted non-proto")
	}
	if err := c.Unmarshal(nil, 1); err == nil {
		t.Fatal("Unmarshal accepted non-proto")
	}
}

func TestUnmarshalBytesFieldDoesNotRetainAlias(t *testing.T) {
	t.Parallel()
	c := protobuf.New()
	in := wrapperspb.Bytes([]byte("secret-payload"))
	b, err := c.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	out := new(wrapperspb.BytesValue)
	if err := c.Unmarshal(b, out); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out.GetValue(), []byte("secret-payload")) {
		t.Fatalf("decoded = %q", out.GetValue())
	}
	for i := range b {
		b[i] = 0
	}
	if !bytes.Equal(out.GetValue(), []byte("secret-payload")) {
		t.Fatalf("bytes field mutated after input reuse: got %q (retained alias of b)", out.GetValue())
	}
}

func TestNamed(t *testing.T) {
	t.Parallel()
	c := protobuf.New()
	n, ok := c.(interface{ CodecName() string })
	if !ok {
		t.Fatal("codec does not implement Named")
	}
	got := n.CodecName()
	if got != "protobuf" {
		t.Fatalf("CodecName = %q", got)
	}
	if !strings.Contains(got, "proto") {
		t.Fatalf("unexpected name %q", got)
	}
}

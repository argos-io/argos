package json

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestMarshalInt64AsJSONString(t *testing.T) {
	t.Parallel()
	c := New()
	b, err := c.Marshal(wrapperspb.Int64(123))
	if err != nil {
		t.Fatal(err)
	}
	got := string(b)
	if !strings.Contains(got, `"123"`) {
		t.Fatalf("int64 must encode as JSON string, got %s", got)
	}
	if strings.Contains(got, `:123`) || strings.Contains(got, `: 123`) {
		t.Fatalf("int64 must not encode as JSON number (encoding/json), got %s", got)
	}
}

func TestRoundTrip(t *testing.T) {
	t.Parallel()
	c := New()
	in := wrapperspb.Int64(1 << 40)
	b, err := c.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	out := new(wrapperspb.Int64Value)
	if err := c.Unmarshal(b, out); err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(in, out) {
		t.Fatalf("got %+v want %+v", out, in)
	}
}

func TestRejectNonProto(t *testing.T) {
	t.Parallel()
	c := New()
	if _, err := c.Marshal("not a proto"); err == nil {
		t.Fatal("Marshal accepted non-proto")
	}
	if err := c.Unmarshal(nil, &struct{}{}); err == nil {
		t.Fatal("Unmarshal accepted non-proto")
	}
}

func TestNamed(t *testing.T) {
	t.Parallel()
	c := New()
	n, ok := c.(interface{ CodecName() string })
	if !ok {
		t.Fatal("codec does not implement Named")
	}
	if got := n.CodecName(); got != "json" {
		t.Fatalf("CodecName = %q", got)
	}
}

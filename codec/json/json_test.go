package json

import (
	"bytes"
	"strings"
	"testing"

	"github.com/argos-io/argos/errs"
	"github.com/argos-io/argos/internal/testdata"
)

func TestMarshalInt64AsJSONString(t *testing.T) {
	var buf bytes.Buffer
	c := New()
	if err := c.Marshal(&buf, &testdata.Num{N: 123, S: "x"}); err != nil {
		t.Fatal(err)
	}
	got := buf.String()
	if !strings.Contains(got, `"123"`) {
		t.Fatalf("int64 must encode as JSON string, got %s", got)
	}
	if strings.Contains(got, `:123`) || strings.Contains(got, `: 123`) {
		t.Fatalf("int64 must not encode as JSON number (encoding/json), got %s", got)
	}
}

func TestRoundTrip(t *testing.T) {
	c := New()
	var buf bytes.Buffer
	in := &testdata.Num{N: 1 << 40, S: "hi"}
	if err := c.Marshal(&buf, in); err != nil {
		t.Fatal(err)
	}
	out := new(testdata.Num)
	if err := c.Unmarshal(&buf, out); err != nil {
		t.Fatal(err)
	}
	if out.GetN() != in.GetN() || out.GetS() != in.GetS() {
		t.Fatalf("got %+v want %+v", out, in)
	}
}

func TestRejectNonProto(t *testing.T) {
	c := New()
	err := c.Marshal(new(bytes.Buffer), "not a proto")
	if errs.CodeOf(err) != errs.Internal {
		t.Fatalf("Marshal: want Internal, got %v (%v)", errs.CodeOf(err), err)
	}
	err = c.Unmarshal(bytes.NewReader(nil), &struct{}{})
	if errs.CodeOf(err) != errs.Internal {
		t.Fatalf("Unmarshal: want Internal, got %v (%v)", errs.CodeOf(err), err)
	}
}

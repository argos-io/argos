package codec

import (
	"bytes"
	"testing"
)

// copyingCodec retains decoded bytes only by copying, satisfying the
// Unmarshal alias contract (b is borrowed only until return).
type copyingCodec struct{}

func (copyingCodec) Marshal(v any) ([]byte, error) {
	return append([]byte(nil), v.([]byte)...), nil
}

func (copyingCodec) Unmarshal(b []byte, v any) error {
	dst := v.(*[]byte)
	*dst = append([]byte(nil), b...)
	return nil
}

func TestUnmarshalDoesNotRetainInputAlias(t *testing.T) {
	t.Parallel()
	c := copyingCodec{}
	b := []byte("secret-payload")
	var out []byte
	if err := c.Unmarshal(b, &out); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if !bytes.Equal(out, []byte("secret-payload")) {
		t.Fatalf("decoded = %q", out)
	}
	for i := range b {
		b[i] = 'x'
	}
	if !bytes.Equal(out, []byte("secret-payload")) {
		t.Fatalf("decoded mutated after input reuse: got %q (codec retained alias of b)", out)
	}
}

// aliasingCodec deliberately retains a sub-slice of b — used to show the
// alias-safety test would fail if a Codec violated the contract.
type aliasingCodec struct{}

func (aliasingCodec) Marshal(v any) ([]byte, error) { return v.([]byte), nil }

func (aliasingCodec) Unmarshal(b []byte, v any) error {
	dst := v.(*[]byte)
	*dst = b // illegal under §2.1
	return nil
}

func TestAliasingCodecWouldFailAliasSafety(t *testing.T) {
	t.Parallel()
	c := aliasingCodec{}
	b := []byte("secret-payload")
	var out []byte
	if err := c.Unmarshal(b, &out); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	for i := range b {
		b[i] = 'x'
	}
	if bytes.Equal(out, []byte("secret-payload")) {
		t.Fatal("expected aliasing codec to observe mutation; test harness broken")
	}
}

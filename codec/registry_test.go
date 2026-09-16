package codec_test

import (
	"testing"

	"github.com/argos-io/argos/codec"
	"github.com/argos-io/argos/codec/json"
	"github.com/argos-io/argos/codec/protobuf"
)

func TestRegisterLookup(t *testing.T) {
	t.Parallel()
	r := codec.NewRegistry()
	if err := protobuf.Register(r); err != nil {
		t.Fatal(err)
	}
	fn, err := r.Lookup(codec.NameProtobuf)
	if err != nil {
		t.Fatal(err)
	}
	c, err := fn()
	if err != nil || c == nil {
		t.Fatalf("factory: c=%v err=%v", c, err)
	}
	if err := json.Register(r); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Lookup(codec.NameJSON); err != nil {
		t.Fatal(err)
	}
}

func TestRegisterRejectsEmptyName(t *testing.T) {
	t.Parallel()
	r := codec.NewRegistry()
	if err := r.Register("", func() (codec.Codec, error) { return nil, nil }); err == nil {
		t.Fatal("expected error")
	}
}

func TestRegisterRejectsNilFactory(t *testing.T) {
	t.Parallel()
	r := codec.NewRegistry()
	if err := r.Register("x", nil); err == nil {
		t.Fatal("expected error")
	}
}

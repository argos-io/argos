package codec_test

import (
	"testing"

	"github.com/argos-io/argos/codec"
	"github.com/argos-io/argos/codec/json"
	"github.com/argos-io/argos/codec/protobuf"
)

func TestBuiltinProtobufRegistered(t *testing.T) {
	t.Parallel()
	_ = protobuf.New
	f := codec.Get("protobuf")
	if f == nil {
		t.Fatal("protobuf not registered")
	}
	if c := f(); c == nil {
		t.Fatal("nil codec")
	}
}

func TestBuiltinJSONRegistered(t *testing.T) {
	t.Parallel()
	_ = json.New
	f := codec.Get("json")
	if f == nil {
		t.Fatal("json not registered")
	}
	if c := f(); c == nil {
		t.Fatal("nil codec")
	}
}

func TestRegisterRejectsEmptyName(t *testing.T) {
	t.Parallel()
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	codec.Register("", func() codec.Codec { return nil })
}

func TestRegisterRejectsNilFactory(t *testing.T) {
	t.Parallel()
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic")
		}
	}()
	codec.Register("x", nil)
}

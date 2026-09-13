package codec_test

import (
	"testing"

	"github.com/argos-io/argos/codec"
	"github.com/argos-io/argos/codec/protobuf"
)

func TestBuiltinProtobufRegistered(t *testing.T) {
	_ = protobuf.New
	if codec.Get("protobuf") == nil {
		t.Fatal("protobuf not registered")
	}
	newCodec := codec.Get("protobuf")
	if newCodec == nil {
		t.Fatal("nil factory")
	}
	if c := newCodec(); c == nil {
		t.Fatal("nil codec")
	}
}

package argos

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/argos-io/argos/codec"
	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/transport"
)

const (
	testStubTransportName = "argos-test-stub-transport"
	testStubSerialCodec   = "argos-test-stub-serial"
)

var stubCodecSerial atomic.Int64

func init() {
	transport.Register(testStubTransportName, func() (transport.Transport, error) {
		return &stubTransport{}, nil
	})
	codec.Register(testStubSerialCodec, func() (codec.Codec, error) {
		return &stubCodec{id: int(stubCodecSerial.Add(1))}, nil
	})
}

func TestAssembleCodecReturnsIndependentInstances(t *testing.T) {
	t.Parallel()
	sc := ServiceOptions{
		Transport: testStubTransportName,
		Codec:     testStubSerialCodec,
	}

	co1, err := sc.AssembleCodec()
	if err != nil {
		t.Fatal(err)
	}
	co2, err := sc.AssembleCodec()
	if err != nil {
		t.Fatal(err)
	}
	sc1 := co1.(*stubCodec)
	sc2 := co2.(*stubCodec)
	if sc1 == sc2 || sc1.id == sc2.id {
		t.Fatalf("Codec reused: %#v vs %#v", sc1, sc2)
	}
}

func TestIncompleteServiceStack(t *testing.T) {
	t.Parallel()
	if _, err := (ServiceOptions{Codec: "protobuf"}).AssembleCodec(); err == nil {
		t.Fatal("expected error without Transport")
	}
}

type stubTransport struct{}

func (s *stubTransport) OpenCall(context.Context, string, descriptor.Method, transport.CallSpec) (transport.Call, error) {
	return nil, nil
}
func (s *stubTransport) Serve(context.Context, func(context.Context, transport.ServerConn), ...transport.ServerOption) error {
	return nil
}
func (s *stubTransport) CallConcurrency() transport.Concurrency { return transport.Sequential }

// The stub deliberately returns the zero codec name: an empty name reads as
// "this axis names no codec on the wire", so the assembly-time codec-name check
// has nothing to compare and a test here never trips over it.
func (s *stubTransport) CodecName() string { return "" }

type stubCodec struct{ id int }

func (s *stubCodec) Marshal(any) ([]byte, error) { return nil, nil }
func (s *stubCodec) Unmarshal([]byte, any) error { return nil }

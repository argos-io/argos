package grpc_test

import (
	"testing"

	"github.com/argos-io/argos"
	grpcbinding "github.com/argos-io/argos/binding/grpc"
)

func TestDefaultRegistryIncludesGRPCAxes(t *testing.T) {
	t.Parallel()
	_ = grpcbinding.New // link package so init registers names
	cfg := argos.DefaultConfig()
	for _, tc := range []struct {
		axis   string
		lookup func() error
	}{
		{"transport", func() error { _, err := cfg.LookupTransport(argos.TransportHTTP2); return err }},
		{"framing", func() error { _, err := cfg.LookupFraming(argos.FramingGRPC); return err }},
		{"codec", func() error { _, err := cfg.LookupCodec(argos.CodecProtobuf); return err }},
	} {
		if err := tc.lookup(); err != nil {
			t.Errorf("%s: %v", tc.axis, err)
		}
	}
	p, err := cfg.ResolveProtocol(argos.Protocol{
		TransportName: argos.TransportHTTP2,
		FramingName:   argos.FramingGRPC,
		CodecName:     argos.CodecProtobuf,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := p.Assemble(); err != nil {
		t.Fatal(err)
	}
}

package argos_test

import (
	"testing"

	"github.com/argos-io/argos"
	"github.com/argos-io/argos/codec"
	codecjson "github.com/argos-io/argos/codec/json"
	codecprotobuf "github.com/argos-io/argos/codec/protobuf"
	"github.com/argos-io/argos/framing"
	framinggrpc "github.com/argos-io/argos/framing/grpc"
	"github.com/argos-io/argos/transport"
	transporthttp2 "github.com/argos-io/argos/transport/http2"
)

func wellKnownRegistries(t *testing.T) (*transport.Registry, *framing.Registry, *codec.Registry) {
	t.Helper()
	tr := transport.NewRegistry()
	if err := transporthttp2.Register(tr); err != nil {
		t.Fatal(err)
	}
	fr := framing.NewRegistry()
	if err := framinggrpc.Register(fr); err != nil {
		t.Fatal(err)
	}
	co := codec.NewRegistry()
	if err := codecprotobuf.Register(co); err != nil {
		t.Fatal(err)
	}
	return tr, fr, co
}

func TestNamedRegistryAtUsageSite(t *testing.T) {
	t.Parallel()
	tr, fr, co := wellKnownRegistries(t)
	cfg, err := argos.ClientConfig(
		argos.WithTransportRegistry(tr),
		argos.WithFramingRegistry(fr),
		argos.WithCodecRegistry(co),
	)
	if err != nil {
		t.Fatal(err)
	}
	axes, err := cfg.ResolveService(argos.ServiceConfig{
		TransportName: transport.NameHTTP2,
		FramingName:   framing.NameGRPC,
		CodecName:     codec.NameProtobuf,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := axes.Assemble(); err != nil {
		t.Fatal(err)
	}
}

func TestRegisterAllDemoAxes(t *testing.T) {
	t.Parallel()
	tr := transport.NewRegistry()
	for _, reg := range []func(*transport.Registry) error{
		transporthttp2.Register,
	} {
		if err := reg(tr); err != nil {
			t.Fatal(err)
		}
	}
	fr := framing.NewRegistry()
	if err := framinggrpc.Register(fr); err != nil {
		t.Fatal(err)
	}
	co := codec.NewRegistry()
	if err := codecjson.Register(co); err != nil {
		t.Fatal(err)
	}
	if _, err := tr.Lookup(transport.NameHTTP2); err != nil {
		t.Fatal(err)
	}
	if _, err := fr.Lookup(framing.NameGRPC); err != nil {
		t.Fatal(err)
	}
	if _, err := co.Lookup(codec.NameJSON); err != nil {
		t.Fatal(err)
	}
}

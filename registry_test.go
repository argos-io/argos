package argos_test

import (
	"testing"

	"github.com/argos-io/argos"
	"github.com/argos-io/argos/codec"
	"github.com/argos-io/argos/framing"
	"github.com/argos-io/argos/transport"
)

func TestNamedAxisRegistryLookup(t *testing.T) {
	t.Parallel()
	cfg := argos.Defaults()
	if err := cfg.RegisterTransport("test-tcp", argos.TransportFunc(func() (transport.Transport, error) {
		return &stubTransport{id: 1}, nil
	})); err != nil {
		t.Fatal(err)
	}
	if _, err := cfg.LookupTransport("test-tcp"); err != nil {
		t.Fatal(err)
	}
	if _, err := (&cfg).ResolveProtocol(argos.Protocol{TransportName: "test-tcp"}); err == nil {
		t.Fatal("ResolveProtocol: want error for missing framing/codec")
	}
}

func TestServiceConfigByName(t *testing.T) {
	t.Parallel()
	var transportCalls int
	base := argos.Defaults()
	if err := base.RegisterTransport("loop", argos.TransportFunc(func() (transport.Transport, error) {
		transportCalls++
		return &stubTransport{id: 1}, nil
	})); err != nil {
		t.Fatal(err)
	}
	if err := base.RegisterFraming("loop", argos.FramingFunc(func() (framing.Framing, error) {
		return &stubFraming{id: 1}, nil
	})); err != nil {
		t.Fatal(err)
	}
	if err := base.RegisterCodec("loop", argos.CodecFunc(func() (codec.Codec, error) {
		return &stubCodec{id: 1}, nil
	})); err != nil {
		t.Fatal(err)
	}

	clientCfg, err := argos.ClientConfig(
		argos.WithConfig(&base),
		argos.WithServiceName("svc"),
		argos.WithService("svc",
			argos.ServiceTransportName("loop"),
			argos.ServiceFramingName("loop"),
			argos.ServiceCodecName("loop"),
			argos.ServiceTarget("ip://127.0.0.1:9"),
		),
	)
	if err != nil {
		t.Fatal(err)
	}
	_, sel := clientCfg.SelectedService()
	p, err := clientCfg.ResolveProtocol(sel.Protocol)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := p.Assemble(); err != nil {
		t.Fatal(err)
	}
	if transportCalls != 1 {
		t.Fatalf("transport factory calls = %d, want 1", transportCalls)
	}
}

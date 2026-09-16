package argos_test

import (
	"testing"

	"github.com/argos-io/argos"
	"github.com/argos-io/argos/codec"
	"github.com/argos-io/argos/framing"
	"github.com/argos-io/argos/transport"
)

func TestNamedAxisRegistryLookup_missingAxes(t *testing.T) {
	t.Parallel()
	tr := transport.NewRegistry()
	if err := tr.Register("test-tcp", func() (transport.Transport, error) {
		return &stubTransport{id: 1}, nil
	}); err != nil {
		t.Fatal(err)
	}
	cfg, err := argos.ClientConfig(
		argos.WithTransportRegistry(tr),
		argos.WithServiceName("svc"),
		argos.WithService("svc",
			argos.ServiceTransportName("test-tcp"),
			argos.ServiceTarget("ip://127.0.0.1:9"),
		),
	)
	if err != nil {
		t.Fatal(err)
	}
	_, sel := cfg.SelectedService()
	if _, err := cfg.ResolveService(sel); err == nil {
		t.Fatal("ResolveService: want error for missing framing/codec")
	}
}

func TestServiceConfigByName(t *testing.T) {
	t.Parallel()
	var transportCalls int
	tr := transport.NewRegistry()
	if err := tr.Register("loop", func() (transport.Transport, error) {
		transportCalls++
		return &stubTransport{id: 1}, nil
	}); err != nil {
		t.Fatal(err)
	}
	fr := framing.NewRegistry()
	if err := fr.Register("loop", func() (framing.Framing, error) {
		return &stubFraming{id: 1}, nil
	}); err != nil {
		t.Fatal(err)
	}
	co := codec.NewRegistry()
	if err := co.Register("loop", func() (codec.Codec, error) {
		return &stubCodec{id: 1}, nil
	}); err != nil {
		t.Fatal(err)
	}

	clientCfg, err := argos.ClientConfig(
		argos.WithTransportRegistry(tr),
		argos.WithFramingRegistry(fr),
		argos.WithCodecRegistry(co),
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
	axes, err := clientCfg.ResolveService(sel)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := axes.Assemble(); err != nil {
		t.Fatal(err)
	}
	if transportCalls != 1 {
		t.Fatalf("transport factory calls = %d, want 1", transportCalls)
	}
}

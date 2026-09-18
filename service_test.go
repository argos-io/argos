package argos

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/argos-io/argos/codec"
	"github.com/argos-io/argos/framing"
	"github.com/argos-io/argos/transport"
)

func TestServiceConfigAssembleReturnsIndependentInstances(t *testing.T) {
	t.Parallel()
	var n atomic.Int64
	transportFn := func() (transport.Transport, error) {
		id := int(n.Add(1))
		return &stubTransport{id: id}, nil
	}
	framingFn := func() (framing.Framing, error) {
		id := int(n.Add(1))
		return &stubFraming{id: id}, nil
	}
	codecFn := func() (codec.Codec, error) {
		id := int(n.Add(1))
		return &stubCodec{id: id}, nil
	}

	cfg, err := ClientConfig(
		WithConfig(&Config{}),
		WithServiceName("echo.v1.EchoService"),
		JoinClient(
			WithTransport(transportFn),
			WithFraming(framingFn),
			WithCodec(codecFn),
		),
	)
	if err != nil {
		t.Fatal(err)
	}
	_, sel := cfg.SelectedService()
	if sel.Transport == nil || sel.Framing == nil || sel.Codec == nil {
		t.Fatal("axes not stored")
	}

	tr1, fr1, co1, err := sel.Assemble()
	if err != nil {
		t.Fatal(err)
	}
	tr2, fr2, co2, err := sel.Assemble()
	if err != nil {
		t.Fatal(err)
	}

	st1 := tr1.(*stubTransport)
	st2 := tr2.(*stubTransport)
	if st1 == st2 || st1.id == st2.id {
		t.Fatalf("Transport reused: %#v vs %#v", st1, st2)
	}
	sf1 := fr1.(*stubFraming)
	sf2 := fr2.(*stubFraming)
	if sf1 == sf2 || sf1.id == sf2.id {
		t.Fatalf("Framing reused: %#v vs %#v", sf1, sf2)
	}
	sc1 := co1.(*stubCodec)
	sc2 := co2.(*stubCodec)
	if sc1 == sc2 || sc1.id == sc2.id {
		t.Fatalf("Codec reused: %#v vs %#v", sc1, sc2)
	}
}

func TestServiceConfigAssembleClosesPartialBuild(t *testing.T) {
	t.Parallel()
	tr := &stubTransport{id: 1}
	sc := ServiceConfig{
		Transport: func() (transport.Transport, error) { return tr, nil },
		Framing:   func() (framing.Framing, error) { return &stubFraming{id: 2}, nil },
		Codec:     func() (codec.Codec, error) { return nil, nil },
	}

	if _, _, _, err := sc.Assemble(); err == nil {
		t.Fatal("Assemble accepted a Codec factory that returned no Codec")
	}
	// Assemble returns no component with an error, so a Transport it built and
	// kept would be unreachable and never closed.
	if !tr.closed.Load() {
		t.Error("failed Assemble left the Transport it had built open")
	}
}

type stubTransport struct {
	id     int
	closed atomic.Bool
}

func (s *stubTransport) Serve(context.Context, func(context.Context, transport.Conn), ...transport.ServerOption) error {
	return nil
}
func (s *stubTransport) Dial(context.Context, transport.DialSpec, ...transport.ClientOption) (transport.Conn, error) {
	return nil, nil
}
func (s *stubTransport) Shutdown(context.Context) error { return nil }
func (s *stubTransport) Close() error                   { s.closed.Store(true); return nil }

type stubFraming struct{ id int }

func (s *stubFraming) Reuse() framing.ReuseModel { return framing.OneCallPerConn }
func (s *stubFraming) NewClientSession(context.Context, transport.Conn, framing.SessionSpec) (framing.ClientSession, error) {
	return nil, nil
}
func (s *stubFraming) NewServerSession(context.Context, transport.Conn, framing.SessionSpec) (framing.ServerSession, error) {
	return nil, nil
}

type stubCodec struct{ id int }

func (s *stubCodec) Marshal(any) ([]byte, error) { return nil, nil }
func (s *stubCodec) Unmarshal([]byte, any) error { return nil }

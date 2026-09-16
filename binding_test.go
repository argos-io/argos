package argos_test

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/argos-io/argos"
	"github.com/argos-io/argos/codec"
	"github.com/argos-io/argos/framing"
	"github.com/argos-io/argos/transport"
)

func TestProtocolAssembleReturnsIndependentInstances(t *testing.T) {
	t.Parallel()
	var n atomic.Int64
	p := argos.Protocol{
		Transport: func() (transport.Transport, error) {
			id := int(n.Add(1))
			return &stubTransport{id: id}, nil
		},
		Framing: func() (framing.Framing, error) {
			id := int(n.Add(1))
			return &stubFraming{id: id}, nil
		},
		Codec: func() (codec.Codec, error) {
			id := int(n.Add(1))
			return &stubCodec{id: id}, nil
		},
	}

	cfg, err := argos.ClientConfig(
		argos.WithConfig(&argos.Config{}),
		argos.WithServiceName("echo.v1.EchoService"),
		argos.WithProtocol(p),
	)
	if err != nil {
		t.Fatal(err)
	}
	_, sel := cfg.SelectedService()
	if sel.Transport == nil || sel.Framing == nil || sel.Codec == nil {
		t.Fatal("protocol not stored")
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

type stubTransport struct{ id int }

func (s *stubTransport) Serve(context.Context, func(context.Context, transport.Conn), ...transport.ServerOption) error {
	return nil
}
func (s *stubTransport) Dial(context.Context, transport.DialSpec, ...transport.ClientOption) (transport.Conn, error) {
	return nil, nil
}
func (s *stubTransport) Shutdown(context.Context) error { return nil }
func (s *stubTransport) Close() error                   { return nil }

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

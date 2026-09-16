package argos_test

import (
	"context"
	"testing"

	"github.com/argos-io/argos"
	"github.com/argos-io/argos/framing"
	"github.com/argos-io/argos/transport"
)

func TestBindingFuncReturnsIndependentInstances(t *testing.T) {
	t.Parallel()
	var n int
	fn := argos.BindingFunc(func() (argos.Binding, error) {
		n++
		return argos.Binding{
			Transport: &stubTransport{id: n},
			Framing:   &stubFraming{id: n},
			Codec:     &stubCodec{id: n},
		}, nil
	})

	cfg, err := argos.ClientConfig(argos.WithConfig(&argos.Config{}), argos.WithBinding(fn))
	if err != nil {
		t.Fatal(err)
	}
	// WithBinding is a call-site override, not the Config.Binding fallback, so
	// SelectedService is the only way to read back what the Client will use.
	_, sel := cfg.SelectedService()
	if sel.Binding == nil {
		t.Fatal("Binding not stored")
	}

	b1, err := sel.Binding()
	if err != nil {
		t.Fatal(err)
	}
	b2, err := sel.Binding()
	if err != nil {
		t.Fatal(err)
	}

	tr1 := b1.Transport.(*stubTransport)
	tr2 := b2.Transport.(*stubTransport)
	if tr1 == tr2 || tr1.id == tr2.id {
		t.Fatalf("Transport reused: %#v vs %#v", tr1, tr2)
	}
	fr1 := b1.Framing.(*stubFraming)
	fr2 := b2.Framing.(*stubFraming)
	if fr1 == fr2 || fr1.id == fr2.id {
		t.Fatalf("Framing reused: %#v vs %#v", fr1, fr2)
	}
	co1 := b1.Codec.(*stubCodec)
	co2 := b2.Codec.(*stubCodec)
	if co1 == co2 || co1.id == co2.id {
		t.Fatalf("Codec reused: %#v vs %#v", co1, co2)
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

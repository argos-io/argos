package echov1

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/argos-io/argos"
	"github.com/argos-io/argos/framing"
	"github.com/argos-io/argos/server"
	"github.com/argos-io/argos/transport"

	_ "github.com/argos-io/argos/resolver/ip"
)

const testListenAddr = "127.0.0.1:0"

type hasAddr interface {
	Addr() net.Addr
}

// baseConfig returns the Config a test's server and client share. A Config is
// a plain struct, so client-only (MaxIdleSessions) and server-only
// (MaxInboundConnAge) tunables sit in one literal and every field left zero
// keeps its built-in default. It is built per test rather than written into
// argos.DefaultConfig(), which would leak tuning into every other test here.
func baseConfig() *argos.Config {
	return &argos.Config{
		MaxConcurrentCalls:     16,
		MaxBufferedBytes:       16 * 16 * 1024 * 1024,
		MaxIdleSessions:        8,
		MaxSessionsPerEndpoint: 8,
		MaxInboundConnIdle:     30 * time.Second,
		MaxInboundConnAge:      30 * time.Minute,
	}
}

func waitAddr(t *testing.T, a hasAddr) string {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if addr := a.Addr(); addr != nil {
			return addr.String()
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("listener Addr not ready")
	return ""
}

// serverProtocol wraps a preset so the server endpoint can capture the listen
// Transport's Addr after Assemble.
func serverProtocol(t *testing.T, preset argos.Protocol, addrTr *hasAddr, bound chan struct{}) argos.Protocol {
	t.Helper()
	var pair atomic.Pointer[struct {
		tr transport.Transport
		fr framing.Framing
	}]
	build := func() {
		tr, err := preset.Transport()
		if err != nil {
			t.Fatal(err)
		}
		a, ok := tr.(hasAddr)
		if !ok {
			t.Fatal("example/echo: Transport does not implement Addr()")
		}
		*addrTr = a
		select {
		case <-bound:
		default:
			close(bound)
		}
		fr, err := preset.Framing()
		if err != nil {
			t.Fatal(err)
		}
		pair.Store(&struct {
			tr transport.Transport
			fr framing.Framing
		}{tr: tr, fr: fr})
	}
	return argos.Protocol{
		Transport: func() (transport.Transport, error) {
			if p := pair.Load(); p != nil {
				return p.tr, nil
			}
			build()
			return pair.Load().tr, nil
		},
		Framing: func() (framing.Framing, error) {
			if p := pair.Load(); p != nil {
				return p.fr, nil
			}
			build()
			return pair.Load().fr, nil
		},
		Codec: preset.Codec,
	}
}

// startEchoServer starts a server with preset and returns the options that reach
// it. Both ends share one Config: WithConfig names the same base for the
// server and for the client, so a test that has to tune a limit — the datagram
// budget, a filter chain — writes it once and both ends of the call agree.
// A nil tune is ignored, which lets a table case leave the field out.
//
// The returned options carry no service name on purpose: the generated stub
// supplies its own.
func startEchoServer(t *testing.T, preset argos.Protocol, tune ...func(*argos.Config)) []argos.ClientOption {
	t.Helper()

	cfg := baseConfig()
	for _, f := range tune {
		if f != nil {
			f(cfg)
		}
	}

	var addrTr hasAddr
	bound := make(chan struct{})
	ep := serverProtocol(t, preset, &addrTr, bound)
	srv := server.New(
		argos.WithConfig(cfg),
		argos.WithListenAddress(testListenAddr),
		argos.WithService("echo.v1.EchoService", argos.ServiceProtocol(ep)),
	)
	if err := RegisterEchoService(srv, NewEchoImpl()); err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Run(context.Background()) }()
	select {
	case <-bound:
	case <-time.After(3 * time.Second):
		t.Fatal("server protocol not assembled")
	}
	if addrTr == nil {
		t.Fatal("listener Addr not captured")
	}
	addr := waitAddr(t, addrTr)
	t.Cleanup(func() { _ = srv.Close() })

	return []argos.ClientOption{
		argos.WithConfig(cfg),
		argos.WithProtocol(preset),
		argos.WithTarget("ip://" + addr),
	}
}

// startEcho starts a server with preset and returns a stub client dialing its
// Addr. The stub owns the Client it built, so closing the stub is what
// releases the session pool.
func startEcho(t *testing.T, preset argos.Protocol, tune ...func(*argos.Config)) EchoServiceClient {
	t.Helper()

	ec, err := NewEchoServiceClient(startEchoServer(t, preset, tune...)...)
	if err != nil {
		t.Fatalf("NewEchoServiceClient: %v", err)
	}
	t.Cleanup(func() { _ = ec.Close() })
	return ec
}

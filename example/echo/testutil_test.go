package echov1

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/argos-io/argos"
	"github.com/argos-io/argos/server"

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

// startEchoServer starts a server with fn and returns the options that reach
// it. Both ends share one Config: WithConfig names the same base for the
// server and for the client, so a test that has to tune a limit — the datagram
// budget, a filter chain — writes it once and both ends of the call agree.
// A nil tune is ignored, which lets a table case leave the field out.
//
// The returned options carry no service name on purpose: the generated stub
// supplies its own.
func startEchoServer(t *testing.T, fn argos.BindingFunc, tune ...func(*argos.Config)) []argos.ClientOption {
	t.Helper()

	cfg := baseConfig()
	for _, f := range tune {
		if f != nil {
			f(cfg)
		}
	}

	var addrTr hasAddr
	bound := make(chan struct{})
	// server.New reports no error; a Config it rejects surfaces on the
	// AddBinding below.
	srv := server.New(argos.WithConfig(cfg), argos.WithListenAddress(testListenAddr))
	if err := srv.AddBinding(func() (argos.Binding, error) {
		b, err := fn()
		if err != nil {
			return b, err
		}
		tr, ok := b.Transport.(hasAddr)
		if !ok {
			return argos.Binding{}, errors.New("example/echo: Transport does not implement Addr()")
		}
		addrTr = tr
		select {
		case <-bound:
		default:
			close(bound)
		}
		return b, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := RegisterEchoService(srv, NewEchoImpl()); err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Run(context.Background()) }()
	select {
	case <-bound:
	case <-time.After(3 * time.Second):
		t.Fatal("server BindingFunc not invoked")
	}
	addr := waitAddr(t, addrTr)
	t.Cleanup(func() { _ = srv.Close() })

	return []argos.ClientOption{
		argos.WithConfig(cfg),
		argos.WithBinding(fn),
		argos.WithTarget("ip://" + addr),
	}
}

// startEcho starts a server with fn and returns a stub client dialing its
// Addr. The stub owns the Client it built, so closing the stub is what
// releases the session pool.
func startEcho(t *testing.T, fn argos.BindingFunc, tune ...func(*argos.Config)) EchoServiceClient {
	t.Helper()

	ec, err := NewEchoServiceClient(startEchoServer(t, fn, tune...)...)
	if err != nil {
		t.Fatalf("NewEchoServiceClient: %v", err)
	}
	t.Cleanup(func() { _ = ec.Close() })
	return ec
}

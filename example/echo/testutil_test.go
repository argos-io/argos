package echov1

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/argos-io/argos"
	"github.com/argos-io/argos/client"
	"github.com/argos-io/argos/server"

	_ "github.com/argos-io/argos/resolver/ip"
)

const (
	echoService    = "echo.v1.EchoService"
	testListenAddr = "127.0.0.1:0"
)

type hasAddr interface {
	Addr() net.Addr
}

func baseOpts(extra ...argos.Option) []argos.Option {
	opts := []argos.Option{
		argos.WithMaxConcurrentCalls(16),
		argos.WithMaxBufferedBytes(16 * 16 * 1024 * 1024),
		argos.WithMaxIdleSessions(8),
		argos.WithMaxSessionsPerEndpoint(8),
		argos.WithMaxInboundConnIdle(30 * time.Second),
		argos.WithMaxInboundConnAge(30 * time.Minute),
		argos.WithListenAddress(testListenAddr),
	}
	return append(opts, extra...)
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

// startEcho starts a server with fn and returns a client dialing its Addr.
func startEcho(t *testing.T, fn argos.BindingFunc, extraOpts ...argos.Option) *client.Client {
	t.Helper()

	cfgOpts := baseOpts(extraOpts...)
	cfg, err := argos.New(cfgOpts...)
	if err != nil {
		t.Fatalf("argos.New: %v", err)
	}

	var addrTr hasAddr
	bound := make(chan struct{})
	srv := server.New(cfg)
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

	cliCfg, err := argos.New(append(cfgOpts,
		argos.WithService(echoService,
			argos.ServiceBinding(fn),
			argos.ServiceTarget("ip://"+addr),
		),
	)...)
	if err != nil {
		t.Fatalf("client argos.New: %v", err)
	}
	cli, err := client.New(cliCfg, echoService)
	if err != nil {
		t.Fatalf("client.New: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })
	return cli
}

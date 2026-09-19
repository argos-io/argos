package echov1

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/argos-io/argos"
	"github.com/argos-io/argos/internal/teststack"
	"github.com/argos-io/argos/server"
	"github.com/argos-io/argos/transport"

	_ "github.com/argos-io/argos/codec/json"
	_ "github.com/argos-io/argos/codec/protobuf"
	_ "github.com/argos-io/argos/resolver/ip"
	_ "github.com/argos-io/argos/transport/grpc"
	_ "github.com/argos-io/argos/transport/httpunary"
)

const testListenAddr = "127.0.0.1:0"

type hasAddr interface {
	Addr() net.Addr
}

// baseOptions returns the Options a test's server and client share. Options is
// a plain struct, so server-only tunables (MaxInboundConnAge) sit in the same
// literal as the shared ones and every field left zero keeps its built-in
// default. It is built per test rather than written into argos.DefaultOptions(),
// which would leak tuning into every other test here.
//
// Session and pool limits are not here: an axis fixes them at construction and
// nothing else holds a second copy, so the transports helpers_test builds carry
// the built-in pool defaults and these Options have nothing to agree with.
func baseOptions() *argos.Options {
	return &argos.Options{
		MaxConcurrentCalls: 16,
		HandshakeTimeout:   10 * time.Second,
		MaxInboundConnIdle: 30 * time.Second,
		MaxInboundConnAge:  30 * time.Minute,
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

// startEchoServer starts a server bound to link and returns client options
// that share the same transport.Transport instance.
func startEchoServer(
	t *testing.T,
	link transport.Transport,
	codecName string,
	tune ...func(*argos.Options),
) []argos.ClientOption {
	t.Helper()

	cfg := baseOptions()
	for _, f := range tune {
		if f != nil {
			f(cfg)
		}
	}

	transportName := teststack.TransportName(t, link)

	cfg.Services = map[string]argos.ServiceOptions{
		"echo.v1.EchoService": {
			Transport:     transportName,
			Codec:         codecName,
			ListenAddress: testListenAddr,
		},
	}
	srv := server.New(
		argos.WithServerOptions(cfg),
		argos.WithListenAddress(testListenAddr),
	)
	if err := srv.Register(EchoServiceDesc, EchoServiceHandlers(NewEchoImpl())); err != nil {
		t.Fatal(err)
	}
	// The ctx given to Run is the server's only stop signal — there is no
	// Server.Close, so canceling it is how the test stops the server.
	runCtx, stopServer := context.WithCancel(context.Background())
	go func() { _ = srv.Run(runCtx) }()

	a, ok := link.(hasAddr)
	if !ok {
		t.Fatal("example/echo: link does not implement Addr()")
	}
	addr := waitAddr(t, a)
	t.Cleanup(stopServer)

	return []argos.ClientOption{
		argos.WithClientOptions(cfg),
		argos.WithTransport(transportName),
		argos.WithCodec(codecName),
		argos.WithTarget("ip://" + addr),
	}
}

func startEcho(t *testing.T, link transport.Transport, codecName string, tune ...func(*argos.Options)) EchoServiceClient {
	t.Helper()
	ec, err := NewEchoServiceClient(startEchoServer(t, link, codecName, tune...)...)
	if err != nil {
		t.Fatalf("NewEchoServiceClient: %v", err)
	}
	return ec
}

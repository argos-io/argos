package grpc

import (
	"context"
	"crypto/tls"
	"fmt"
	"testing"
	"time"

	"github.com/argos-io/argos"
	"github.com/argos-io/argos/client"
	"github.com/argos-io/argos/codec"
	_ "github.com/argos-io/argos/codec/protobuf"
	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/filter"
	"github.com/argos-io/argos/internal/teststack"
	"github.com/argos-io/argos/server"
	argoshttp2 "github.com/argos-io/argos/transport/http2"
)

// transportFrom builds the grpc axis a test drives. Session and pool limits are
// fixed when the axis is constructed and no Options holds a second copy, so the
// axis is built bare: there is nothing left for it to agree with, and a test
// that wants particular pool numbers says so through the axis's own options.
func transportFrom(opts ...composeOpt) (*Transport, *Transport, string, error) {
	o := applyComposeOpts(opts)
	var http2Opts []argoshttp2.Option
	if o.serverTLS != nil {
		http2Opts = append(http2Opts, argoshttp2.WithServerTLS(o.serverTLS))
	}
	cliTLS := o.clientTLS
	if cliTLS == nil && o.authority != "" {
		cliTLS = &tls.Config{ServerName: o.authority}
	} else if cliTLS != nil && o.authority != "" && cliTLS.ServerName == "" {
		c := cliTLS.Clone()
		c.ServerName = o.authority
		cliTLS = c
	}
	if cliTLS != nil {
		http2Opts = append(http2Opts, argoshttp2.WithClientTLS(cliTLS))
	}
	trOpts := []TransportOption{
		WithHTTP2Pipe(argoshttp2.New(http2Opts...)),
	}
	if len(o.compressors) > 0 {
		trOpts = append(trOpts, WithCompressors(o.compressors...))
	}
	if o.sendName != "" {
		trOpts = append(trOpts, WithSendCompressor(o.sendName))
	}
	ax, err := NewTransport(trOpts...)
	if err != nil {
		return nil, nil, "", err
	}
	codecName := "protobuf"
	if o.codec != nil {
		codecName = fmt.Sprintf("grpc-compose-codec-%p", o.codec)
		codec.Register(codecName, func() (codec.Codec, error) { return o.codec, nil })
	}
	return ax, ax, codecName, nil
}

func waitTransportAddr(t *testing.T, tr *Transport) string {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if a := tr.Addr(); a != nil {
			return a.String()
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("grpc transport listener Addr not ready")
	return ""
}

type echoHarness struct {
	srv    *server.Server
	cli    *client.Client
	addr   string
	method descriptor.Method
	srvAx  *Transport
}

func startEchoHarness(t *testing.T, srvOpts, cliOpts []composeOpt) *echoHarness {
	t.Helper()

	cfg := &argos.Options{
		MaxConcurrentCalls: 16,
		MaxInboundConnIdle: 30 * time.Second,
		MaxInboundConnAge:  30 * time.Minute,
		ListenAddress:      "127.0.0.1:0",
	}

	srvAx, cliAx, codecName, err := transportFrom(srvOpts...)
	if err != nil {
		t.Fatal(err)
	}
	if len(cliOpts) > 0 {
		cliAx, _, codecName, err = transportFrom(cliOpts...)
		if err != nil {
			t.Fatal(err)
		}
	}
	srvTr := teststack.TransportName(t, srvAx)
	cliTr := teststack.TransportName(t, cliAx)

	srv := server.New(
		argos.WithServerOptions(cfg),
		argos.WithServerService(echoService,
			argos.ServiceTransport(srvTr),
			argos.ServiceCodec(codecName),
			argos.ServiceListenAddress(cfg.ListenAddress),
		),
	)
	if err := srv.Register(echoDesc(), map[string]filter.Handler{"Echo": echoHandler}); err != nil {
		t.Fatal(err)
	}
	// A Server stops when Run's ctx is canceled — there is no Server.Close — so
	// the test stops it by canceling that ctx and waiting for Run to return.
	// Release the axes here too: they belong to whoever constructed them, and
	// the server no longer closes them.
	runCtx, stopServer := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() {
		defer close(runDone)
		_ = srv.Run(runCtx)
	}()
	addr := waitTransportAddr(t, srvAx)
	t.Cleanup(func() {
		stopServer()
		<-runDone
		_ = srvAx.Close()
		_ = cliAx.Close()
	})

	cli, err := client.New(
		argos.WithClientOptions(cfg),
		argos.WithServiceName(echoService),
		argos.WithTransport(cliTr),
		argos.WithCodec(codecName),
		argos.WithTarget("ip://"+addr),
	)
	if err != nil {
		t.Fatalf("client.New: %v", err)
	}

	return &echoHarness{
		srv:    srv,
		cli:    cli,
		addr:   addr,
		method: descriptor.MustMethod(echoMethod, descriptor.Unary),
		srvAx:  srvAx,
	}
}

func newInteropClient(t *testing.T, addr string, bindOpts []composeOpt, extra ...argos.ClientOption) *client.Client {
	t.Helper()
	cfg := &argos.Options{
		MaxConcurrentCalls: 64,
	}
	ax, _, codecName, err := transportFrom(bindOpts...)
	if err != nil {
		t.Fatal(err)
	}
	trName := teststack.TransportName(t, ax)
	t.Cleanup(func() { _ = ax.Close() })
	cli, err := client.New(append([]argos.ClientOption{
		argos.WithClientOptions(cfg),
		argos.WithServiceName(interopService),
		argos.WithTransport(trName),
		argos.WithCodec(codecName),
		argos.WithTarget("ip://" + addr),
	}, extra...)...)
	if err != nil {
		t.Fatalf("client.New: %v", err)
	}
	return cli
}

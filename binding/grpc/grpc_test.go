package grpc_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"io"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/argos-io/argos"
	grpcbinding "github.com/argos-io/argos/binding/grpc"
	"github.com/argos-io/argos/client"
	"github.com/argos-io/argos/codec/protobuf"
	"github.com/argos-io/argos/compressor/gzip"
	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/filter"
	"github.com/argos-io/argos/server"
	"github.com/argos-io/argos/stream"
	argoshttp2 "github.com/argos-io/argos/transport/http2"
	"google.golang.org/protobuf/types/known/wrapperspb"

	_ "github.com/argos-io/argos/resolver/ip"
)

const (
	echoService = "echo.v1.Echo"
	echoMethod  = echoService + ".Echo"
)

func echoHandler(_ context.Context, _ descriptor.Method, st stream.Stream) error {
	var req wrapperspb.StringValue
	if err := st.Recv(&req); err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	for {
		var discard wrapperspb.StringValue
		if err := st.Recv(&discard); err != nil {
			break
		}
	}
	return st.Send(&wrapperspb.StringValue{Value: "echo:" + req.GetValue()})
}

func echoDesc() descriptor.Service {
	return descriptor.MustService(echoService,
		descriptor.MustMethod(echoMethod, descriptor.Unary))
}

type harness struct {
	srv    *server.Server
	cli    *client.Client
	addr   string
	method descriptor.Method
	srvTr  *argoshttp2.Transport
}

func waitAddr(t *testing.T, tr *argoshttp2.Transport) string {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if a := tr.Addr(); a != nil {
			return a.String()
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("http2 listener Addr not ready")
	return ""
}

func startEcho(t *testing.T, srvOpts, cliOpts []grpcbinding.Option, extra ...argos.Option) *harness {
	t.Helper()

	cfgOpts := append([]argos.Option{
		argos.WithMaxConcurrentCalls(16),
		argos.WithMaxBufferedBytes(16 * 16 * 1024 * 1024),
		argos.WithMaxIdleSessions(8),
		argos.WithMaxSessionsPerEndpoint(8),
		argos.WithMaxInboundConnIdle(30 * time.Second),
		argos.WithMaxInboundConnAge(30 * time.Minute),
		argos.WithListenAddress("127.0.0.1:0"),
	}, extra...)

	cfg, err := argos.New(cfgOpts...)
	if err != nil {
		t.Fatalf("argos.New: %v", err)
	}

	var srvTr *argoshttp2.Transport
	bound := make(chan struct{})
	srvFn := grpcbinding.New(srvOpts...)
	srv := server.New(cfg)
	if err := srv.AddBinding(func() (argos.Binding, error) {
		b, err := srvFn()
		if err != nil {
			return b, err
		}
		tr, ok := b.Transport.(*argoshttp2.Transport)
		if !ok {
			return argos.Binding{}, errors.New("binding/grpc test: Transport is not *http2.Transport")
		}
		srvTr = tr
		select {
		case <-bound:
		default:
			close(bound)
		}
		return b, nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := srv.Register(echoDesc(), map[string]filter.Handler{"Echo": echoHandler}); err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Run(context.Background()) }()
	select {
	case <-bound:
	case <-time.After(3 * time.Second):
		t.Fatal("server BindingFunc not invoked")
	}
	addr := waitAddr(t, srvTr)
	t.Cleanup(func() { _ = srv.Close() })

	cliCfg, err := argos.New(append(cfgOpts,
		argos.WithService(echoService,
			argos.ServiceBinding(grpcbinding.New(cliOpts...)),
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

	return &harness{
		srv:    srv,
		cli:    cli,
		addr:   addr,
		method: descriptor.MustMethod(echoMethod, descriptor.Unary),
		srvTr:  srvTr,
	}
}

func unaryEcho(t *testing.T, cli *client.Client, m descriptor.Method, payload string) string {
	t.Helper()
	cs, err := cli.Open(context.Background(), m)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer cs.Close()
	if err := cs.Send(&wrapperspb.StringValue{Value: payload}); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if err := cs.HalfClose(); err != nil {
		t.Fatalf("HalfClose: %v", err)
	}
	var got wrapperspb.StringValue
	if err := cs.Recv(&got); err != nil {
		t.Fatalf("Recv: %v", err)
	}
	for {
		var discard wrapperspb.StringValue
		if err := cs.Recv(&discard); err != nil {
			break
		}
	}
	return got.GetValue()
}

func selfSigned(t *testing.T) (srv, cli *tls.Config) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "argos-binding-grpc"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse certificate: %v", err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(leaf)

	srv = &tls.Config{
		Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf}},
		NextProtos:   []string{"h2"},
		MinVersion:   tls.VersionTLS12,
	}
	cli = &tls.Config{
		RootCAs:    pool,
		NextProtos: []string{"h2"},
		MinVersion: tls.VersionTLS12,
		ServerName: "127.0.0.1",
	}
	return srv, cli
}

func TestH2CUnaryEcho(t *testing.T) {
	h := startEcho(t, nil, nil)
	got := unaryEcho(t, h.cli, h.method, "h2c")
	if got != "echo:h2c" {
		t.Fatalf("got %q, want echo:h2c", got)
	}
}

func TestTLSALPNUnaryEcho(t *testing.T) {
	srvTLS, cliTLS := selfSigned(t)
	h := startEcho(t,
		[]grpcbinding.Option{grpcbinding.WithServerTLS(srvTLS)},
		[]grpcbinding.Option{
			grpcbinding.WithClientTLS(cliTLS),
			grpcbinding.WithAuthority("127.0.0.1"),
		},
	)
	got := unaryEcho(t, h.cli, h.method, "tls")
	if got != "echo:tls" {
		t.Fatalf("got %q, want echo:tls", got)
	}
}

func TestCompressorGzipSmoke(t *testing.T) {
	compOpts := []grpcbinding.Option{
		grpcbinding.WithCompressor(gzip.New()),
		grpcbinding.WithSendCompressor(gzip.Name),
	}
	h := startEcho(t, compOpts, compOpts)
	got := unaryEcho(t, h.cli, h.method, "gzip-path")
	if got != "echo:gzip-path" {
		t.Fatalf("got %q, want echo:gzip-path", got)
	}
}

func TestBindingFuncIndependentInstances(t *testing.T) {
	fn := grpcbinding.New(grpcbinding.WithCodec(protobuf.New()))
	b1, err := fn()
	if err != nil {
		t.Fatal(err)
	}
	b2, err := fn()
	if err != nil {
		t.Fatal(err)
	}
	if b1.Transport == b2.Transport {
		t.Fatal("Transport instances shared across BindingFunc calls")
	}
	if b1.Framing == b2.Framing {
		t.Fatal("Framing instances shared across BindingFunc calls")
	}
	_ = b1.Transport.Close()
	_ = b2.Transport.Close()
}

func TestSendCompressorRequiresInjection(t *testing.T) {
	fn := grpcbinding.New(grpcbinding.WithSendCompressor(gzip.Name))
	_, err := fn()
	if err == nil {
		t.Fatal("expected error when send compressor is not configured")
	}
}

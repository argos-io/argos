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
	"github.com/argos-io/argos/transport"
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

func grpcServerProtocol(preset argos.Protocol, srvTr **argoshttp2.Transport, bound chan struct{}) argos.Protocol {
	var pair struct {
		tr *argoshttp2.Transport
	}
	build := func() error {
		tr, err := preset.Transport()
		if err != nil {
			return err
		}
		h2, ok := tr.(*argoshttp2.Transport)
		if !ok {
			return errors.New("binding/grpc test: Transport is not *http2.Transport")
		}
		pair.tr = h2
		*srvTr = h2
		select {
		case <-bound:
		default:
			close(bound)
		}
		return nil
	}
	return argos.Protocol{
		Transport: func() (transport.Transport, error) {
			if pair.tr != nil {
				return pair.tr, nil
			}
			if err := build(); err != nil {
				return nil, err
			}
			return pair.tr, nil
		},
		Framing: preset.Framing,
		Codec:   preset.Codec,
	}
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

func startEcho(t *testing.T, srvOpts, cliOpts []grpcbinding.Option) *harness {
	t.Helper()

	// Shared by both halves: the session limits are client-only and the
	// inbound-connection limits and ListenAddress server-only, so the tuning
	// travels as a Config instead of as one option list for both constructors.
	cfg := &argos.Config{
		MaxConcurrentCalls:     16,
		MaxBufferedBytes:       16 * 16 * 1024 * 1024,
		MaxIdleSessions:        8,
		MaxSessionsPerEndpoint: 8,
		MaxInboundConnIdle:     30 * time.Second,
		MaxInboundConnAge:      30 * time.Minute,
		ListenAddress:          "127.0.0.1:0",
	}

	var srvTr *argoshttp2.Transport
	bound := make(chan struct{})
	preset := grpcbinding.New(srvOpts...)
	srv := server.New(argos.WithConfig(cfg))
	ep := grpcServerProtocol(preset, &srvTr, bound)
	if err := srv.AddEndpoint(argos.EndpointConfig{Protocol: ep}); err != nil {
		t.Fatal(err)
	}
	if err := srv.Register(echoDesc(), map[string]filter.Handler{"Echo": echoHandler}); err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Run(context.Background()) }()
	select {
	case <-bound:
	case <-time.After(3 * time.Second):
		t.Fatal("server protocol not assembled")
	}
	addr := waitAddr(t, srvTr)
	t.Cleanup(func() { _ = srv.Close() })

	cli, err := client.New(
		argos.WithConfig(cfg),
		argos.WithServiceName(echoService),
		argos.WithProtocol(grpcbinding.New(cliOpts...)),
		argos.WithTarget("ip://"+addr),
	)
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

func TestProtocolAssembleIndependentInstances(t *testing.T) {
	p := grpcbinding.New(grpcbinding.WithCodec(protobuf.New()))
	tr1, fr1, _, err := p.Assemble()
	if err != nil {
		t.Fatal(err)
	}
	tr2, fr2, _, err := p.Assemble()
	if err != nil {
		t.Fatal(err)
	}
	if tr1 == tr2 {
		t.Fatal("Transport instances shared across Assemble calls")
	}
	if fr1 == fr2 {
		t.Fatal("Framing instances shared across Assemble calls")
	}
	_ = tr1.Close()
	_ = tr2.Close()
}

func TestSendCompressorRequiresInjection(t *testing.T) {
	p := grpcbinding.New(grpcbinding.WithSendCompressor(gzip.Name))
	_, err := p.Framing()
	if err == nil {
		t.Fatal("expected error when send compressor is not configured")
	}
}

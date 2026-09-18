package grpc

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
	"github.com/argos-io/argos/client"
	"github.com/argos-io/argos/codec"
	"github.com/argos-io/argos/codec/protobuf"
	"github.com/argos-io/argos/compressor"
	"github.com/argos-io/argos/compressor/gzip"
	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/filter"
	"github.com/argos-io/argos/framing"
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

type composeOpt interface {
	apply(*composeOpts)
}

type composeOpts struct {
	serverTLS   *tls.Config
	clientTLS   *tls.Config
	authority   string
	compressors []compressor.Compressor
	sendName    string
	codec       codec.Codec
}

type composeOptionFunc func(*composeOpts)

func (f composeOptionFunc) apply(o *composeOpts) { f(o) }

func WithServerTLS(cfg *tls.Config) composeOpt {
	return composeOptionFunc(func(o *composeOpts) { o.serverTLS = cfg })
}

func WithClientTLS(cfg *tls.Config) composeOpt {
	return composeOptionFunc(func(o *composeOpts) { o.clientTLS = cfg })
}

func WithAuthority(authority string) composeOpt {
	return composeOptionFunc(func(o *composeOpts) { o.authority = authority })
}

func WithCompressor(cs ...compressor.Compressor) composeOpt {
	return composeOptionFunc(func(o *composeOpts) {
		o.compressors = append(o.compressors, cs...)
	})
}

func composeWithSendCompressor(name string) composeOpt {
	return composeOptionFunc(func(o *composeOpts) { o.sendName = name })
}

func WithCodec(c codec.Codec) composeOpt {
	return composeOptionFunc(func(o *composeOpts) { o.codec = c })
}

func applyComposeOpts(opts []composeOpt) composeOpts {
	var o composeOpts
	for _, opt := range opts {
		if opt != nil {
			opt.apply(&o)
		}
	}
	return o
}

func axesFrom(opts ...composeOpt) (argos.TransportFunc, argos.FramingFunc, argos.CodecFunc) {
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
	var frOpts []Option
	if len(o.compressors) > 0 {
		frOpts = append(frOpts, WithCompressors(o.compressors...))
	}
	if o.sendName != "" {
		frOpts = append(frOpts, WithSendCompressor(o.sendName))
	}
	return argos.TransportFunc(func() (transport.Transport, error) {
			return argoshttp2.New(http2Opts...), nil
		}),
		argos.FramingFunc(func() (framing.Framing, error) {
			return New(frOpts...)
		}),
		argos.CodecFunc(func() (codec.Codec, error) {
			cd := o.codec
			if cd == nil {
				cd = protobuf.New()
			}
			return cd, nil
		})
}

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

func grpcServerService(srvOpts []composeOpt, srvTr **argoshttp2.Transport, bound chan struct{}) argos.ServiceOption {
	baseT, baseF, baseC := axesFrom(srvOpts...)
	var pair struct {
		tr *argoshttp2.Transport
	}
	build := func() error {
		tr, err := baseT()
		if err != nil {
			return err
		}
		h2, ok := tr.(*argoshttp2.Transport)
		if !ok {
			return errors.New("framing/grpc test: Transport is not *http2.Transport")
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
	return argos.JoinService(
		argos.ServiceTransport(func() (transport.Transport, error) {
			if pair.tr != nil {
				return pair.tr, nil
			}
			if err := build(); err != nil {
				return nil, err
			}
			return pair.tr, nil
		}),
		argos.ServiceFraming(baseF),
		argos.ServiceCodec(baseC),
	)
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

func startEcho(t *testing.T, srvOpts, cliOpts []composeOpt) *harness {
	t.Helper()

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
	srv := server.New(
		argos.WithConfig(cfg),
		argos.WithService(echoService,
			grpcServerService(srvOpts, &srvTr, bound),
			argos.ServiceListenAddress(cfg.ListenAddress),
		),
	)
	if err := srv.Register(echoDesc(), map[string]filter.Handler{"Echo": echoHandler}); err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Run(context.Background()) }()
	select {
	case <-bound:
	case <-time.After(3 * time.Second):
		t.Fatal("server axes not assembled")
	}
	addr := waitAddr(t, srvTr)
	t.Cleanup(func() { _ = srv.Close() })

	tFn, fFn, cFn := axesFrom(cliOpts...)
	cli, err := client.New(
		argos.WithConfig(cfg),
		argos.WithServiceName(echoService),
		argos.JoinClient(
			argos.WithTransport(tFn),
			argos.WithFraming(fFn),
			argos.WithCodec(cFn),
		),
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
		Subject:               pkix.Name{CommonName: "argos-grpc-interop"},
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
		[]composeOpt{WithServerTLS(srvTLS)},
		[]composeOpt{
			WithClientTLS(cliTLS),
			WithAuthority("127.0.0.1"),
		},
	)
	got := unaryEcho(t, h.cli, h.method, "tls")
	if got != "echo:tls" {
		t.Fatalf("got %q, want echo:tls", got)
	}
}

func TestCompressorGzipSmoke(t *testing.T) {
	compOpts := []composeOpt{
		WithCompressor(gzip.New()),
		composeWithSendCompressor(gzip.Name),
	}
	h := startEcho(t, compOpts, compOpts)
	got := unaryEcho(t, h.cli, h.method, "gzip-path")
	if got != "echo:gzip-path" {
		t.Fatalf("got %q, want echo:gzip-path", got)
	}
}

func TestAxesAssembleIndependentInstances(t *testing.T) {
	var sc argos.ServiceConfig
	sc.Transport, sc.Framing, sc.Codec = axesFrom(WithCodec(protobuf.New()))
	tr1, fr1, _, err := sc.Assemble()
	if err != nil {
		t.Fatal(err)
	}
	tr2, fr2, _, err := sc.Assemble()
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
	_, fFn, _ := axesFrom(composeWithSendCompressor(gzip.Name))
	_, err := fFn()
	if err == nil {
		t.Fatal("expected error when send compressor is not configured")
	}
}

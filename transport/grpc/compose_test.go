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

	"github.com/argos-io/argos/client"
	"github.com/argos-io/argos/codec"
	"github.com/argos-io/argos/compressor"
	"github.com/argos-io/argos/compressor/gzip"
	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/stream"
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
	h := startEchoHarness(t, nil, nil)
	got := unaryEcho(t, h.cli, h.method, "h2c")
	if got != "echo:h2c" {
		t.Fatalf("got %q, want echo:h2c", got)
	}
}

func TestTLSALPNUnaryEcho(t *testing.T) {
	srvTLS, cliTLS := selfSigned(t)
	h := startEchoHarness(t,
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
	h := startEchoHarness(t, compOpts, compOpts)
	got := unaryEcho(t, h.cli, h.method, "gzip-path")
	if got != "echo:gzip-path" {
		t.Fatalf("got %q, want echo:gzip-path", got)
	}
}

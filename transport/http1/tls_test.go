package http1

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"io"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/argos-io/argos/transport"
)

func selfSignedHTTP1(t *testing.T) (srv, cli *tls.Config) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "argos-http1"},
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
		MinVersion:   tls.VersionTLS12,
	}
	cli = &tls.Config{
		RootCAs:    pool,
		MinVersion: tls.VersionTLS12,
		ServerName: "127.0.0.1",
	}
	return srv, cli
}

func startTLSServer(t *testing.T, srvTLS *tls.Config, onConn func(context.Context, transport.Conn)) (*Transport, string) {
	t.Helper()
	raw := New(WithServerTLS(srvTLS))
	tr := raw.(*Transport)
	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	go func() {
		errCh <- tr.Serve(ctx, onConn, transport.WithListenAddress("127.0.0.1:0"))
	}()

	a := waitAddr(t, tr)
	t.Cleanup(func() {
		cancel()
		_ = tr.Close()
		select {
		case <-errCh:
		case <-time.After(2 * time.Second):
			t.Error("Serve did not return after Close")
		}
	})
	return tr, a.String()
}

func dialTLS(t *testing.T, addr string, cliTLS *tls.Config) transport.StreamConn {
	t.Helper()
	clientTr := New(WithClientTLS(cliTLS))
	t.Cleanup(func() { _ = clientTr.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)

	conn, err := clientTr.Dial(ctx, transport.DialSpec{Endpoint: addr})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	sc, ok := conn.(transport.StreamConn)
	if !ok {
		t.Fatal("Dial did not return StreamConn")
	}
	return sc
}

func TestTLSOpenStreamRoundTrip(t *testing.T) {
	srvTLS, cliTLS := selfSignedHTTP1(t)
	_, addr := startTLSServer(t, srvTLS, func(_ context.Context, c transport.Conn) {
		cc := c.(transport.CarrierConn)
		car := cc.Carrier().(interface {
			transport.ByteStreamCarrier
			transport.UnaryResponseWriter
		})
		body, _ := io.ReadAll(car)
		_ = car.WriteResponse(200, nil, body)
	})

	sc := dialTLS(t, addr, cliTLS)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	car, err := sc.OpenStream(ctx, transport.RequestPreface{
		RequestTarget: "/tls-echo",
		Headers:       transport.Headers{{Name: "Content-Type", Value: "application/octet-stream"}},
	})
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	bc := car.(transport.ByteStreamCarrier)
	if _, err := bc.Write([]byte("tls-payload")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := car.(interface{ CloseSend() error }).CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}
	got, err := io.ReadAll(bc)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if string(got) != "tls-payload" {
		t.Fatalf("got %q, want tls-payload", got)
	}
}

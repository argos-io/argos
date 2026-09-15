package probe

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"golang.org/x/net/http2"
)

// selfSigned 生成一张 127.0.0.1 的自签证书，同时返回服务端配置与信任它的
// 客户端配置。两边都声明 ALPN "h2"——grpc-go 不接受 ALPN 协商失败的连接。
func selfSigned(t *testing.T) (srv, cli *tls.Config) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "argos-probe"},
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
	cli = &tls.Config{RootCAs: pool, NextProtos: []string{"h2"}, MinVersion: tls.VersionTLS12}
	return srv, cli
}

// newTLSServer 起一个 TLS + ALPN h2 的 HTTP/2 服务器，并返回只走该 TLS 配置的客户端。
func newTLSServer(t *testing.T, h http.HandlerFunc) *probeServer {
	t.Helper()
	serverTLS, clientTLS := selfSigned(t)

	srv := httptest.NewUnstartedServer(h)
	srv.TLS = serverTLS
	srv.EnableHTTP2 = true
	srv.StartTLS()
	t.Cleanup(srv.Close)

	cl := &http.Client{Transport: &http2.Transport{
		TLSClientConfig: clientTLS,
	}}
	t.Cleanup(cl.CloseIdleConnections)

	return &probeServer{url: srv.URL, client: cl, addr: srv.Listener.Addr()}
}

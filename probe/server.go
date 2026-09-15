package probe

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

type probeServer struct {
	url    string
	client *http.Client
	addr   net.Addr
}

// newH2CServer 起一个明文 HTTP/2（h2c）服务器，并返回一个只走 h2c 的客户端。
func newH2CServer(t *testing.T, h http.HandlerFunc) *probeServer {
	t.Helper()
	h2s := &http2.Server{}
	srv := httptest.NewUnstartedServer(h2c.NewHandler(h, h2s))
	srv.EnableHTTP2 = true
	srv.Start()
	t.Cleanup(srv.Close)

	cl := &http.Client{Transport: &http2.Transport{
		AllowHTTP: true,
		DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, addr)
		},
	}}
	t.Cleanup(cl.CloseIdleConnections)

	return &probeServer{url: srv.URL, client: cl, addr: srv.Listener.Addr()}
}

func grpcHeaders() http.Header {
	return http.Header{
		"Content-Type": {"application/grpc+proto"},
		"Te":           {"trailers"},
	}
}

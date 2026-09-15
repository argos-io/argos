package transport_test

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/argos-io/argos/transport"
	argoshttp1 "github.com/argos-io/argos/transport/http1"
	"github.com/argos-io/argos/transport/http2"
	"github.com/argos-io/argos/transport/tcp"
	"github.com/argos-io/argos/transport/udp"
	"github.com/argos-io/argos/transport/ws"
)

// TestConnCarrierMatrix is Task 6.3 / §9-3 thin combo gate: assert §4.4 Conn
// and Carrier interface shapes for tcp/ws/udp/http1/http2 (client + server).
func TestConnCarrierMatrix(t *testing.T) {
	t.Parallel()

	t.Run("tcp", func(t *testing.T) {
		t.Parallel()
		cli, srv := dialServePair(t, tcp.New(), func(_ context.Context, c transport.Conn) {
			assertCarrierConn(t, "tcp/server", c, true)
			car := c.(transport.CarrierConn).Carrier()
			assertCarrier(t, "tcp/server", car, wantCarrier{
				byteStream: true,
				sendCloser: true,
			})
			_, _ = io.Copy(io.Discard, car.(io.Reader))
			_ = c.Close()
		})
		assertCarrierConn(t, "tcp/client", cli, true)
		assertNotStreamConn(t, "tcp/client", cli)
		assertCarrier(t, "tcp/client", cli.(transport.CarrierConn).Carrier(), wantCarrier{
			byteStream: true,
			sendCloser: true,
		})
		_ = cli.Close()
		_ = srv // kept alive by cleanup
	})

	t.Run("ws", func(t *testing.T) {
		t.Parallel()
		cli, _ := dialServePair(t, ws.New(), func(_ context.Context, c transport.Conn) {
			assertCarrierConn(t, "ws/server", c, true)
			car := c.(transport.CarrierConn).Carrier()
			assertCarrier(t, "ws/server", car, wantCarrier{message: true})
			_, _ = car.(transport.MessageCarrier).RecvMessage()
			_ = c.Close()
		})
		assertCarrierConn(t, "ws/client", cli, true)
		assertNotStreamConn(t, "ws/client", cli)
		assertCarrier(t, "ws/client", cli.(transport.CarrierConn).Carrier(), wantCarrier{message: true})
		_ = cli.Close()
	})

	t.Run("udp", func(t *testing.T) {
		t.Parallel()
		gotSrv := make(chan struct{}, 1)
		cli, _ := dialServePair(t, udp.New(), func(_ context.Context, c transport.Conn) {
			assertCarrierConn(t, "udp/server", c, true)
			car := c.(transport.CarrierConn).Carrier()
			assertCarrier(t, "udp/server", car, wantCarrier{datagram: true})
			select {
			case gotSrv <- struct{}{}:
			default:
			}
			_, _ = car.(transport.DatagramCarrier).RecvDatagram()
			_ = c.Close()
		})
		assertCarrierConn(t, "udp/client", cli, true)
		assertNotStreamConn(t, "udp/client", cli)
		car := cli.(transport.CarrierConn).Carrier()
		assertCarrier(t, "udp/client", car, wantCarrier{datagram: true})
		// UDP onConn fires on first datagram — wake the server side.
		if err := car.(transport.DatagramCarrier).SendDatagram([]byte("ping")); err != nil {
			t.Fatalf("SendDatagram: %v", err)
		}
		select {
		case <-gotSrv:
		case <-time.After(2 * time.Second):
			t.Fatal("udp server Conn not observed")
		}
		_ = cli.Close()
	})

	t.Run("http1", func(t *testing.T) {
		t.Parallel()
		srvReady := make(chan transport.Conn, 1)
		cli, _ := dialServePair(t, argoshttp1.New(), func(_ context.Context, c transport.Conn) {
			assertCarrierConn(t, "http1/server", c, true)
			assertNotStreamConn(t, "http1/server", c)
			car := c.(transport.CarrierConn).Carrier()
			assertCarrier(t, "http1/server", car, wantCarrier{
				byteStream:     true,
				reqHeaders:     true,
				unaryRespWrite: true,
			})
			select {
			case srvReady <- c:
			default:
			}
			_, _ = io.Copy(io.Discard, car.(io.Reader))
			if urw, ok := car.(transport.UnaryResponseWriter); ok {
				_ = urw.WriteResponse(200, nil, []byte("ok"))
			}
			_ = c.Close()
		})
		assertStreamConn(t, "http1/client", cli)
		assertNotCarrierConn(t, "http1/client", cli)
		sc := cli.(transport.StreamConn)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		car, err := sc.OpenStream(ctx, transport.RequestPreface{RequestTarget: "/matrix"})
		if err != nil {
			t.Fatalf("OpenStream: %v", err)
		}
		assertCarrier(t, "http1/client", car, wantCarrier{
			byteStream:  true,
			sendCloser:  true,
			respHeaders: true,
		})
		if scCloser, ok := car.(transport.SendCloser); ok {
			_ = scCloser.CloseSend()
		}
		_, _ = io.Copy(io.Discard, car.(io.Reader))
		_ = cli.Close()
		select {
		case <-srvReady:
		case <-time.After(2 * time.Second):
			t.Fatal("http1 server Conn not observed")
		}
	})

	t.Run("http2", func(t *testing.T) {
		t.Parallel()
		srvReady := make(chan transport.Conn, 1)
		cli, _ := dialServePair(t, http2.New(), func(_ context.Context, c transport.Conn) {
			assertCarrierConn(t, "http2/server", c, true)
			assertNotStreamConn(t, "http2/server", c)
			car := c.(transport.CarrierConn).Carrier()
			assertCarrier(t, "http2/server", car, wantCarrier{
				byteStream: true,
				reqHeaders: true,
				respWriter: true,
			})
			select {
			case srvReady <- c:
			default:
			}
			_, _ = io.Copy(io.Discard, car.(io.Reader))
			if rw, ok := car.(transport.ResponseWriter); ok {
				_ = rw.Finish(200, nil, nil)
			}
			_ = c.Close()
		})
		assertStreamConn(t, "http2/client", cli)
		assertNotCarrierConn(t, "http2/client", cli)
		sc := cli.(transport.StreamConn)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		car, err := sc.OpenStream(ctx, transport.RequestPreface{RequestTarget: "/matrix"})
		if err != nil {
			t.Fatalf("OpenStream: %v", err)
		}
		assertCarrier(t, "http2/client", car, wantCarrier{
			byteStream:   true,
			sendCloser:   true,
			respHeaders:  true,
			respTrailers: true,
		})
		if scCloser, ok := car.(transport.SendCloser); ok {
			_ = scCloser.CloseSend()
		}
		_, _ = io.Copy(io.Discard, car.(io.Reader))
		_ = cli.Close()
		select {
		case <-srvReady:
		case <-time.After(2 * time.Second):
			t.Fatal("http2 server Conn not observed")
		}
	})
}

type wantCarrier struct {
	byteStream     bool
	message        bool
	datagram       bool
	sendCloser     bool
	reqHeaders     bool
	respHeaders    bool
	respTrailers   bool
	respWriter     bool
	unaryRespWrite bool
}

func assertCarrier(t *testing.T, label string, car transport.Carrier, want wantCarrier) {
	t.Helper()
	check := func(name string, got, expect bool) {
		t.Helper()
		if got != expect {
			t.Errorf("%s: %s = %v, want %v", label, name, got, expect)
		}
	}
	_, isBS := car.(transport.ByteStreamCarrier)
	_, isMsg := car.(transport.MessageCarrier)
	_, isDG := car.(transport.DatagramCarrier)
	_, isSC := car.(transport.SendCloser)
	_, isRH := car.(transport.RequestHeaderReader)
	_, isRespH := car.(transport.ResponseHeaderReader)
	_, isRT := car.(transport.ResponseTrailerReader)
	_, isRW := car.(transport.ResponseWriter)
	_, isURW := car.(transport.UnaryResponseWriter)
	check("ByteStreamCarrier", isBS, want.byteStream)
	check("MessageCarrier", isMsg, want.message)
	check("DatagramCarrier", isDG, want.datagram)
	check("SendCloser", isSC, want.sendCloser)
	check("RequestHeaderReader", isRH, want.reqHeaders)
	check("ResponseHeaderReader", isRespH, want.respHeaders)
	check("ResponseTrailerReader", isRT, want.respTrailers)
	check("ResponseWriter", isRW, want.respWriter)
	check("UnaryResponseWriter", isURW, want.unaryRespWrite)
}

func assertCarrierConn(t *testing.T, label string, c transport.Conn, want bool) {
	t.Helper()
	_, ok := c.(transport.CarrierConn)
	if ok != want {
		t.Errorf("%s: CarrierConn = %v, want %v", label, ok, want)
	}
}

func assertNotCarrierConn(t *testing.T, label string, c transport.Conn) {
	t.Helper()
	assertCarrierConn(t, label, c, false)
}

func assertStreamConn(t *testing.T, label string, c transport.Conn) {
	t.Helper()
	if _, ok := c.(transport.StreamConn); !ok {
		t.Errorf("%s: want StreamConn", label)
	}
}

func assertNotStreamConn(t *testing.T, label string, c transport.Conn) {
	t.Helper()
	if _, ok := c.(transport.StreamConn); ok {
		t.Errorf("%s: unexpectedly implements StreamConn", label)
	}
}

func dialServePair(t *testing.T, tr transport.Transport, onConn func(context.Context, transport.Conn)) (clientConn transport.Conn, serverTr transport.Transport) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- tr.Serve(ctx, onConn, transport.WithListenAddress("127.0.0.1:0"))
	}()

	addr := waitTransportAddr(t, tr)
	t.Cleanup(func() {
		cancel()
		_ = tr.Close()
		select {
		case <-errCh:
		case <-time.After(3 * time.Second):
			t.Error("Serve did not return after Close")
		}
	})

	clientTr := newClientTransport(t, tr)
	t.Cleanup(func() { _ = clientTr.Close() })

	dctx, dcancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer dcancel()
	conn, err := clientTr.Dial(dctx, transport.DialSpec{Endpoint: addr})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	return conn, tr
}

func newClientTransport(t *testing.T, server transport.Transport) transport.Transport {
	t.Helper()
	switch server.(type) {
	case *tcp.Transport:
		return tcp.New()
	case *ws.Transport:
		return ws.New()
	case *udp.Transport:
		return udp.New()
	case *argoshttp1.Transport:
		return argoshttp1.New()
	case *http2.Transport:
		return http2.New()
	default:
		t.Fatalf("unknown transport type %T", server)
		return nil
	}
}

func waitTransportAddr(t *testing.T, tr transport.Transport) string {
	t.Helper()
	type addrer interface{ Addr() net.Addr }
	a, ok := tr.(addrer)
	if !ok {
		t.Fatalf("%T does not implement Addr() net.Addr (§9-13)", tr)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if addr := a.Addr(); addr != nil {
			return addr.String()
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("listener Addr not set within timeout")
	return ""
}

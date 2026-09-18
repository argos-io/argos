package resp

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/argos-io/argos/transport"
	"github.com/argos-io/argos/transport/tcp"
)

// These tests are in-package: they drive a Session directly (AcceptCall over a
// carrier the test can kill at a chosen moment), which the exported API cannot
// express — the composition layer owns accept loops and sessions.

type addrConn interface {
	Addr() net.Addr
}

// tcpPair serves a tcp transport on a loopback port and dials it. It returns the
// server-side Conn handed to the accept callback plus the raw peer, so the test
// controls exactly when the peer writes and when it dies.
func tcpPair(t *testing.T) (transport.Conn, net.Conn) {
	t.Helper()
	tr := tcp.New()
	served, ok := tr.(addrConn)
	if !ok {
		t.Fatalf("transport %T has no Addr(); cannot find the listen port", tr)
	}

	inbound := make(chan transport.Conn, 1)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() {
		_ = tr.Serve(ctx, func(_ context.Context, c transport.Conn) { inbound <- c },
			transport.WithListenAddress("127.0.0.1:0"))
	}()

	deadline := time.Now().Add(3 * time.Second)
	var addr net.Addr
	for addr == nil && time.Now().Before(deadline) {
		addr = served.Addr()
		if addr == nil {
			time.Sleep(5 * time.Millisecond)
		}
	}
	if addr == nil {
		t.Fatal("tcp listener never bound")
	}

	peer, err := net.Dial("tcp", addr.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = peer.Close() })

	select {
	case c := <-inbound:
		return c, peer
	case <-time.After(3 * time.Second):
		t.Fatal("transport did not hand over the accepted conn")
		return nil, nil
	}
}

// serverSessionWithPeerHandshake completes the connection-level HELLO handshake
// with peer and returns the server session under test.
func serverSessionWithPeerHandshake(t *testing.T, ax *Transport, conn transport.Conn, peer net.Conn) *serverConn {
	t.Helper()
	if _, err := peer.Write(EncodeArray("HELLO", "2")); err != nil {
		t.Fatalf("peer HELLO: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	sess := &serverConn{axis: ax, conn: conn}
	if err := sess.Handshake(ctx); err != nil {
		t.Fatalf("Handshake: %v", err)
	}
	t.Cleanup(func() { _ = sess.Close() })

	buf := make([]byte, 16)
	n, err := peer.Read(buf)
	if err != nil {
		t.Fatalf("peer HELLO reply: %v", err)
	}
	if !strings.HasPrefix(string(buf[:n]), "+OK") {
		t.Fatalf("peer HELLO reply = %q, want +OK", buf[:n])
	}
	return sess
}

// TestServerAcceptCallReturnsWhenPeerDiesMidFrame: readLoop exits on a carrier
// read error, but nothing terminated the session for waiters. With a partial
// value in readBuf, readValue cannot surface readErr (it only does so when the
// buffer is empty), so it parked on a wake channel that broadcastReadLocked had
// already replaced — and with the read loop gone nothing would ever close that
// one again. AcceptCall then parked for the whole open budget (a minute here),
// held s.busy, and left Reusable() true, so a pool would keep lending out a
// session that can never carry a call.
func TestServerAcceptCallReturnsWhenPeerDiesMidFrame(t *testing.T) {
	srvConn, peer := tcpPair(t)
	fr := New(WithOpenTimeout(time.Minute))
	ss := serverSessionWithPeerHandshake(t, fr, srvConn, peer)

	// A command truncated mid-argument: the array header and the first bulk
	// arrive, the second bulk's declared 4 bytes never do.
	truncated := []byte("*2\r\n$4\r\nPING\r\n$4\r\nab")
	if _, err := peer.Write(truncated); err != nil {
		t.Fatalf("peer write: %v", err)
	}
	if err := peer.Close(); err != nil {
		t.Fatalf("peer close: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := ss.AcceptCall(context.Background(), transport.CallSpec{})
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("AcceptCall accepted a call from a peer that died mid-frame")
		}
		if strings.Contains(err.Error(), "OpenTimeout") {
			t.Fatalf("AcceptCall fell back on the open budget instead of the carrier failure: %v", err)
		}
		t.Logf("AcceptCall returned %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("AcceptCall still parked 2s after the peer died mid-frame")
	}

	if ss.Reusable() {
		t.Error("Reusable() = true after carrier death; a pool would lend this session out again")
	}
}

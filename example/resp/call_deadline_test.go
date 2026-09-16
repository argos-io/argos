package resp

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/argos-io/argos/framing"
	"github.com/argos-io/argos/transport"
	"github.com/argos-io/argos/transport/tcp"
)

// In-package: the defect is in the framing Call itself — its ctx and the
// deadline it reports — so the test opens a call on a clientSession directly
// instead of going through client.CallStream, whose cancel watch would mask the
// missing deadline by closing the call for its own reasons.

// silentPeer accepts connections, answers the RESP hello handshake, and then
// never replies to anything. The peer would never reply.
func silentPeer(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				buf := make([]byte, 64)
				if _, err := c.Read(buf); err != nil {
					return
				}
				if _, err := c.Write(EncodeSimple("OK")); err != nil {
					return
				}
				_, _ = io.Copy(io.Discard, c)
			}(c)
		}
	}()
	return ln.Addr().String()
}

// clientCallAgainstSilentPeer dials silentPeer, completes the client handshake
// and opens one call with callCtx.
func clientCallAgainstSilentPeer(t *testing.T, callCtx context.Context) framing.Call {
	t.Helper()
	fr := New()
	tr := tcp.New()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := tr.Dial(ctx, transport.DialSpec{Endpoint: silentPeer(t)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	sess, err := fr.NewClientSession(ctx, conn, framing.SessionSpec{})
	if err != nil {
		t.Fatalf("NewClientSession: %v", err)
	}
	t.Cleanup(func() { _ = sess.Close() })

	c, err := sess.OpenCall(callCtx, MethodPING, framing.CallSpec{})
	if err != nil {
		t.Fatalf("OpenCall: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// TestCallReportsCallerDeadline: a call must observe the ctx it was opened with
// and report that deadline, so the deadline has one authority — the ctx the
// composition layer handed to OpenCall. Reporting none while the caller's ctx
// carried one is what let a call outlive its caller's deadline.
func TestCallReportsCallerDeadline(t *testing.T) {
	deadline := time.Now().Add(2 * time.Second)
	callCtx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()

	c := clientCallAgainstSilentPeer(t, callCtx)

	got, ok := c.Deadline()
	if !ok {
		t.Fatal("Deadline() = (zero, false); want the deadline of the ctx given to OpenCall")
	}
	if !got.Equal(deadline) {
		t.Fatalf("Deadline() = %v, want %v (the ctx deadline)", got, deadline)
	}
}

// TestCallRecvBoundedByCallerDeadline: with the call ctx derived from
// context.Background(), Recv on a peer that never replies had nothing to stop
// it — the call outlived the caller's deadline and blocked forever.
func TestCallRecvBoundedByCallerDeadline(t *testing.T) {
	callCtx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	c := clientCallAgainstSilentPeer(t, callCtx)
	if err := c.Send(EncodeArgs()); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if err := c.HalfClose(); err != nil {
		t.Fatalf("HalfClose: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		_, _, err := c.Recv()
		done <- err
	}()

	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Recv err = %v, want context.DeadlineExceeded", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Recv still blocked 2.7s past the caller deadline")
	}
}

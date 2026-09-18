package resp

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/argos-io/argos"
	"github.com/argos-io/argos/client"
	"github.com/argos-io/argos/internal/teststack"
)

// silentRESPPeer accepts connections, answers the HELLO handshake, and then
// never replies to anything.
func silentRESPPeer(t *testing.T) string {
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
					return // HELLO
				}
				if _, err := c.Write([]byte("+OK\r\n")); err != nil {
					return
				}
				_, _ = io.Copy(io.Discard, c)
			}(c)
		}
	}()
	return ln.Addr().String()
}

// TestCallerDeadlineEndsBlockedRecv is the caller-visible half of the deadline
// defect: a caller that supplied a deadline must not stay blocked on a peer that
// answers HELLO and then goes silent. (Axis-level coverage of the same
// defect: TestCallRecvBoundedByCallerDeadline.)
func TestCallerDeadlineEndsBlockedRecv(t *testing.T) {
	target := silentRESPPeer(t)

	axis := New()
	t.Cleanup(func() { _ = axis.Close() })
	cli, err := client.New(
		argos.WithClientOptions(baseOptions()),
		argos.WithServiceName(svcName),
		argos.WithTransport(teststack.TransportName(t, axis)), argos.WithCodec("raw"),
		argos.WithTarget("ip://"+target),
	)
	if err != nil {
		t.Fatalf("client.New: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	st, err := cli.Open(ctx, MethodPING)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	if err := st.Send(EncodeArgs()); err != nil {
		t.Fatalf("Send: %v", err)
	}
	_ = st.HalfClose()

	done := make(chan error, 1)
	go func() {
		var out []byte
		done <- st.Recv(&out)
	}()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Recv err = %v, want context.DeadlineExceeded", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Recv still blocked 4.5s past the caller deadline")
	}
}

package resp_test

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/argos-io/argos"
	"github.com/argos-io/argos/client"
	"github.com/argos-io/argos/example/resp"
	"github.com/argos-io/argos/transport/tcp"
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
// answers HELLO and then goes silent. (Framing-level coverage of the same
// defect: TestCallRecvBoundedByCallerDeadline.)
func TestCallerDeadlineEndsBlockedRecv(t *testing.T) {
	target := silentRESPPeer(t)

	var clientFr *resp.Framing
	clientFn := func() (argos.Binding, error) {
		// HELLO runs in NewClientSession, so the peer must answer the handshake
		// before the call is even admitted.
		clientFr = resp.New()
		return argos.Binding{
			Transport: tcp.New(),
			Framing:   clientFr,
			Codec:     resp.NewBytesCodec(),
		}, nil
	}
	cfg, err := argos.New(append(baseOpts(),
		argos.WithService(svcName,
			argos.ServiceBinding(clientFn),
			argos.ServiceTarget("ip://"+target),
		),
	)...)
	if err != nil {
		t.Fatalf("argos.New: %v", err)
	}
	cli, err := client.New(cfg, svcName)
	if err != nil {
		t.Fatalf("client.New: %v", err)
	}
	t.Cleanup(func() { _ = cli.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	st, err := cli.Open(ctx, resp.MethodPING)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	if err := st.Send(resp.EncodeArgs()); err != nil {
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

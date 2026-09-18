package grpc

import (
	"context"
	"testing"
	"time"

	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/internal/fake"
	"github.com/argos-io/argos/internal/session"
	"github.com/argos-io/argos/metadata"
	"github.com/argos-io/argos/status"
)

// Send over MaxFrameSize is the same class of call-scoped limit as the inbound
// checks and must be reportable by code, not only by message text.
func TestGRPCOversizeWireSendReportsResourceExhausted(t *testing.T) {
	t.Parallel()
	cli, lis := fake.HTTPLoopback()
	t.Cleanup(func() { _ = cli.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	go func() {
		for {
			srv, err := lis.Accept(ctx)
			if err != nil {
				return
			}
			_ = srv.Close()
		}
	}()

	f := newTestFraming(t, nil, "")
	// MaxFrameSize small enough that any payload exceeds it once framed.
	spec := session.SessionSpec{CodecName: "proto", Options: session.Options{
		MaxMessageSize: 1 << 20,
		MaxFrameSize:   4,
	}}
	cs, err := f.NewClientSession(ctx, cli, spec)
	if err != nil {
		t.Fatalf("NewClientSession: %v", err)
	}
	t.Cleanup(func() { _ = cs.Close() })

	call, err := cs.OpenCall(ctx, descriptor.MustMethod("echo.v1.Echo.Echo", descriptor.Unary),
		session.CallSpec{Metadata: metadata.New(metadata.RoleInitiator, nil)})
	if err != nil {
		t.Fatalf("OpenCall: %v", err)
	}
	t.Cleanup(func() { _ = call.Close() })

	err = call.Send([]byte("more than four bytes"))
	if status.CodeOf(err) != status.ResourceExhausted {
		t.Fatalf("Send = %v (code %v), want ResourceExhausted", err, status.CodeOf(err))
	}
	// A limit we chose is not damage to the connection.
	if !cs.Reusable() {
		t.Fatal("an oversize Send poisoned the shared Concurrent session")
	}
}

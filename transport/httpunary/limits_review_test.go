package httpunary

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/internal/session"
	"github.com/argos-io/argos/metadata"
	"github.com/argos-io/argos/status"
	"github.com/argos-io/argos/transport"
	"github.com/argos-io/argos/transport/http1"
)

// openLimitedCall dials a server that answers with body and serves one call
// under the given limits.
func openLimitedCall(t *testing.T, cfg session.Options, body []byte, hdr map[string]string) (session.ClientSession, session.Call) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		for k, v := range hdr {
			w.Header().Set(k, v)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)

	conn, err := http1.New().Dial(ctx, transport.DialSpec{Endpoint: srv.URL})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	cs, err := newRPC().NewClientSession(ctx, conn,
		session.SessionSpec{CodecName: "bytes", Options: cfg})
	if err != nil {
		t.Fatalf("NewClientSession: %v", err)
	}
	t.Cleanup(func() { _ = cs.Close() })

	call, err := cs.OpenCall(ctx, descriptor.MustMethod("svc.Limits", descriptor.Unary),
		session.CallSpec{Metadata: metadata.New(metadata.RoleInitiator, nil)})
	if err != nil {
		t.Fatalf("OpenCall: %v", err)
	}
	t.Cleanup(func() { _ = call.Close() })
	return cs, call
}

// Reuse() is Concurrent: one response over MaxMessageSize is a limit we chose,
// not damage to the connection, so it must not drop the endpoint handle out
// from under the other calls sharing it.
func TestOversizeResponseKeepsSessionReusable(t *testing.T) {
	t.Parallel()
	cs, call := openLimitedCall(t, session.Options{MaxMessageSize: 16}, make([]byte, 4096), nil)

	if err := call.HalfClose(); err != nil {
		t.Fatalf("HalfClose: %v", err)
	}
	_, _, err := call.Recv()
	if status.CodeOf(err) != status.ResourceExhausted {
		t.Fatalf("Recv = %v, want ResourceExhausted", err)
	}
	if !cs.Reusable() {
		t.Fatal("an oversize response poisoned the shared Concurrent session")
	}
}

// Same reasoning for the inbound metadata ceiling.
func TestOversizeResponseMetadataKeepsSessionReusable(t *testing.T) {
	t.Parallel()
	big := make([]byte, 4096)
	for i := range big {
		big[i] = 'a'
	}
	cs, call := openLimitedCall(t,
		session.Options{MaxMessageSize: 1 << 20, MaxInboundMetadataSize: 32},
		[]byte("ok"),
		map[string]string{"x-argos-big": string(big)})

	if err := call.HalfClose(); err != nil {
		t.Fatalf("HalfClose: %v", err)
	}
	_, _, err := call.Recv()
	if status.CodeOf(err) != status.ResourceExhausted {
		t.Fatalf("Recv = %v, want ResourceExhausted", err)
	}
	if !cs.Reusable() {
		t.Fatal("oversize response metadata poisoned the shared Concurrent session")
	}
}

// Send over MaxMessageSize is the same class of limit as the inbound checks and
// must be reportable by code, not only by message text.
func TestOversizeSendReportsResourceExhausted(t *testing.T) {
	t.Parallel()
	_, call := openLimitedCall(t, session.Options{MaxMessageSize: 16}, []byte("ok"), nil)
	err := call.Send(make([]byte, 64))
	if status.CodeOf(err) != status.ResourceExhausted {
		t.Fatalf("Send = %v (code %v), want ResourceExhausted", err, status.CodeOf(err))
	}
}

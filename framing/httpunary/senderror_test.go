package httpunary

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/framing"
	"github.com/argos-io/argos/metadata"
	"github.com/argos-io/argos/status"
	"github.com/argos-io/argos/stream"
	"github.com/argos-io/argos/transport"
	"github.com/argos-io/argos/transport/http1"
)

// bytesCodec passes payloads through unchanged: this test is about the send
// direction, not about encoding.
type bytesCodec struct{}

func (bytesCodec) Marshal(v any) ([]byte, error) {
	switch x := v.(type) {
	case []byte:
		return append([]byte(nil), x...), nil
	case string:
		return []byte(x), nil
	default:
		return nil, fmt.Errorf("bytesCodec: unsupported %T", v)
	}
}

func (bytesCodec) Unmarshal(b []byte, v any) error {
	switch x := v.(type) {
	case *[]byte:
		*x = append([]byte(nil), b...)
		return nil
	case *string:
		*x = string(b)
		return nil
	default:
		return fmt.Errorf("bytesCodec: unsupported %T", v)
	}
}

func (bytesCodec) CodecName() string { return "bytes" }

// TestEarlyRejectionKeepsResponseReadable is the httpunary payoff end to end on
// the real HTTP/1.1 transport: the server rejects the request without reading
// its body (the classic Unauthenticated-on-upload case), so the initiator's
// body write dies with a transport write error while the 401 response is
// already on the wire. The initiator must see stream.ErrSendClosed from
// HalfClose — the send direction is over, the exchange is not — and its next
// Recv must still produce the remote status.
func TestEarlyRejectionKeepsResponseReadable(t *testing.T) {
	t.Parallel()

	rejection := status.Error(status.Unauthenticated, "token expired")
	body := EncodeErrorBody(rejection)

	// The handler answers immediately and never reads the request body, which
	// is what makes the client's body write fail: net/http stops reading the
	// request body once the response is complete.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("content-type", "application/json")
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	conn, err := http1.New().Dial(ctx, transport.DialSpec{Endpoint: srv.URL})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = conn.Close() }()

	method := descriptor.MustMethod("svc.Senderror", descriptor.Unary)
	cs, err := NewRPC().NewClientSession(ctx, conn, framing.SessionSpec{CodecName: "bytes"})
	if err != nil {
		t.Fatalf("NewClientSession: %v", err)
	}
	defer func() { _ = cs.Close() }()

	md := metadata.New(metadata.RoleInitiator, nil)
	call, err := cs.OpenCall(ctx, method, framing.CallSpec{Metadata: md})
	if err != nil {
		t.Fatalf("OpenCall: %v", err)
	}
	defer func() { _ = call.Close() }()
	st := stream.Wrap(call, bytesCodec{})

	// Larger than the kernel can absorb (tcp_wmem + tcp_rmem autotune to a few
	// MiB each on loopback), so the body write cannot complete before the
	// rejection arrives.
	upload := make([]byte, 32<<20)
	if err := call.Send(upload); err != nil {
		t.Fatalf("Send (buffered): %v", err)
	}

	sendErr := st.HalfClose()
	if !errors.Is(sendErr, stream.ErrSendClosed) {
		t.Fatalf("HalfClose after early rejection = %v; want stream.ErrSendClosed", sendErr)
	}
	// The framing error stays the transport's SendError: cause and the verdict
	// that the exchange is still readable are observable there, while
	// stream.Wrap replaces it with its own sentinel.
	se, ok := transport.AsSendError(call.HalfClose())
	if !ok || !se.ReceiveOpen() {
		t.Fatalf("call.HalfClose() = %v; want a transport.SendError with ReceiveOpen true", call.HalfClose())
	}

	var resp []byte
	recvErr := st.Recv(&resp)
	if status.CodeOf(recvErr) != status.Unauthenticated {
		t.Fatalf("Recv after send failure = %v (body %q); want status %v",
			recvErr, resp, status.Unauthenticated)
	}
	if recvErr.Error() != rejection.Error() {
		t.Fatalf("Recv status = %q; want %q", recvErr, rejection)
	}

	// One endpoint handle serves concurrent calls: a body write that failed
	// because the response arrived first says nothing about the endpoint.
	if !cs.Reusable() {
		t.Fatal("client session not reusable after a body write failure on an open exchange")
	}
}

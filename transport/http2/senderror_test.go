package http2

import (
	"errors"
	"testing"

	"github.com/argos-io/argos/transport"
)

// A request-body write failure on an HTTP/2 stream says nothing about the
// response direction: streams are independent, so a server that rejected the
// call early may already have written trailers. The failure must therefore be
// classified as a transport.SendError with the receive direction still open, so
// framing maps it to stream.ErrSendClosed instead of tearing the stream down.
func TestRequestWriteFailureKeepsReceiveOpen(t *testing.T) {
	t.Parallel()
	writeErr := errors.New("write: broken pipe")

	live := &clientCarrier{}
	got := live.sendErr(writeErr)
	var se transport.SendError
	if !errors.As(got, &se) {
		t.Fatalf("write failure %v is not a transport.SendError", got)
	}
	if !se.ReceiveOpen() {
		t.Fatal("a live carrier must keep the receive direction open")
	}
	if !errors.Is(got, writeErr) {
		t.Fatalf("cause not reachable: %v", got)
	}

	// Once the carrier is torn down, nothing can be received.
	dead := &clientCarrier{aborted: true}
	var se2 transport.SendError
	if !errors.As(dead.sendErr(writeErr), &se2) {
		t.Fatal("aborted carrier write failure not classified")
	}
	if se2.ReceiveOpen() {
		t.Fatal("an aborted carrier must report the receive direction closed")
	}

	if err := live.sendErr(nil); err != nil {
		t.Fatalf("nil write error became %v", err)
	}
}

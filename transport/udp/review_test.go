package udp_test

import (
	"context"
	"testing"
	"time"

	"github.com/argos-io/argos/transport"
	"github.com/argos-io/argos/transport/udp"
)

// The client read path reuses one MaxDatagramSize landing buffer instead of
// allocating 64 KiB per call and returning a slice of it, which kept the whole
// array alive for as long as the caller held the payload. A returned payload
// must therefore be a copy: the next read overwrites the buffer.
func TestClientRecvDatagramPayloadSurvivesNextRead(t *testing.T) {
	_, addr := startEchoServer(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	clientTr := udp.New()
	t.Cleanup(func() { _ = clientTr.Close() })
	conn, err := clientTr.Dial(ctx, transport.DialSpec{Endpoint: addr})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer func() { _ = conn.Close() }()
	dc := conn.(transport.CarrierConn).Carrier().(transport.DatagramCarrier)

	first := []byte("first-payload")
	second := []byte("SECOND")

	if err := dc.SendDatagram(first); err != nil {
		t.Fatalf("SendDatagram #1: %v", err)
	}
	a, err := dc.RecvDatagram()
	if err != nil {
		t.Fatalf("RecvDatagram #1: %v", err)
	}
	if string(a) != string(first) {
		t.Fatalf("payload #1 = %q, want %q", a, first)
	}

	if err := dc.SendDatagram(second); err != nil {
		t.Fatalf("SendDatagram #2: %v", err)
	}
	b, err := dc.RecvDatagram()
	if err != nil {
		t.Fatalf("RecvDatagram #2: %v", err)
	}
	if string(b) != string(second) {
		t.Fatalf("payload #2 = %q, want %q", b, second)
	}
	// The first payload must not have been clobbered by the second read.
	if string(a) != string(first) {
		t.Fatalf("payload #1 became %q after the next read: the reused buffer is aliased", a)
	}
}

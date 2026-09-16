package udp

import (
	"errors"
	"net"
	"testing"

	"github.com/argos-io/argos/transport"
)

// UDP carries one request datagram per call: a failed send means no response
// can ever arrive, so framing must treat it as terminal (ReceiveOpen() false)
// rather than as a finished send direction it may keep reading behind.
func TestSendDatagramFailureReportsReceiveClosed(t *testing.T) {
	uc, err := net.DialUDP("udp", nil, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9})
	if err != nil {
		t.Fatalf("DialUDP: %v", err)
	}
	c := newClientConn(uc)

	// Close the socket behind the Conn so Conn.closed stays false: the write is
	// a real I/O failure, not the closed-Conn fast path.
	if err := uc.Close(); err != nil {
		t.Fatalf("close socket: %v", err)
	}

	sendErr := c.SendDatagram([]byte("ping"))
	if sendErr == nil {
		t.Fatal("SendDatagram succeeded on a closed socket")
	}
	var se transport.SendError
	if !errors.As(sendErr, &se) {
		t.Fatalf("SendDatagram error %v (%T) is not a transport.SendError", sendErr, sendErr)
	}
	if se.ReceiveOpen() {
		t.Fatal("ReceiveOpen() = true; one datagram per call means no response can arrive")
	}
}

// The same holds for the closed-association fast path.
func TestOversizeSendDatagramReportsReceiveClosed(t *testing.T) {
	uc, err := net.DialUDP("udp", nil, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9})
	if err != nil {
		t.Fatalf("DialUDP: %v", err)
	}
	c := newClientConn(uc)

	sendErr := c.SendDatagram(make([]byte, MaxDatagramSize+1))
	if sendErr == nil {
		t.Fatal("SendDatagram succeeded on oversize payload")
	}
	var se transport.SendError
	if !errors.As(sendErr, &se) {
		t.Fatalf("SendDatagram error %v (%T) is not a transport.SendError", sendErr, sendErr)
	}
	if se.ReceiveOpen() {
		t.Fatal("ReceiveOpen() = true; oversize datagram never left")
	}
}

func TestSendDatagramOnClosedConnReportsReceiveClosed(t *testing.T) {
	uc, err := net.DialUDP("udp", nil, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9})
	if err != nil {
		t.Fatalf("DialUDP: %v", err)
	}
	c := newClientConn(uc)
	_ = c.Close()

	sendErr := c.SendDatagram([]byte("ping"))
	if sendErr == nil {
		t.Fatal("SendDatagram succeeded on a closed Conn")
	}
	if !errors.Is(sendErr, net.ErrClosed) {
		t.Fatalf("SendDatagram error = %v, want net.ErrClosed", sendErr)
	}
	var se transport.SendError
	if !errors.As(sendErr, &se) {
		t.Fatalf("SendDatagram error %v (%T) is not a transport.SendError", sendErr, sendErr)
	}
	if se.ReceiveOpen() {
		t.Fatal("ReceiveOpen() = true on a closed association")
	}
}

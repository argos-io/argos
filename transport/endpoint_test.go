package transport

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

func TestDialAddressExplicitOverride(t *testing.T) {
	s := NewListenState()
	addr, err := s.DialAddress(context.Background(), ClientOptions{DialAddress: "10.0.0.1:8080"})
	if err != nil {
		t.Fatalf("DialAddress: %v", err)
	}
	if addr != "10.0.0.1:8080" {
		t.Fatalf("addr = %q", addr)
	}
}

func TestDialAddressRequiresConfiguredOrExplicit(t *testing.T) {
	s := NewListenState()
	_, err := s.DialAddress(context.Background(), ClientOptions{})
	if err == nil || err.Error() != "transport: dial address required" {
		t.Fatalf("error = %v", err)
	}
}

func TestDialAddressFixedListenAddress(t *testing.T) {
	s := NewListenState()
	s.MarkListen("127.0.0.1:9090", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 9090}, nil)
	addr, err := s.DialAddress(context.Background(), ClientOptions{})
	if err != nil {
		t.Fatalf("DialAddress: %v", err)
	}
	if addr != "127.0.0.1:9090" {
		t.Fatalf("addr = %q", addr)
	}
}

func TestDialAddressPortZeroWaitsForBind(t *testing.T) {
	s := NewListenState()
	s.MarkListen("127.0.0.1:0", &net.TCPAddr{IP: net.IPv4zero, Port: 54321}, nil)

	addr, err := s.DialAddress(context.Background(), ClientOptions{})
	if err != nil {
		t.Fatalf("DialAddress: %v", err)
	}
	if addr != "127.0.0.1:54321" {
		t.Fatalf("addr = %q, want 127.0.0.1:54321", addr)
	}
}

func TestDialAddressPortZeroContextCancel(t *testing.T) {
	s := NewListenState()
	s.configured = "127.0.0.1:0"

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	_, err := s.DialAddress(ctx, ClientOptions{})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want context deadline", err)
	}
}

func TestDialAddressPortZeroListenError(t *testing.T) {
	s := NewListenState()
	listenErr := errors.New("bind failed")
	s.MarkListen("127.0.0.1:0", nil, listenErr)

	_, err := s.DialAddress(context.Background(), ClientOptions{})
	if !errors.Is(err, listenErr) {
		t.Fatalf("error = %v", err)
	}
}

func TestDialableAddress(t *testing.T) {
	tests := []struct {
		name string
		addr net.Addr
		want string
	}{
		{
			name: "unspecified tcp",
			addr: &net.TCPAddr{IP: net.IPv4zero, Port: 8080},
			want: "127.0.0.1:8080",
		},
		{
			name: "specific tcp",
			addr: &net.TCPAddr{IP: net.IPv4(192, 168, 1, 1), Port: 9000},
			want: "192.168.1.1:9000",
		},
		{
			name: "unspecified udp",
			addr: &net.UDPAddr{IP: net.IPv6unspecified, Port: 7000},
			want: "127.0.0.1:7000",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := DialableAddress(tc.addr); got != tc.want {
				t.Fatalf("DialableAddress = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestBoundAddrAfterMarkListen(t *testing.T) {
	s := NewListenState()
	bound := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1234}
	s.MarkListen(":1234", bound, nil)
	if got := s.BoundAddr(); got != bound {
		t.Fatalf("BoundAddr = %v, want %v", got, bound)
	}
}

func TestListenTCPRequiresAddress(t *testing.T) {
	_, err := ListenTCP(ServerOptions{})
	if err == nil || err.Error() != "transport: listen address required" {
		t.Fatalf("error = %v", err)
	}
}

func TestListenTCPWithInjectedListener(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	got, err := ListenTCP(ApplyServerOptions([]ServerOption{WithListener(ln)}))
	if err != nil {
		t.Fatalf("ListenTCP: %v", err)
	}
	if got != ln {
		t.Fatal("expected injected listener")
	}
}

func TestListenUDPRequiresAddress(t *testing.T) {
	_, err := ListenUDP(ServerOptions{})
	if err == nil || err.Error() != "transport: listen address required" {
		t.Fatalf("error = %v", err)
	}
}

func TestListenUDPOpensPacketConn(t *testing.T) {
	pc, err := ListenUDP(ServerOptions{ListenAddress: "127.0.0.1:0", Network: "udp"})
	if err != nil {
		t.Fatalf("ListenUDP: %v", err)
	}
	defer pc.Close()
	if pc.LocalAddr() == nil {
		t.Fatal("expected local address")
	}
}

package transport

import (
	"net"
	"testing"
)

func TestApplyServerOptionsMerges(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	opts := ApplyServerOptions([]ServerOption{
		WithListenAddress(":9090"),
		WithListener(ln),
		WithListenNetwork("tcp4"),
	})
	if opts.ListenAddress != ":9090" {
		t.Fatalf("ListenAddress = %q", opts.ListenAddress)
	}
	if opts.Listener != ln {
		t.Fatal("Listener not set")
	}
	if opts.Network != "tcp4" {
		t.Fatalf("Network = %q", opts.Network)
	}
}

func TestApplyClientOptionsMerges(t *testing.T) {
	opts := ApplyClientOptions([]ClientOption{
		WithDialAddress("127.0.0.1:7000"),
	})
	if opts.DialAddress != "127.0.0.1:7000" {
		t.Fatalf("DialAddress = %q", opts.DialAddress)
	}
}

package echov1

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/argos-io/argos"
	jsoncodec "github.com/argos-io/argos/codec/json"
	protobufcodec "github.com/argos-io/argos/codec/protobuf"
	"github.com/argos-io/argos/transport/http1"
	"github.com/argos-io/argos/transport/http2"
	"github.com/argos-io/argos/transport/tcp"
	"github.com/argos-io/argos/transport/telnet"
	"github.com/argos-io/argos/transport/udp"
	"github.com/argos-io/argos/transport/ws"
)

// TestProtocolAccept runs external clients (grpcurl, curl, python3 scripts) against
// a live echo server on each transport. Set ARGOS_PROTOCOL_ACCEPT=1 (see make
// test-protocol); otherwise the test is skipped.
func TestProtocolAccept(t *testing.T) {
	requireProtocolAccept(t)

	cases := []struct {
		name   string
		newTR  func() addrTransport
		codec  argos.Codec
		script string
	}{
		{
			name:   "grpc-http2",
			newTR:  func() addrTransport { return http2.New().(addrTransport) },
			codec:  protobufcodec.New(),
			script: "accept-grpcurl.sh",
		},
		{
			name:   "rest-http1",
			newTR:  func() addrTransport { return http1.New().(addrTransport) },
			codec:  jsoncodec.New(),
			script: "accept-curl.sh",
		},
		{
			name:   "envelope-tcp",
			newTR:  func() addrTransport { return tcp.New().(addrTransport) },
			codec:  protobufcodec.New(),
			script: "accept-envelope.sh",
		},
		{
			name:   "envelope-ws",
			newTR:  func() addrTransport { return ws.New().(addrTransport) },
			codec:  protobufcodec.New(),
			script: "accept-ws.sh",
		},
		{
			name:   "envelope-udp",
			newTR:  func() addrTransport { return udp.New().(addrTransport) },
			codec:  protobufcodec.New(),
			script: "accept-udp.sh",
		},
		{
			name:   "telnet-json",
			newTR:  func() addrTransport { return telnet.New().(addrTransport) },
			codec:  jsoncodec.New(),
			script: "accept-telnet.sh",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tr := tc.newTR()
			startEchoServer(t, tr, tc.codec)

			script := scriptPath(t, tc.script)
			cmd := exec.CommandContext(t.Context(), script, dialAddr(tr.Addr()), tc.name)
			output, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("%s: %v\n%s", tc.script, err, output)
			}
			t.Logf("%s", output)
		})
	}
}

// TestProtocolAcceptWatch runs external clients against server-streaming Watch on
// transports that support streaming (http2, tcp, ws).
func TestProtocolAcceptWatch(t *testing.T) {
	requireProtocolAccept(t)

	cases := []struct {
		name   string
		newTR  func() addrTransport
		codec  argos.Codec
		script string
	}{
		{
			name:   "grpc-http2",
			newTR:  func() addrTransport { return http2.New().(addrTransport) },
			codec:  protobufcodec.New(),
			script: "accept-grpcurl-watch.sh",
		},
		{
			name:   "envelope-tcp",
			newTR:  func() addrTransport { return tcp.New().(addrTransport) },
			codec:  protobufcodec.New(),
			script: "accept-envelope-watch.sh",
		},
		{
			name:   "envelope-ws",
			newTR:  func() addrTransport { return ws.New().(addrTransport) },
			codec:  protobufcodec.New(),
			script: "accept-ws-watch.sh",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tr := tc.newTR()
			startEchoServer(t, tr, tc.codec)

			script := scriptPath(t, tc.script)
			cmd := exec.CommandContext(t.Context(), script, dialAddr(tr.Addr()), tc.name)
			output, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("%s: %v\n%s", tc.script, err, output)
			}
			t.Logf("%s", output)
		})
	}
}

func requireProtocolAccept(t *testing.T) {
	t.Helper()
	if os.Getenv("ARGOS_PROTOCOL_ACCEPT") != "1" {
		t.Skip("set ARGOS_PROTOCOL_ACCEPT=1 to run external protocol accept tests (make test-protocol)")
	}
	for _, tool := range []string{"bash", "python3", "grpcurl", "curl"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Fatalf("%s is required for protocol accept tests: %v", tool, err)
		}
	}
}

func scriptPath(t *testing.T, name string) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatalf("Abs: %v", err)
	}
	path := filepath.Join(root, "scripts", name)
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("script %s: %v", name, err)
	}
	return path
}

func dialAddr(addr net.Addr) string {
	switch a := addr.(type) {
	case *net.TCPAddr:
		if a.IP.IsUnspecified() || len(a.IP) == 0 {
			return net.JoinHostPort("127.0.0.1", fmt.Sprint(a.Port))
		}
	case *net.UDPAddr:
		if a.IP.IsUnspecified() || len(a.IP) == 0 {
			return net.JoinHostPort("127.0.0.1", fmt.Sprint(a.Port))
		}
	}
	return addr.String()
}

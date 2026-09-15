package probe

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"io"
	"net"
	"net/http"
	"testing"

	"golang.org/x/net/http2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/protobuf/proto"
)

// readLPM 读一条 gRPC 长度前缀消息。零长度是一条空消息，不是流结束——
// 这条坑在 v1 transport/http2 的 readLPM 里有注释，§4.6 也记了。
func readLPM(r io.Reader) ([]byte, error) {
	var hdr [5]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}
	if hdr[0] != 0 {
		return nil, io.ErrUnexpectedEOF // 探针不处理压缩
	}
	n := binary.BigEndian.Uint32(hdr[1:])
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

func writeLPM(w io.Writer, payload []byte) error {
	var hdr [5]byte
	binary.BigEndian.PutUint32(hdr[1:], uint32(len(payload)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	_, err := w.Write(payload)
	return err
}

func TestGRPCGoClientToProbeServer(t *testing.T) {
	srv := newH2CServer(t, func(w http.ResponseWriter, r *http.Request) {
		if got, want := r.URL.Path, "/grpc.health.v1.Health/Check"; got != want {
			t.Errorf("path = %q, want %q", got, want)
		}
		raw, err := readLPM(r.Body)
		if err != nil {
			t.Errorf("read request: %v", err)
			return
		}
		var req grpc_health_v1.HealthCheckRequest
		if err := proto.Unmarshal(raw, &req); err != nil {
			t.Errorf("unmarshal: %v", err)
			return
		}

		w.Header().Set("Content-Type", "application/grpc+proto")
		w.Header().Set("Trailer", "Grpc-Status, Grpc-Message")
		w.WriteHeader(http.StatusOK)

		out, _ := proto.Marshal(&grpc_health_v1.HealthCheckResponse{
			Status: grpc_health_v1.HealthCheckResponse_SERVING,
		})
		if err := writeLPM(w, out); err != nil {
			t.Errorf("write response: %v", err)
			return
		}
		w.(http.Flusher).Flush()
		w.Header().Set("Grpc-Status", "0")
	})

	cc, err := grpc.NewClient(srv.addr.String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	t.Cleanup(func() { _ = cc.Close() })

	rsp, err := grpc_health_v1.NewHealthClient(cc).Check(
		context.Background(), &grpc_health_v1.HealthCheckRequest{})
	if err != nil {
		t.Fatalf("Check: %v", err)
	}
	if rsp.Status != grpc_health_v1.HealthCheckResponse_SERVING {
		t.Fatalf("status = %v, want SERVING", rsp.Status)
	}
}

func TestProbeClientToGRPCGoServer(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	gs := grpc.NewServer()
	grpc_health_v1.RegisterHealthServer(gs, health.NewServer())
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)

	cl := &http.Client{Transport: &http2.Transport{
		AllowHTTP: true,
		DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, addr)
		},
	}}
	t.Cleanup(cl.CloseIdleConnections)

	url := "http://" + lis.Addr().String() + "/grpc.health.v1.Health/Check"
	c := newH2Endpoint(cl, url).openH2Stream(t.Context(), grpcHeaders())

	payload, _ := proto.Marshal(&grpc_health_v1.HealthCheckRequest{})
	if err := writeLPM(c, payload); err != nil {
		t.Fatalf("send: %v", err)
	}
	if err := c.CloseSend(); err != nil {
		t.Fatalf("close send: %v", err)
	}

	body, err := c.Body()
	if err != nil {
		t.Fatalf("response: %v", err)
	}
	raw, err := readLPM(body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	var rsp grpc_health_v1.HealthCheckResponse
	if err := proto.Unmarshal(raw, &rsp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if rsp.Status != grpc_health_v1.HealthCheckResponse_SERVING {
		t.Fatalf("status = %v, want SERVING", rsp.Status)
	}

	// 必须读到 EOF 才能拿到完整 trailers（§2.2 单响应终态）。
	if _, err := readLPM(body); err != io.EOF {
		t.Fatalf("second recv = %v, want io.EOF", err)
	}
	if got := c.Trailers().Get("Grpc-Status"); got != "0" {
		t.Fatalf("grpc-status = %q, want 0", got)
	}
}

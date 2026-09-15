package probe

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// tlsClientCreds builds grpc TransportCredentials that trust the probe TLS server.
// InsecureSkipVerify is fine here: newTLSServer already pins ALPN h2 via selfSigned.
func tlsClientCreds() credentials.TransportCredentials {
	return credentials.NewTLS(&tls.Config{
		InsecureSkipVerify: true,
		NextProtos:         []string{"h2"},
		MinVersion:         tls.VersionTLS12,
	})
}

// parseGrpcTimeout parses the gRPC Timeout header (e.g. "100m", "100000u").
func parseGrpcTimeout(s string) (time.Duration, error) {
	if s == "" {
		return 0, strconv.ErrSyntax
	}
	unit := s[len(s)-1]
	n, err := strconv.ParseInt(s[:len(s)-1], 10, 64)
	if err != nil {
		return 0, err
	}
	switch unit {
	case 'H':
		return time.Duration(n) * time.Hour, nil
	case 'M':
		return time.Duration(n) * time.Minute, nil
	case 'S':
		return time.Duration(n) * time.Second, nil
	case 'm':
		return time.Duration(n) * time.Millisecond, nil
	case 'u':
		return time.Duration(n) * time.Microsecond, nil
	case 'n':
		return time.Duration(n) * time.Nanosecond, nil
	default:
		return 0, strconv.ErrSyntax
	}
}

func TestGRPCGoTLS(t *testing.T) {
	t.Run("TLS_ALPN", func(t *testing.T) {
		srv := newTLSServer(t, func(w http.ResponseWriter, r *http.Request) {
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

		// Probe HTTP/2 client: unary LPM round-trip + ALPN assertion.
		url := srv.url + "/grpc.health.v1.Health/Check"
		pr, pw := io.Pipe()
		req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, url, pr)
		if err != nil {
			t.Fatalf("new request: %v", err)
		}
		for k, vs := range grpcHeaders() {
			for _, v := range vs {
				req.Header.Add(k, v)
			}
		}
		errc := make(chan error, 1)
		var resp *http.Response
		go func() {
			var doErr error
			resp, doErr = srv.client.Do(req)
			errc <- doErr
		}()
		payload, _ := proto.Marshal(&grpc_health_v1.HealthCheckRequest{})
		if err := writeLPM(pw, payload); err != nil {
			t.Fatalf("send: %v", err)
		}
		if err := pw.Close(); err != nil {
			t.Fatalf("close send: %v", err)
		}
		if err := <-errc; err != nil {
			t.Fatalf("Do: %v", err)
		}
		defer resp.Body.Close()
		if resp.TLS == nil {
			t.Fatal("resp.TLS is nil")
		}
		if got, want := resp.TLS.NegotiatedProtocol, "h2"; got != want {
			t.Fatalf("NegotiatedProtocol = %q, want %q", got, want)
		}
		raw, err := readLPM(resp.Body)
		if err != nil {
			t.Fatalf("read response: %v", err)
		}
		var hrsp grpc_health_v1.HealthCheckResponse
		if err := proto.Unmarshal(raw, &hrsp); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if hrsp.Status != grpc_health_v1.HealthCheckResponse_SERVING {
			t.Fatalf("status = %v, want SERVING", hrsp.Status)
		}
		if _, err := readLPM(resp.Body); err != io.EOF {
			t.Fatalf("second recv = %v, want io.EOF", err)
		}

		// grpc-go client over the same TLS endpoint.
		cc, err := grpc.NewClient(srv.addr.String(),
			grpc.WithTransportCredentials(tlsClientCreds()))
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
			t.Fatalf("grpc-go status = %v, want SERVING", rsp.Status)
		}
	})

	t.Run("TrailersOnly", func(t *testing.T) {
		srv := newTLSServer(t, func(w http.ResponseWriter, r *http.Request) {
			// Trailers-only: grpc-status in headers, WriteHeader(200), no DATA.
			w.Header().Set("Content-Type", "application/grpc")
			w.Header().Set("Grpc-Status", "5")
			w.Header().Set("Grpc-Message", "not found")
			w.WriteHeader(http.StatusOK)
		})

		cc, err := grpc.NewClient(srv.addr.String(),
			grpc.WithTransportCredentials(tlsClientCreds()))
		if err != nil {
			t.Fatalf("new client: %v", err)
		}
		t.Cleanup(func() { _ = cc.Close() })

		var hdr metadata.MD
		_, err = grpc_health_v1.NewHealthClient(cc).Check(
			context.Background(),
			&grpc_health_v1.HealthCheckRequest{},
			grpc.Header(&hdr),
		)
		if status.Code(err) != codes.NotFound {
			t.Fatalf("code = %v, want NotFound; err = %v", status.Code(err), err)
		}
		if len(hdr) != 0 {
			t.Fatalf("initial metadata = %v, want empty (trailers-only)", hdr)
		}
	})

	t.Run("Timeout", func(t *testing.T) {
		gotTimeout := make(chan string, 1)
		srv := newTLSServer(t, func(w http.ResponseWriter, r *http.Request) {
			select {
			case gotTimeout <- r.Header.Get("Grpc-Timeout"):
			default:
			}
			// Hold the request until the client deadline fires.
			<-r.Context().Done()
			w.Header().Set("Content-Type", "application/grpc")
			w.Header().Set("Grpc-Status", "4") // DeadlineExceeded
			w.WriteHeader(http.StatusOK)
		})

		cc, err := grpc.NewClient(srv.addr.String(),
			grpc.WithTransportCredentials(tlsClientCreds()))
		if err != nil {
			t.Fatalf("new client: %v", err)
		}
		t.Cleanup(func() { _ = cc.Close() })

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		_, err = grpc_health_v1.NewHealthClient(cc).Check(
			ctx, &grpc_health_v1.HealthCheckRequest{})
		if status.Code(err) != codes.DeadlineExceeded {
			t.Fatalf("code = %v, want DeadlineExceeded; err = %v", status.Code(err), err)
		}

		select {
		case raw := <-gotTimeout:
			d, err := parseGrpcTimeout(raw)
			if err != nil {
				t.Fatalf("parse Grpc-Timeout %q: %v", raw, err)
			}
			if d <= 0 || d > 100*time.Millisecond {
				t.Fatalf("Grpc-Timeout = %v (%q), want (0, 100ms]", d, raw)
			}
		case <-time.After(time.Second):
			t.Fatal("server did not observe Grpc-Timeout")
		}
	})

	t.Run("BinaryMetadata", func(t *testing.T) {
		trace := []byte{0x00, 0x01, 0xff, 0xfe, 0x7f, 'A'}
		wantWire := base64.RawStdEncoding.EncodeToString(trace)

		srv := newTLSServer(t, func(w http.ResponseWriter, r *http.Request) {
			wire := r.Header.Get("X-Trace-Bin")
			if wire == "" {
				// http2 may preserve lowercase; try canonical + raw.
				wire = r.Header.Get("x-trace-bin")
			}
			if strings.Contains(wire, "=") {
				t.Errorf("wire x-trace-bin is padded: %q", wire)
			}
			if wire != wantWire {
				t.Errorf("wire x-trace-bin = %q, want %q", wire, wantWire)
			}
			decoded, err := base64.RawStdEncoding.DecodeString(wire)
			if err != nil {
				t.Errorf("decode wire: %v", err)
				return
			}
			if !bytes.Equal(decoded, trace) {
				t.Errorf("decoded = %v, want %v", decoded, trace)
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
			w.Header().Set("X-Trace-Bin", wire) // echo wire form
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
			grpc.WithTransportCredentials(tlsClientCreds()))
		if err != nil {
			t.Fatalf("new client: %v", err)
		}
		t.Cleanup(func() { _ = cc.Close() })

		ctx := metadata.NewOutgoingContext(context.Background(),
			metadata.Pairs("x-trace-bin", string(trace)))
		var hdr metadata.MD
		rsp, err := grpc_health_v1.NewHealthClient(cc).Check(
			ctx, &grpc_health_v1.HealthCheckRequest{}, grpc.Header(&hdr))
		if err != nil {
			t.Fatalf("Check: %v", err)
		}
		if rsp.Status != grpc_health_v1.HealthCheckResponse_SERVING {
			t.Fatalf("status = %v, want SERVING", rsp.Status)
		}
		got := hdr.Get("x-trace-bin")
		if len(got) != 1 {
			t.Fatalf("response x-trace-bin = %v, want 1 value", got)
		}
		if !bytes.Equal([]byte(got[0]), trace) {
			t.Fatalf("client got %v, want %v", []byte(got[0]), trace)
		}
	})
}

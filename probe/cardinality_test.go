package probe

import (
	"errors"
	"io"
	"net/http"
	"testing"

	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/protobuf/proto"
)

// errCardinality 模拟 §2.2 / status.ErrCardinality：成功状态下零条或两条
// 响应时，单响应 wrapper 返回可 errors.Is 区分的本地哨兵，而不是当成功。
var errCardinality = errors.New("cardinality: want exactly one response")

// recvUnary 模拟生成桩的单响应接收：恰好一条 DATA，再读一次确认 EOF；
// 第二次接收必须用独立消息对象（§2.2 点名的坑）。
func recvUnary(body io.Reader, trailers func() http.Header) (*grpc_health_v1.HealthCheckResponse, error) {
	raw1, err := readLPM(body)
	if err == io.EOF {
		// trailers-only：OK 状态零条响应 → cardinality，不是成功。
		_ = trailers()
		return nil, errCardinality
	}
	if err != nil {
		return nil, err
	}
	first := &grpc_health_v1.HealthCheckResponse{}
	if err := proto.Unmarshal(raw1, first); err != nil {
		return nil, err
	}

	// 独立临时消息：绝不能复用 first，否则第二条会覆盖拟返回的响应。
	second := &grpc_health_v1.HealthCheckResponse{}
	raw2, err := readLPM(body)
	if err == nil {
		if err := proto.Unmarshal(raw2, second); err != nil {
			return nil, err
		}
		// 检出多条：first 必须仍完好（独立对象断言见测试）。
		_ = trailers()
		return first, errCardinality
	}
	if err != io.EOF {
		return nil, err
	}

	st := trailers().Get("Grpc-Status")
	if st != "0" {
		return nil, errors.New("remote grpc-status: " + st)
	}
	return first, nil
}

func writeProbeUnaryHeaders(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/grpc+proto")
	w.Header().Set("Trailer", "Grpc-Status, Grpc-Message")
	w.WriteHeader(http.StatusOK)
}

func servingPayload() []byte {
	out, _ := proto.Marshal(&grpc_health_v1.HealthCheckResponse{
		Status: grpc_health_v1.HealthCheckResponse_SERVING,
	})
	return out
}

func notServingPayload() []byte {
	out, _ := proto.Marshal(&grpc_health_v1.HealthCheckResponse{
		Status: grpc_health_v1.HealthCheckResponse_NOT_SERVING,
	})
	return out
}

func TestCardinality(t *testing.T) {
	t.Run("OneResponseThenEOF", func(t *testing.T) {
		srv := newH2CServer(t, func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.Copy(io.Discard, r.Body)
			writeProbeUnaryHeaders(w)
			if err := writeLPM(w, servingPayload()); err != nil {
				t.Errorf("write response: %v", err)
				return
			}
			w.(http.Flusher).Flush()
			w.Header().Set("Grpc-Status", "0")
		})

		c := newH2Endpoint(srv.client, srv.url).openH2Stream(t.Context(), grpcHeaders())
		if err := writeLPM(c, nil); err != nil {
			t.Fatalf("send: %v", err)
		}
		if err := c.CloseSend(); err != nil {
			t.Fatalf("close send: %v", err)
		}
		body, err := c.Body()
		if err != nil {
			t.Fatalf("response: %v", err)
		}

		rsp, err := recvUnary(body, c.Trailers)
		if err != nil {
			t.Fatalf("recvUnary: %v", err)
		}
		if rsp.Status != grpc_health_v1.HealthCheckResponse_SERVING {
			t.Fatalf("status = %v, want SERVING", rsp.Status)
		}
		if got := c.Trailers().Get("Grpc-Status"); got != "0" {
			t.Fatalf("grpc-status = %q, want 0", got)
		}
	})

	t.Run("ZeroResponseOK", func(t *testing.T) {
		srv := newH2CServer(t, func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.Copy(io.Discard, r.Body)
			writeProbeUnaryHeaders(w)
			// trailers-only：无 DATA，仅 Grpc-Status: 0。
			w.Header().Set("Grpc-Status", "0")
		})

		c := newH2Endpoint(srv.client, srv.url).openH2Stream(t.Context(), grpcHeaders())
		if err := writeLPM(c, nil); err != nil {
			t.Fatalf("send: %v", err)
		}
		if err := c.CloseSend(); err != nil {
			t.Fatalf("close send: %v", err)
		}
		body, err := c.Body()
		if err != nil {
			t.Fatalf("response: %v", err)
		}

		// 直接断言首次 readLPM 就是 EOF（trailers-only）。
		if _, err := readLPM(body); err != io.EOF {
			t.Fatalf("first readLPM = %v, want io.EOF", err)
		}
		if got := c.Trailers().Get("Grpc-Status"); got != "0" {
			t.Fatalf("grpc-status = %q, want 0", got)
		}

		// 同一场景下，单响应逻辑必须报 cardinality，不能当成功。
		c2 := newH2Endpoint(srv.client, srv.url).openH2Stream(t.Context(), grpcHeaders())
		if err := writeLPM(c2, nil); err != nil {
			t.Fatalf("send2: %v", err)
		}
		if err := c2.CloseSend(); err != nil {
			t.Fatalf("close send2: %v", err)
		}
		body2, err := c2.Body()
		if err != nil {
			t.Fatalf("response2: %v", err)
		}
		rsp, err := recvUnary(body2, c2.Trailers)
		if !errors.Is(err, errCardinality) {
			t.Fatalf("recvUnary = (%v, %v), want errCardinality", rsp, err)
		}
		if rsp != nil {
			t.Fatalf("rsp = %v, want nil on cardinality", rsp)
		}
	})

	t.Run("TwoResponsesOK", func(t *testing.T) {
		srv := newH2CServer(t, func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.Copy(io.Discard, r.Body)
			writeProbeUnaryHeaders(w)
			if err := writeLPM(w, servingPayload()); err != nil {
				t.Errorf("write first: %v", err)
				return
			}
			if err := writeLPM(w, notServingPayload()); err != nil {
				t.Errorf("write second: %v", err)
				return
			}
			w.(http.Flusher).Flush()
			w.Header().Set("Grpc-Status", "0")
		})

		c := newH2Endpoint(srv.client, srv.url).openH2Stream(t.Context(), grpcHeaders())
		if err := writeLPM(c, nil); err != nil {
			t.Fatalf("send: %v", err)
		}
		if err := c.CloseSend(); err != nil {
			t.Fatalf("close send: %v", err)
		}
		body, err := c.Body()
		if err != nil {
			t.Fatalf("response: %v", err)
		}

		raw1, err := readLPM(body)
		if err != nil {
			t.Fatalf("first readLPM: %v", err)
		}
		first := &grpc_health_v1.HealthCheckResponse{}
		if err := proto.Unmarshal(raw1, first); err != nil {
			t.Fatalf("unmarshal first: %v", err)
		}
		if first.Status != grpc_health_v1.HealthCheckResponse_SERVING {
			t.Fatalf("first status = %v, want SERVING", first.Status)
		}

		// 第二次必须用新对象：若复用 first，第二条会覆盖第一条。
		second := &grpc_health_v1.HealthCheckResponse{}
		raw2, err := readLPM(body)
		if err != nil {
			t.Fatalf("second readLPM = %v, want success (extra message)", err)
		}
		if err := proto.Unmarshal(raw2, second); err != nil {
			t.Fatalf("unmarshal second: %v", err)
		}
		if second.Status != grpc_health_v1.HealthCheckResponse_NOT_SERVING {
			t.Fatalf("second status = %v, want NOT_SERVING", second.Status)
		}
		if first.Status != grpc_health_v1.HealthCheckResponse_SERVING {
			t.Fatal("first message overwritten: second recv must use a new object (§2.2)")
		}
		if first == second {
			t.Fatal("first and second must be distinct message objects")
		}

		// drain to trailers
		if _, err := readLPM(body); err != io.EOF {
			t.Fatalf("third readLPM = %v, want io.EOF", err)
		}
		if got := c.Trailers().Get("Grpc-Status"); got != "0" {
			t.Fatalf("grpc-status = %q, want 0", got)
		}

		// 单响应逻辑检出多条 → cardinality；返回的 first 仍完好。
		c2 := newH2Endpoint(srv.client, srv.url).openH2Stream(t.Context(), grpcHeaders())
		if err := writeLPM(c2, nil); err != nil {
			t.Fatalf("send2: %v", err)
		}
		if err := c2.CloseSend(); err != nil {
			t.Fatalf("close send2: %v", err)
		}
		body2, err := c2.Body()
		if err != nil {
			t.Fatalf("response2: %v", err)
		}
		rsp, err := recvUnary(body2, c2.Trailers)
		if !errors.Is(err, errCardinality) {
			t.Fatalf("recvUnary = (%v, %v), want errCardinality", rsp, err)
		}
		if rsp == nil || rsp.Status != grpc_health_v1.HealthCheckResponse_SERVING {
			t.Fatalf("preserved first = %v, want SERVING (not overwritten)", rsp)
		}
	})

	t.Run("OneResponseThenError", func(t *testing.T) {
		srv := newH2CServer(t, func(w http.ResponseWriter, r *http.Request) {
			_, _ = io.Copy(io.Discard, r.Body)
			writeProbeUnaryHeaders(w)
			if err := writeLPM(w, servingPayload()); err != nil {
				t.Errorf("write response: %v", err)
				return
			}
			w.(http.Flusher).Flush()
			w.Header().Set("Grpc-Status", "13") // Internal
			w.Header().Set("Grpc-Message", "boom")
		})

		c := newH2Endpoint(srv.client, srv.url).openH2Stream(t.Context(), grpcHeaders())
		if err := writeLPM(c, nil); err != nil {
			t.Fatalf("send: %v", err)
		}
		if err := c.CloseSend(); err != nil {
			t.Fatalf("close send: %v", err)
		}
		body, err := c.Body()
		if err != nil {
			t.Fatalf("response: %v", err)
		}

		rsp, err := recvUnary(body, c.Trailers)
		if err == nil {
			t.Fatalf("recvUnary succeeded with %v, want remote error", rsp)
		}
		if errors.Is(err, errCardinality) {
			t.Fatalf("got cardinality, want remote error: %v", err)
		}
		if got := err.Error(); got != "remote grpc-status: 13" {
			t.Fatalf("err = %q, want remote grpc-status: 13", got)
		}
		if rsp != nil {
			t.Fatalf("rsp = %v, must not return DATA as success when status is non-OK", rsp)
		}
		if got := c.Trailers().Get("Grpc-Status"); got != "13" {
			t.Fatalf("grpc-status = %q, want 13", got)
		}
	})
}

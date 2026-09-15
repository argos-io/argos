package probe

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net/http"
	"runtime"
	"testing"
	"time"
)

// TestSlowConsumerBudgetPeak：服务端尽力写 1000×64KiB，客户端每条 sleep 1ms。
// 有界预读下计费峰值必须 ≤ perCall（16 MiB）。
func TestSlowConsumerBudgetPeak(t *testing.T) {
	const (
		msgSize = 64 << 10 // 64 KiB
		msgCount = 1000
	)

	srv := newH2CServer(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		h := w.Header()
		h.Set("Content-Type", "application/grpc+proto")
		h.Set("Trailer", "Grpc-Status, Grpc-Message")
		w.WriteHeader(http.StatusOK)

		payload := make([]byte, msgSize)
		for range msgCount {
			if err := writeLPM(w, payload); err != nil {
				return
			}
			w.(http.Flusher).Flush()
		}
		w.Header().Set("Grpc-Status", "0")
	})

	ctx := t.Context()
	c := newH2Endpoint(srv.client, srv.url).openH2Stream(ctx, grpcHeaders())
	if err := c.CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}
	body, err := c.Body()
	if err != nil {
		t.Fatalf("Body: %v", err)
	}
	t.Cleanup(func() { _ = body.Close() })

	bud := &callBudget{}
	q := startRecvQueue(ctx, body, bud)

	for i := range msgCount {
		msg, err := q.Recv(ctx)
		if err != nil {
			t.Fatalf("Recv #%d: %v", i, err)
		}
		if len(msg) != msgSize {
			t.Fatalf("Recv #%d: len=%d, want %d", i, len(msg), msgSize)
		}
		time.Sleep(time.Millisecond)
	}
	// 流应结束
	if _, err := q.Recv(ctx); !errors.Is(err, io.EOF) {
		t.Fatalf("final Recv = %v, want io.EOF", err)
	}
	q.Wait()
	q.Drain()

	used, peak := bud.snapshot()
	t.Logf("measured peak charged bytes: %d (perCall=%d, used_after=%d)", peak, perCall, used)
	if peak > perCall {
		t.Fatalf("peak charged %d > perCall %d", peak, perCall)
	}
	if used != 0 {
		t.Fatalf("budget not fully released after drain: used=%d", used)
	}
	// 无界队列会吃到约 msgCount×msgSize = 64 MiB；有界预读峰值应远低于 perCall。
	if peak > 2*msgSize {
		// 允许在途一条 + 队列一条；若更大说明读 ahead 失控。
		t.Fatalf("peak %d exceeds 2×msgSize=%d; read-ahead appears unbounded", peak, 2*msgSize)
	}
}

// TestCancelMidBackpressure：背压中途取消 ctx，接收 goroutine 退出、额度归零、无泄漏。
func TestCancelMidBackpressure(t *testing.T) {
	runtime.GC()
	base := runtime.NumGoroutine()

	writing := make(chan struct{})
	srv := newH2CServer(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		h := w.Header()
		h.Set("Content-Type", "application/grpc+proto")
		h.Set("Trailer", "Grpc-Status, Grpc-Message")
		w.WriteHeader(http.StatusOK)

		payload := make([]byte, 64<<10)
		first := true
		for {
			if err := writeLPM(w, payload); err != nil {
				return
			}
			w.(http.Flusher).Flush()
			if first {
				close(writing)
				first = false
			}
		}
	})

	ctx, cancel := context.WithCancel(t.Context())
	c := newH2Endpoint(srv.client, srv.url).openH2Stream(ctx, grpcHeaders())
	if err := c.CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}
	body, err := c.Body()
	if err != nil {
		t.Fatalf("Body: %v", err)
	}

	bud := &callBudget{}
	q := startRecvQueue(ctx, body, bud)

	select {
	case <-writing:
	case <-time.After(5 * time.Second):
		t.Fatal("server never started writing")
	}
	// 不消费，让 channel 填满并在 send 上背压；同时给读侧一点时间 charge。
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		used, _ := bud.snapshot()
		if used > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	usedBefore, _ := bud.snapshot()
	if usedBefore == 0 {
		t.Fatal("expected charged budget under backpressure before cancel")
	}

	cancel()
	_ = body.Close() // 打断可能阻塞在 ReadFull 上的接收 goroutine
	q.Wait()
	q.Drain()

	used, _ := bud.snapshot()
	if used != 0 {
		t.Fatalf("budget not fully released after cancel: used=%d (was %d)", used, usedBefore)
	}

	// 给 HTTP/2 与 RoundTrip goroutine 一点收敛时间。
	settle := time.Now().Add(2 * time.Second)
	var after int
	for time.Now().Before(settle) {
		runtime.GC()
		after = runtime.NumGoroutine()
		if after <= base+8 { // 允许 httptest / 空闲连接等少量常驻差额
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if after > base+8 {
		t.Fatalf("goroutine leak: before=%d after=%d", base, after)
	}
}

// TestLargeMessageExactMaxOK：正好 MaxMessageSize（4 MiB）应通过。
func TestLargeMessageExactMaxOK(t *testing.T) {
	payload := make([]byte, MaxMessageSize)
	for i := range payload {
		payload[i] = byte(i)
	}
	var buf bytes.Buffer
	if err := writeLPM(&buf, payload); err != nil {
		t.Fatalf("writeLPM: %v", err)
	}

	bud := &callBudget{}
	q := startRecvQueue(t.Context(), &buf, bud)
	msg, err := q.Recv(t.Context())
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if len(msg) != MaxMessageSize {
		t.Fatalf("len=%d, want %d", len(msg), MaxMessageSize)
	}
	if _, err := q.Recv(t.Context()); !errors.Is(err, io.EOF) {
		t.Fatalf("second Recv = %v, want EOF", err)
	}
	q.Wait()
	q.Drain()
	used, peak := bud.snapshot()
	if used != 0 {
		t.Fatalf("used=%d, want 0", used)
	}
	if peak != int64(MaxMessageSize) {
		t.Fatalf("peak=%d, want %d", peak, MaxMessageSize)
	}
}

// TestLargeMessageOversizeRejected：MaxMessageSize+1 在分配前被拒，额度不加。
func TestLargeMessageOversizeRejected(t *testing.T) {
	var hdr [5]byte
	binary.BigEndian.PutUint32(hdr[1:], uint32(MaxMessageSize)+1)
	// 故意不附带超大 payload：若实现先分配再读，会在这里暴露（OOM/挂起）；
	// 正确路径只读 5 字节头就返回 ErrMessageTooLarge。
	r := bytes.NewReader(hdr[:])

	bud := &callBudget{}
	q := startRecvQueue(t.Context(), r, bud)
	_, err := q.Recv(t.Context())
	if !errors.Is(err, ErrMessageTooLarge) {
		t.Fatalf("Recv = %v, want ErrMessageTooLarge", err)
	}
	q.Wait()
	q.Drain()
	used, peak := bud.snapshot()
	if used != 0 || peak != 0 {
		t.Fatalf("budget touched on oversize reject: used=%d peak=%d", used, peak)
	}
}

package probe

import (
	"context"
	"io"
	"net/http"
	"runtime"
	"sync"
	"testing"
	"time"
)

// writeUnary 发一个最小请求体并结束发送侧，然后读完响应。
func writeUnary(ctx context.Context, ep *h2Endpoint) error {
	c := ep.openH2Stream(ctx, grpcHeaders())
	if _, err := c.Write([]byte{0}); err != nil {
		_ = c.CloseSend()
		return err
	}
	if err := c.CloseSend(); err != nil {
		return err
	}
	body, err := c.Body()
	if err != nil {
		return err
	}
	defer body.Close()
	_, err = io.Copy(io.Discard, body)
	return err
}

func grpcOKHandler(record func(remoteAddr string), hold <-chan struct{}) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if record != nil {
			record(r.RemoteAddr)
		}
		_, _ = io.Copy(io.Discard, r.Body)
		if hold != nil {
			<-hold
		}
		h := w.Header()
		h.Set("Content-Type", "application/grpc+proto")
		h.Set("Trailer", "Grpc-Status, Grpc-Message")
		w.WriteHeader(http.StatusOK)
		h.Set("Grpc-Status", "0")
	}
}

// TestHTTP2ConnectionReuse：同一 http.Client 并发 32 个调用，服务端 RemoteAddr
// 去重后必须恰好一条 TCP 连接——HTTP/2 Concurrent 复用模型的直接证据。
func TestHTTP2ConnectionReuse(t *testing.T) {
	var mu sync.Mutex
	addrs := make(map[string]struct{})
	record := func(addr string) {
		mu.Lock()
		addrs[addr] = struct{}{}
		mu.Unlock()
	}

	srv := newH2CServer(t, grpcOKHandler(record, nil))
	ep := newH2Endpoint(srv.client, srv.url)

	const n = 32
	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			errs[i] = writeUnary(t.Context(), ep)
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Errorf("call %d: %v", i, err)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	if got := len(addrs); got != 1 {
		t.Fatalf("unique TCP connections = %d, want 1; addrs=%v", got, addrs)
	}
	t.Logf("unique connections = %d", len(addrs))
}

// TestHTTP2StreamIsolation：取消其中一个调用的 ctx，其余 31 个必须照常完成。
// 这是 §2.4「HTTP/2 不得关闭其他流共用的连接」的证据。
func TestHTTP2StreamIsolation(t *testing.T) {
	var mu sync.Mutex
	addrs := make(map[string]struct{})
	arrived := make(chan struct{}, 32)
	hold := make(chan struct{})

	record := func(addr string) {
		mu.Lock()
		addrs[addr] = struct{}{}
		mu.Unlock()
		arrived <- struct{}{}
	}

	srv := newH2CServer(t, grpcOKHandler(record, hold))
	ep := newH2Endpoint(srv.client, srv.url)

	const n = 32
	ctxs := make([]context.Context, n)
	cancels := make([]context.CancelFunc, n)
	for i := range n {
		ctxs[i], cancels[i] = context.WithCancel(context.Background())
	}
	t.Cleanup(func() {
		for _, c := range cancels {
			c()
		}
	})

	type result struct {
		i   int
		err error
	}
	results := make(chan result, n)

	for i := range n {
		go func(i int) {
			results <- result{i: i, err: writeUnary(ctxs[i], ep)}
		}(i)
	}

	// 等 32 个请求都到达服务端，确保已多路复用到同一条连接上。
	for range n {
		select {
		case <-arrived:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for 32 streams to arrive")
		}
	}

	cancels[0]() // 只取消第 0 路
	close(hold)  // 放行全部 handler 写响应

	var failed, ok int
	for range n {
		r := <-results
		if r.i == 0 {
			if r.err == nil {
				t.Error("cancelled call 0 unexpectedly succeeded")
			}
			failed++
			continue
		}
		if r.err != nil {
			t.Errorf("call %d failed after sibling cancel: %v", r.i, r.err)
			continue
		}
		ok++
	}
	if ok != n-1 {
		t.Fatalf("successful calls = %d, want %d", ok, n-1)
	}
	if failed != 1 {
		t.Fatalf("failed calls = %d, want 1 (the cancelled one)", failed)
	}

	mu.Lock()
	defer mu.Unlock()
	if got := len(addrs); got != 1 {
		t.Fatalf("unique TCP connections = %d, want 1; addrs=%v", got, addrs)
	}
	t.Logf("isolation ok: cancelled=1 succeeded=%d unique_conn=%d", ok, len(addrs))
}

// TestHTTP2StreamGoroutineLeak：32 路结束后 CloseIdleConnections + 短暂收敛，
// NumGoroutine 不得相对基线失控增长（允许小幅抖动）。
func TestHTTP2StreamGoroutineLeak(t *testing.T) {
	srv := newH2CServer(t, grpcOKHandler(nil, nil))
	ep := newH2Endpoint(srv.client, srv.url)

	runtime.GC()
	time.Sleep(50 * time.Millisecond)
	baseline := runtime.NumGoroutine()

	const n = 32
	var wg sync.WaitGroup
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := writeUnary(t.Context(), ep); err != nil {
				t.Errorf("call: %v", err)
			}
		}()
	}
	wg.Wait()

	srv.client.CloseIdleConnections()
	time.Sleep(100 * time.Millisecond)
	runtime.GC()
	time.Sleep(50 * time.Millisecond)

	after := runtime.NumGoroutine()
	const slack = 10 // 运行时/GC 抖动余量；禁止 32 路量级的常驻增长
	if after > baseline+slack {
		t.Fatalf("goroutines after settle = %d, baseline = %d (slack %d): runaway growth from %d streams",
			after, baseline, slack, n)
	}
	t.Logf("goroutines baseline=%d after=%d delta=%d", baseline, after, after-baseline)
}

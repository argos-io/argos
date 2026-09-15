package probe

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func dialAddr(addr string) func(context.Context) (net.Conn, error) {
	return func(ctx context.Context) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", addr)
	}
}

// ---------------------------------------------------------------------------
// 步骤 1：单连接 100 次顺序调用，无串包
// ---------------------------------------------------------------------------

func TestSequential100CallsOneConn(t *testing.T) {
	addr, closeFn, err := StartSeqServer(SeqServerOpt{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(closeFn)

	pool := &SeqPool{MaxSessions: 64, Dial: dialAddr(addr)}
	t.Cleanup(pool.Close)

	ctx := context.Background()
	for i := 0; i < 100; i++ {
		payload := []byte(fmt.Sprintf("req-%d", i))
		call, sess, err := pool.Open(ctx)
		if err != nil {
			t.Fatalf("call %d Open: %v", i, err)
		}
		if err := call.Send(payload); err != nil {
			t.Fatalf("call %d Send: %v", i, err)
		}
		got, err := call.Recv()
		if err != nil {
			t.Fatalf("call %d Recv: %v", i, err)
		}
		if !bytes.Equal(got, payload) {
			t.Fatalf("call %d: got %q want %q (mix-up)", i, got, payload)
		}
		if _, err := call.Recv(); err != io.EOF {
			t.Fatalf("call %d terminal: %v", i, err)
		}
		pool.CloseCall(call, sess)
	}
	if pool.DialCount != 1 {
		t.Fatalf("DialCount=%d, want 1 (one connection)", pool.DialCount)
	}
	t.Logf("step1: DialCount=%d BorrowCount=%d ReturnCount=%d", pool.DialCount, pool.BorrowCount, pool.ReturnCount)
}

// ---------------------------------------------------------------------------
// 步骤 2：Call.Close 不关 Conn
// ---------------------------------------------------------------------------

func TestSequentialCloseDoesNotCloseConn(t *testing.T) {
	addr, closeFn, err := StartSeqServer(SeqServerOpt{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(closeFn)

	var flag *closeFlagConn
	pool := &SeqPool{
		MaxSessions: 64,
		Dial: func(ctx context.Context) (net.Conn, error) {
			c, err := dialAddr(addr)(ctx)
			if err != nil {
				return nil, err
			}
			flag = &closeFlagConn{Conn: c}
			return flag, nil
		},
	}
	t.Cleanup(pool.Close)

	ctx := context.Background()
	for i := 0; i < 100; i++ {
		call, sess, err := pool.Open(ctx)
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		payload := []byte(fmt.Sprintf("c-%d", i))
		if err := call.Send(payload); err != nil {
			t.Fatal(err)
		}
		got, err := call.Recv()
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, payload) {
			t.Fatalf("mismatch %q vs %q", got, payload)
		}
		_, _ = call.Recv()
		pool.CloseCall(call, sess)
	}
	if flag == nil {
		t.Fatal("no conn dialed")
	}
	if flag.Closed() {
		t.Fatal("Conn.Close was called after 100 calls; Call.Close must not close Conn")
	}
}

// ---------------------------------------------------------------------------
// 步骤 3：并发借出会话 → 8 条互不相同，不排队
// ---------------------------------------------------------------------------

func TestConcurrentSessionBorrow(t *testing.T) {
	addr, closeFn, err := StartSeqServer(SeqServerOpt{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(closeFn)

	pool := &SeqPool{MaxSessions: 64, Dial: dialAddr(addr)}
	t.Cleanup(pool.Close)

	const n = 8
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		sessions []*SeqSession
		openErr  error
	)
	start := make(chan struct{})
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			<-start
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			call, sess, err := pool.Open(ctx)
			if err != nil {
				mu.Lock()
				openErr = err
				mu.Unlock()
				return
			}
			mu.Lock()
			sessions = append(sessions, sess)
			mu.Unlock()
			time.Sleep(50 * time.Millisecond)
			_ = call.Close()
			pool.Return(sess)
		}()
	}
	close(start)
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("borrow queued/blocked; expected 8 distinct sessions without waiting")
	}
	if openErr != nil {
		t.Fatalf("Open: %v", openErr)
	}
	if len(sessions) != n {
		t.Fatalf("got %d sessions, want %d", len(sessions), n)
	}
	seen := map[*SeqSession]struct{}{}
	for _, s := range sessions {
		if _, ok := seen[s]; ok {
			t.Fatal("duplicate session borrowed; Sequential capacity 1 must yield distinct sessions")
		}
		seen[s] = struct{}{}
	}
	if pool.DialCount > 64 {
		t.Fatalf("DialCount=%d exceeds cap", pool.DialCount)
	}
	if pool.DialCount < n {
		t.Fatalf("DialCount=%d, want >= %d", pool.DialCount, n)
	}
	t.Logf("step3: DialCount=%d distinct=%d", pool.DialCount, len(seen))
}

// ---------------------------------------------------------------------------
// 步骤 3b：ErrSessionBusy 兜底；耗尽 → ResourceExhausted；哨兵不外泄
// ---------------------------------------------------------------------------

func TestSessionBusyFallback(t *testing.T) {
	addr, closeFn, err := StartSeqServer(SeqServerOpt{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(closeFn)

	const max = 3
	framing := NewSeqFraming()
	framing.OpenCallHook = func(sessionID int, callSeq int) error {
		if callSeq >= 2 {
			return ErrSessionBusy
		}
		return nil
	}

	pool := &SeqPool{
		MaxSessions: max,
		Framing:     framing,
		Dial:        dialAddr(addr),
	}
	t.Cleanup(pool.Close)

	ctx := context.Background()

	var held []*struct {
		c *SeqCall
		s *SeqSession
	}
	for i := 0; i < max; i++ {
		call, sess, err := pool.Open(ctx)
		if err != nil {
			t.Fatalf("warm Open %d: %v", i, err)
		}
		held = append(held, &struct {
			c *SeqCall
			s *SeqSession
		}{call, sess})
	}
	for _, h := range held {
		_ = h.c.Send([]byte("warm"))
		_, _ = h.c.Recv()
		_, _ = h.c.Recv()
		pool.CloseCall(h.c, h.s)
	}

	call, sess, err := pool.Open(ctx)
	if err == nil {
		pool.CloseCall(call, sess)
		t.Fatal("expected ResourceExhausted after Busy retries exhausted")
	}
	if errors.Is(err, ErrSessionBusy) {
		t.Fatalf("ErrSessionBusy leaked to caller: %v", err)
	}
	if !errors.Is(err, ErrSessionsExhausted) {
		t.Fatalf("want errors.Is ErrSessionsExhausted, got %v", err)
	}
	var re *resourceExhaustedError
	if !errors.As(err, &re) {
		t.Fatalf("want ResourceExhausted wrapper, got %T %v", err, err)
	}
	if pool.BusyRetry == 0 {
		t.Fatal("expected Busy retries")
	}
	if pool.BusyRetry > max {
		t.Fatalf("BusyRetry=%d exceeds MaxSessionsPerEndpoint=%d", pool.BusyRetry, max)
	}
	t.Logf("step3b: BusyRetry=%d DialCount=%d err=%v", pool.BusyRetry, pool.DialCount, err)
}

// ---------------------------------------------------------------------------
// 步骤 4：故障丢弃
// ---------------------------------------------------------------------------

func TestFaultDiscardSession(t *testing.T) {
	addr, closeFn, err := StartSeqServer(SeqServerOpt{
		DropOnCall:   50,
		DropMidWrite: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(closeFn)

	pool := &SeqPool{MaxSessions: 64, Dial: dialAddr(addr)}
	t.Cleanup(pool.Close)

	ctx := context.Background()
	var badSess *SeqSession
	for i := 1; i <= 50; i++ {
		call, sess, err := pool.Open(ctx)
		if err != nil {
			t.Fatalf("Open %d: %v", i, err)
		}
		payload := []byte(fmt.Sprintf("f-%d", i))
		if err := call.Send(payload); err != nil {
			t.Fatalf("Send %d: %v", i, err)
		}
		_, err = call.Recv()
		if i < 50 {
			if err != nil {
				t.Fatalf("Recv %d: %v", i, err)
			}
			_, _ = call.Recv()
			pool.CloseCall(call, sess)
			continue
		}
		if err == nil {
			t.Fatal("call 50: expected read error after peer drop")
		}
		badSess = sess
		_ = call.Close()
		if badSess.Reusable() {
			t.Fatal("faulted session still Reusable(); want false")
		}
		pool.Return(sess)
	}

	dialsBefore := pool.DialCount
	for i := 0; i < 5; i++ {
		call, sess, err := pool.Open(ctx)
		if err != nil {
			t.Fatalf("post-fault Open: %v", err)
		}
		payload := []byte(fmt.Sprintf("ok-%d", i))
		if err := call.Send(payload); err != nil {
			t.Fatal(err)
		}
		got, err := call.Recv()
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got, payload) {
			t.Fatalf("got %q want %q", got, payload)
		}
		_, _ = call.Recv()
		pool.CloseCall(call, sess)
	}
	if pool.DialCount <= dialsBefore {
		t.Fatalf("expected new dial after fault discard; DialCount=%d before=%d", pool.DialCount, dialsBefore)
	}
	t.Logf("step4: DialCount=%d (new conn after discard)", pool.DialCount)
}

// ---------------------------------------------------------------------------
// 步骤 5：上限 2，4 并发 → 立即 ResourceExhausted
// ---------------------------------------------------------------------------

func TestCapResourceExhaustedImmediate(t *testing.T) {
	addr, closeFn, err := StartSeqServer(SeqServerOpt{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(closeFn)

	pool := &SeqPool{MaxSessions: 2, Dial: dialAddr(addr)}
	t.Cleanup(pool.Close)

	release := make(chan struct{})
	var wg sync.WaitGroup
	var (
		mu      sync.Mutex
		results []error
		held    int32
	)

	launch := func() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			started := time.Now()
			call, sess, err := pool.Open(ctx)
			elapsed := time.Since(started)
			mu.Lock()
			results = append(results, err)
			mu.Unlock()
			if err != nil {
				if elapsed > 200*time.Millisecond {
					t.Errorf("exhausted path blocked %v; want immediate", elapsed)
				}
				return
			}
			atomic.AddInt32(&held, 1)
			<-release
			_ = call.Close()
			pool.Return(sess)
		}()
	}

	for i := 0; i < 4; i++ {
		launch()
	}
	deadline := time.Now().Add(2 * time.Second)
	for atomic.LoadInt32(&held) < 2 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if atomic.LoadInt32(&held) < 2 {
		close(release)
		wg.Wait()
		t.Fatalf("only %d held, want 2", held)
	}
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()

	var ok, exhausted int
	for _, err := range results {
		if err == nil {
			ok++
			continue
		}
		if errors.Is(err, ErrSessionsExhausted) {
			exhausted++
			if errors.Is(err, ErrSessionBusy) {
				t.Fatal("ErrSessionBusy leaked")
			}
			continue
		}
		t.Fatalf("unexpected err: %v", err)
	}
	if ok != 2 || exhausted != 2 {
		t.Fatalf("ok=%d exhausted=%d results=%v", ok, exhausted, results)
	}
	t.Logf("step5: ok=%d exhausted=%d DialCount=%d", ok, exhausted, pool.DialCount)
}

// ---------------------------------------------------------------------------
// 步骤 5b：承载卫生
// ---------------------------------------------------------------------------

func TestCarrierHygiene(t *testing.T) {
	t.Run("hygiene_on_no_mix", func(t *testing.T) {
		addr, closeFn, err := StartSeqServer(SeqServerOpt{
			MultiResponseFor: "multi-30",
			SlowTailFor:      "multi-30",
			SlowTail:         30 * time.Millisecond,
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(closeFn)

		pool := &SeqPool{MaxSessions: 64, Dial: dialAddr(addr)}
		t.Cleanup(pool.Close)
		ctx := context.Background()

		var markedBad bool
		for i := 1; i <= 31; i++ {
			call, sess, err := pool.Open(ctx)
			if err != nil {
				t.Fatalf("Open %d: %v", i, err)
			}
			var payload []byte
			if i == 30 {
				payload = []byte("multi-30")
			} else {
				payload = []byte(fmt.Sprintf("h-%d", i))
			}
			if err := call.Send(payload); err != nil {
				t.Fatal(err)
			}
			got, err := call.Recv()
			if err != nil {
				t.Fatalf("Recv %d: %v", i, err)
			}
			if i == 30 {
				if !bytes.Equal(got, payload) {
					t.Fatalf("call30 got %q", got)
				}
				_ = call.Close()
				if sess.Reusable() {
					t.Fatal("hygiene: session still reusable after early Close")
				}
				markedBad = true
				pool.Return(sess)
				continue
			}
			if !bytes.Equal(got, payload) {
				t.Fatalf("call %d mix: got %q want %q", i, got, payload)
			}
			_, _ = call.Recv()
			pool.CloseCall(call, sess)
		}
		if !markedBad {
			t.Fatal("call 30 did not mark session bad")
		}
		if pool.DialCount < 2 {
			t.Fatalf("expected new session after hygiene discard; DialCount=%d", pool.DialCount)
		}
	})

	t.Run("hygiene_off_mixes", func(t *testing.T) {
		addr, closeFn, err := StartSeqServer(SeqServerOpt{
			MultiResponseFor: "multi-30",
			SlowTailFor:      "multi-30",
			SlowTail:         30 * time.Millisecond,
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(closeFn)

		pool := &SeqPool{
			MaxSessions: 64,
			Dial:        dialAddr(addr),
			Framing:     &SeqFraming{Hygiene: false, HandshakeTimeout: 10 * time.Second},
		}
		t.Cleanup(pool.Close)
		ctx := context.Background()

		mixed := false
		for i := 1; i <= 31; i++ {
			call, sess, err := pool.Open(ctx)
			if err != nil {
				t.Fatalf("Open %d: %v", i, err)
			}
			var payload []byte
			if i == 30 {
				payload = []byte("multi-30")
			} else {
				payload = []byte(fmt.Sprintf("x-%d", i))
			}
			if err := call.Send(payload); err != nil {
				t.Fatal(err)
			}
			got, err := call.Recv()
			if err != nil {
				if i == 31 {
					mixed = true
					_ = call.Close()
					pool.Return(sess)
					break
				}
				t.Fatalf("Recv %d: %v", i, err)
			}
			if i == 30 {
				_ = call.Close()
				if !sess.Reusable() {
					t.Fatal("hygiene off: session should remain reusable")
				}
				pool.Return(sess)
				time.Sleep(50 * time.Millisecond)
				continue
			}
			if i == 31 {
				want := payload
				if !bytes.Equal(got, want) {
					mixed = true
					t.Logf("hygiene off: call31 got leftover %q instead of %q (expected mix)", got, want)
				} else {
					t.Fatal("hygiene off: expected packet mix but call31 matched; rule necessity unproven")
				}
				_ = call.Close()
				pool.Return(sess)
				continue
			}
			if !bytes.Equal(got, payload) {
				t.Fatalf("call %d: %q vs %q", i, got, payload)
			}
			_, _ = call.Recv()
			pool.CloseCall(call, sess)
		}
		if !mixed {
			t.Fatal("hygiene off did not observe mix")
		}
	})
}

// ---------------------------------------------------------------------------
// 步骤 5c：Read 永不并发
// ---------------------------------------------------------------------------

func TestReadGoroutineHandoff(t *testing.T) {
	addr, closeFn, err := StartSeqServer(SeqServerOpt{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(closeFn)

	var rc *reentryConn
	pool := &SeqPool{
		MaxSessions: 64,
		Dial: func(ctx context.Context) (net.Conn, error) {
			c, err := dialAddr(addr)(ctx)
			if err != nil {
				return nil, err
			}
			rc = &reentryConn{Conn: c}
			return rc, nil
		},
	}
	t.Cleanup(pool.Close)

	ctx := context.Background()
	for i := 0; i < 100; i++ {
		call, sess, err := pool.Open(ctx)
		if err != nil {
			t.Fatal(err)
		}
		payload := []byte(fmt.Sprintf("r-%d", i))
		if err := call.Send(payload); err != nil {
			t.Fatal(err)
		}
		if _, err := call.Recv(); err != nil {
			t.Fatal(err)
		}
		_, _ = call.Recv()
		if err := call.Close(); err != nil {
			t.Fatal(err)
		}
		pool.Return(sess)
	}
	if rc == nil {
		t.Fatal("no conn")
	}
	if rc.ConcurrentHits() != 0 {
		t.Fatalf("concurrent Read reentry hits=%d", rc.ConcurrentHits())
	}
}

// ---------------------------------------------------------------------------
// 步骤 5d：跨调用读缓冲
// ---------------------------------------------------------------------------

func TestCrossCallReadBuffer(t *testing.T) {
	frame := func(p []byte) []byte {
		h := make([]byte, 4+len(p))
		binary.BigEndian.PutUint32(h[:4], uint32(len(p)))
		copy(h[4:], p)
		return h
	}
	end := frame(nil)
	next := frame([]byte("NEXT-CALL"))
	part1 := end[:2]
	part2 := append(append([]byte{}, end[2:]...), next...)

	sc := &scriptedConn{reads: [][]byte{part1, part2}}
	framing := NewSeqFraming()
	sess, err := framing.NewClientSession(context.Background(), sc)
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()

	call, err := sess.OpenCall(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	_, err = call.Recv()
	if err != io.EOF {
		t.Fatalf("want EOF terminal, got %v", err)
	}
	if err := call.Close(); err != nil {
		t.Fatal(err)
	}

	buf := sess.PeekBuf()
	if len(buf) == 0 {
		t.Fatal("Session buffer empty after Close; next-call bytes were discarded")
	}
	if !bytes.Equal(buf, next) {
		t.Fatalf("Session buf=%x want next frame %x", buf, next)
	}

	call2, err := sess.OpenCall(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer call2.Close()
	got, err := call2.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "NEXT-CALL" {
		t.Fatalf("got %q", got)
	}
}

// ---------------------------------------------------------------------------
// 步骤 5e：连接 ctx 归属
// ---------------------------------------------------------------------------

func TestClientConnCtxOwnership(t *testing.T) {
	t.Run("not_child_of_call_ctx", func(t *testing.T) {
		addr, closeFn, err := StartSeqServer(SeqServerOpt{})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(closeFn)

		clientCtx, clientCancel := context.WithCancel(context.Background())
		defer clientCancel()

		pool := &SeqPool{
			MaxSessions: 64,
			Dial:        dialAddr(addr),
			ClientCtx:   clientCtx,
		}
		t.Cleanup(pool.Close)

		callCtx, callCancel := context.WithCancel(context.Background())
		call, sess, err := pool.Open(callCtx)
		if err != nil {
			t.Fatal(err)
		}
		if err := call.Send([]byte("one")); err != nil {
			t.Fatal(err)
		}
		if _, err := call.Recv(); err != nil {
			t.Fatal(err)
		}
		_, _ = call.Recv()
		pool.CloseCall(call, sess)

		callCancel()
		if !sess.Reusable() {
			t.Fatal("session not reusable after call ctx cancel")
		}
		call2, sess2, err := pool.Open(context.Background())
		if err != nil {
			t.Fatalf("borrow after call ctx cancel: %v", err)
		}
		_ = call2.Send([]byte("two"))
		got, err := call2.Recv()
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != "two" {
			t.Fatalf("got %q", got)
		}
		_, _ = call2.Recv()
		pool.CloseCall(call2, sess2)

		select {
		case <-sess.ConnCtx().Done():
			t.Fatal("conn ctx cancelled with call ctx; conn ctx must be under Client lifetime")
		default:
		}
	})

	t.Run("handshake_timeout_not_on_conn", func(t *testing.T) {
		addr, closeFn, err := StartSeqServer(SeqServerOpt{})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(closeFn)

		pool := &SeqPool{
			MaxSessions:      64,
			Dial:             dialAddr(addr),
			HandshakeTimeout: 50 * time.Millisecond,
		}
		t.Cleanup(pool.Close)

		call, sess, err := pool.Open(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		_ = call.Send([]byte("early"))
		_, _ = call.Recv()
		_, _ = call.Recv()
		pool.CloseCall(call, sess)

		time.Sleep(80 * time.Millisecond)

		call2, sess2, err := pool.Open(context.Background())
		if err != nil {
			t.Fatalf("call after idle > HandshakeTimeout: %v (timeout must not be on conn ctx)", err)
		}
		if err := call2.Send([]byte("late")); err != nil {
			t.Fatal(err)
		}
		got, err := call2.Recv()
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != "late" {
			t.Fatalf("got %q", got)
		}
		_, _ = call2.Recv()
		pool.CloseCall(call2, sess2)
	})
}

package probe

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// 客户端 envelope-lite 助手
// ---------------------------------------------------------------------------

func dialLocal(addr string) (net.Conn, error) {
	var d net.Dialer
	return d.DialContext(context.Background(), "tcp", addr)
}

func dialAcceptServer(t *testing.T, s *acceptServer) net.Conn {
	t.Helper()
	c, err := dialLocal(s.addr())
	if err != nil {
		t.Fatal(err)
	}
	s.waitReady(t)
	return c
}

func clientOpen(w io.Writer, callID uint32, method string) error {
	return writeEnvFrame(w, seqFrameOpen, callID, []byte(method))
}

func clientData(w io.Writer, callID uint32, payload []byte) error {
	return writeEnvFrame(w, seqFrameData, callID, payload)
}

func clientEnd(w io.Writer, callID uint32) error {
	return writeEnvFrame(w, seqFrameEnd, callID, nil)
}

func clientReadStatus(r io.Reader, buf *[]byte) (callID uint32, code uint32, msg string, err error) {
	raw, rest, err := readFrameFrom(r, *buf)
	*buf = rest
	if err != nil {
		return 0, 0, "", err
	}
	typ, id, payload, err := parseEnvFrame(raw)
	if err != nil {
		return 0, 0, "", err
	}
	if typ != seqFrameStatus {
		return 0, 0, "", fmt.Errorf("expected STATUS, got %d", typ)
	}
	code, msg, err = parseStatusPayload(payload)
	return id, code, msg, err
}

func unaryCall(c net.Conn, buf *[]byte, callID uint32, method string, payload []byte) (code uint32, msg string, err error) {
	if err := clientOpen(c, callID, method); err != nil {
		return 0, "", err
	}
	if err := clientData(c, callID, payload); err != nil {
		return 0, "", err
	}
	if err := clientEnd(c, callID); err != nil {
		return 0, "", err
	}
	id, code, msg, err := clientReadStatus(c, buf)
	if err != nil {
		return 0, "", err
	}
	if id != callID {
		return 0, "", fmt.Errorf("status callID=%d want %d", id, callID)
	}
	return code, msg, nil
}

type acceptServer struct {
	ln      net.Listener
	f       *SeqFraming
	stats   *AcceptLoopStats
	handler func(ctx context.Context, call *ServerSeqCall) error

	mu           sync.Mutex
	acceptCancel context.CancelFunc
	connCancel   context.CancelFunc

	// ready 在第一条连接的 accept/conn cancel 发布后关闭。
	ready chan struct{}
	// firstDone 在第一条连接的 onConn 返回时关闭；创建后不再替换。
	firstDone chan struct{}
}

func startAcceptServer(t *testing.T, f *SeqFraming, handler func(ctx context.Context, call *ServerSeqCall) error) *acceptServer {
	t.Helper()
	if f == nil {
		f = NewSeqFraming()
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &acceptServer{
		ln:        ln,
		f:         f,
		stats:     &AcceptLoopStats{},
		handler:   handler,
		ready:     make(chan struct{}),
		firstDone: make(chan struct{}),
	}
	go s.serve()
	t.Cleanup(func() {
		_ = ln.Close()
		s.Shutdown()
		s.hardClose()
	})
	return s
}

func (s *acceptServer) addr() string { return s.ln.Addr().String() }

func (s *acceptServer) Shutdown() {
	s.mu.Lock()
	cancel := s.acceptCancel
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (s *acceptServer) hardClose() {
	s.mu.Lock()
	cancel := s.connCancel
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (s *acceptServer) waitReady(t *testing.T) {
	t.Helper()
	select {
	case <-s.ready:
	case <-time.After(2 * time.Second):
		t.Fatal("server conn not ready")
	}
}

func (s *acceptServer) waitOnConn(t *testing.T, d time.Duration) {
	t.Helper()
	select {
	case <-s.firstDone:
	case <-time.After(d):
		t.Fatalf("onConn did not exit within %v", d)
	}
}

func (s *acceptServer) serve() {
	c, err := s.ln.Accept()
	if err != nil {
		return
	}
	// 本探针 harness 只服务第一条连接（多连接场景用专用测试）。
	_ = s.ln.Close()

	connCtx, connCancel := context.WithCancel(context.Background())
	acceptCtx, acceptCancel := context.WithCancel(context.Background())
	s.mu.Lock()
	s.connCancel = connCancel
	s.acceptCancel = acceptCancel
	s.mu.Unlock()
	close(s.ready)

	defer close(s.firstDone)
	RunAcceptLoop(connCtx, acceptCtx, s.f, c, s.stats, s.handler)
}

func echoHandler(_ context.Context, call *ServerSeqCall) error {
	ctx := context.Background()
	for {
		payload, err := call.Recv(ctx)
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		_ = payload
	}
}

// ---------------------------------------------------------------------------
// 步骤 1：一条入站连接处理 10 个调用，全部在同一个 onConn 内
// ---------------------------------------------------------------------------

func TestAcceptCallMultiCallOneConn(t *testing.T) {
	s := startAcceptServer(t, NewSeqFraming(), echoHandler)

	c := dialAcceptServer(t, s)
	defer c.Close()

	var buf []byte
	for i := 1; i <= 10; i++ {
		code, _, err := unaryCall(c, &buf, uint32(i), "echo.Echo", []byte(fmt.Sprintf("p-%d", i)))
		if err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
		if code != 0 {
			t.Fatalf("call %d status code=%d", i, code)
		}
	}
	if s.stats.HandlerCount.Load() != 10 {
		t.Fatalf("HandlerCount=%d want 10", s.stats.HandlerCount.Load())
	}
	if s.stats.OnConnID.Load() != 1 {
		t.Fatalf("OnConnID=%d want 1 (single onConn)", s.stats.OnConnID.Load())
	}
	if s.stats.HandlerOnConn.Load() != 1 {
		t.Fatalf("HandlerOnConn=%d want 1 (all handlers in same onConn)", s.stats.HandlerOnConn.Load())
	}
}

// ---------------------------------------------------------------------------
// 步骤 2：第 5 个 handler 失败 → 对端收错误状态；6–10 照常
// ---------------------------------------------------------------------------

func TestAcceptCallSingleFailureContinues(t *testing.T) {
	s := startAcceptServer(t, NewSeqFraming(), func(ctx context.Context, call *ServerSeqCall) error {
		if call.CallID() == 5 {
			_, _ = call.Recv(ctx)
			return errors.New("handler boom")
		}
		return echoHandler(ctx, call)
	})

	c := dialAcceptServer(t, s)
	defer c.Close()

	var buf []byte
	for i := 1; i <= 10; i++ {
		code, msg, err := unaryCall(c, &buf, uint32(i), "echo.Echo", []byte("x"))
		if err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
		if i == 5 {
			if code == 0 {
				t.Fatal("call 5: want non-zero error status")
			}
			if msg == "" {
				t.Fatal("call 5: want error message")
			}
			continue
		}
		if code != 0 {
			t.Fatalf("call %d: unexpected status %d %q", i, code, msg)
		}
	}
	if s.stats.HandlerCount.Load() != 10 {
		t.Fatalf("HandlerCount=%d want 10", s.stats.HandlerCount.Load())
	}
}

// ---------------------------------------------------------------------------
// 步骤 3：对端关闭发送 → AcceptCall io.EOF；Session.Close 恰好一次
// ---------------------------------------------------------------------------

func TestAcceptCallNormalExitEOF(t *testing.T) {
	s := startAcceptServer(t, NewSeqFraming(), echoHandler)

	c := dialAcceptServer(t, s)
	var buf []byte
	if _, _, err := unaryCall(c, &buf, 1, "echo.Echo", []byte("one")); err != nil {
		t.Fatal(err)
	}
	// 关闭客户端连接（对端关闭）→ 服务端 AcceptCall 得 io.EOF
	_ = c.Close()

	select {
	case <-s.firstDone:
	case <-time.After(2 * time.Second):
		t.Fatal("onConn did not exit after peer close")
	}
	if s.stats.SessionCloses.Load() != 1 {
		t.Fatalf("SessionCloses=%d want 1", s.stats.SessionCloses.Load())
	}
	if s.stats.HandlerCount.Load() != 1 {
		t.Fatalf("HandlerCount=%d want 1", s.stats.HandlerCount.Load())
	}
}

// ---------------------------------------------------------------------------
// 步骤 4：第 5 个调用执行中 Shutdown → 跑完 #5，拒 #6+，然后关闭
// ---------------------------------------------------------------------------

func TestAcceptCallDrainOnShutdownMidCall(t *testing.T) {
	entered5 := make(chan struct{})
	release5 := make(chan struct{})
	var saw6 atomic.Bool

	s := startAcceptServer(t, NewSeqFraming(), func(ctx context.Context, call *ServerSeqCall) error {
		if call.CallID() == 5 {
			close(entered5)
			<-release5
			return echoHandler(ctx, call)
		}
		if call.CallID() >= 6 {
			saw6.Store(true)
		}
		return echoHandler(ctx, call)
	})

	c := dialAcceptServer(t, s)
	defer c.Close()

	var buf []byte
	for i := 1; i <= 4; i++ {
		if _, _, err := unaryCall(c, &buf, uint32(i), "echo.Echo", []byte("x")); err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
	}
	// 启动 call 5（OPEN+DATA，先不 END，让 handler 进 Recv 后我们再 Shutdown）
	if err := clientOpen(c, 5, "echo.Echo"); err != nil {
		t.Fatal(err)
	}
	if err := clientData(c, 5, []byte("mid")); err != nil {
		t.Fatal(err)
	}
	if err := clientEnd(c, 5); err != nil {
		t.Fatal(err)
	}

	select {
	case <-entered5:
	case <-time.After(2 * time.Second):
		t.Fatal("handler 5 did not start")
	}
	s.Shutdown() // 取消 accept ctx；#5 仍在跑

	// 客户端仍可能发 #6，但服务端不应处理
	_ = clientOpen(c, 6, "echo.Echo")
	_ = clientData(c, 6, []byte("nope"))
	_ = clientEnd(c, 6)

	close(release5)
	_, code, _, err := clientReadStatus(c, &buf)
	if err != nil {
		t.Fatalf("status for call 5: %v", err)
	}
	if code != 0 {
		t.Fatalf("call 5 status=%d", code)
	}

	select {
	case <-s.firstDone:
	case <-time.After(2 * time.Second):
		t.Fatal("onConn did not exit after drain")
	}
	if saw6.Load() {
		t.Fatal("call 6 was handled after Shutdown; want reject")
	}
	if s.stats.HandlerCount.Load() != 5 {
		t.Fatalf("HandlerCount=%d want 5", s.stats.HandlerCount.Load())
	}
	if s.stats.SessionCloses.Load() != 1 {
		t.Fatalf("SessionCloses=%d want 1", s.stats.SessionCloses.Load())
	}
}

// ---------------------------------------------------------------------------
// 步骤 4b：空闲连接 Shutdown → onConn 毫秒级返回（稳态关键路径）
// ---------------------------------------------------------------------------

func TestShutdownIdleConnReturnsQuickly(t *testing.T) {
	s := startAcceptServer(t, NewSeqFraming(), echoHandler)

	c := dialAcceptServer(t, s)
	defer c.Close()

	var buf []byte
	if _, _, err := unaryCall(c, &buf, 1, "echo.Echo", []byte("x")); err != nil {
		t.Fatal(err)
	}
	// 循环应正阻塞在 AcceptCall
	time.Sleep(50 * time.Millisecond)

	shutdownDeadline := 5 * time.Second // 若实现错误会等到这个
	start := time.Now()
	s.Shutdown()
	select {
	case <-s.firstDone:
	case <-time.After(shutdownDeadline):
		t.Fatal("onConn waited for Shutdown deadline; accept ctx cancel must wake AcceptCall")
	}
	elapsed := time.Since(start)
	if elapsed > 500*time.Millisecond {
		t.Fatalf("onConn returned in %v; want milliseconds-scale wake", elapsed)
	}
	t.Logf("step4b: idle Shutdown woke onConn in %v", elapsed)
}

// ---------------------------------------------------------------------------
// 步骤 4c：排空不杀在途——长调用 + 空闲连接；Shutdown 后空闲立刻退、长调用跑完
// ---------------------------------------------------------------------------

func TestShutdownDoesNotKillInFlight(t *testing.T) {
	longEntered := make(chan struct{})
	longRelease := make(chan struct{})
	var longFinished atomic.Bool
	var idleOnConnDone, longOnConnDone chan struct{}

	f := NewSeqFraming()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	type connCtrl struct {
		acceptCancel context.CancelFunc
		done         chan struct{}
		stats        *AcceptLoopStats
	}
	var (
		mu    sync.Mutex
		ctrls []*connCtrl
	)

	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			connCtx, connCancel := context.WithCancel(context.Background())
			acceptCtx, acceptCancel := context.WithCancel(context.Background())
			_ = connCancel
			stats := &AcceptLoopStats{}
			done := make(chan struct{})
			ctrl := &connCtrl{acceptCancel: acceptCancel, done: done, stats: stats}
			mu.Lock()
			ctrls = append(ctrls, ctrl)
			n := len(ctrls)
			mu.Unlock()

			handler := func(ctx context.Context, call *ServerSeqCall) error {
				if n == 1 {
					// 长调用连接
					close(longEntered)
					<-longRelease
					longFinished.Store(true)
					return nil
				}
				return echoHandler(ctx, call)
			}
			go func(c net.Conn) {
				defer close(done)
				RunAcceptLoop(connCtx, acceptCtx, f, c, stats, handler)
			}(c)
		}
	}()

	// 连接 1：长调用
	c1, err := dialLocal(ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Close()
	if err := clientOpen(c1, 1, "long"); err != nil {
		t.Fatal(err)
	}
	if err := clientData(c1, 1, []byte("hold")); err != nil {
		t.Fatal(err)
	}
	if err := clientEnd(c1, 1); err != nil {
		t.Fatal(err)
	}
	select {
	case <-longEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("long handler not entered")
	}

	// 连接 2：跑完一次后空闲
	c2, err := dialLocal(ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	var buf []byte
	if _, _, err := unaryCall(c2, &buf, 1, "echo.Echo", []byte("idle")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)

	mu.Lock()
	if len(ctrls) < 2 {
		mu.Unlock()
		t.Fatalf("want 2 conns, got %d", len(ctrls))
	}
	longCtrl, idleCtrl := ctrls[0], ctrls[1]
	longOnConnDone, idleOnConnDone = longCtrl.done, idleCtrl.done
	mu.Unlock()

	// Shutdown：取消两条的 accept ctx
	start := time.Now()
	longCtrl.acceptCancel()
	idleCtrl.acceptCancel()

	select {
	case <-idleOnConnDone:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("idle onConn did not exit quickly")
	}
	idleElapsed := time.Since(start)
	if idleElapsed > 500*time.Millisecond {
		t.Fatalf("idle exit too slow: %v", idleElapsed)
	}

	// 长调用仍在途
	select {
	case <-longOnConnDone:
		if !longFinished.Load() {
			t.Fatal("long onConn exited before handler finished")
		}
	default:
	}
	if longFinished.Load() {
		// 尚未 release，不应完成
		t.Fatal("long call finished before release")
	}

	close(longRelease)
	select {
	case <-longOnConnDone:
	case <-time.After(2 * time.Second):
		t.Fatal("long onConn did not exit after handler finished")
	}
	if !longFinished.Load() {
		t.Fatal("long call did not finish")
	}
	t.Logf("step4c: idle woke in %v; long ran to completion independently", idleElapsed)
}

// ---------------------------------------------------------------------------
// 步骤 5：握手永不返回 → HandshakeTimeout 关连接；handler 次数 0
// ---------------------------------------------------------------------------

func TestHandshakeTimeoutClosesConn(t *testing.T) {
	f := NewSeqFraming()
	f.HandshakeTimeout = 100 * time.Millisecond
	f.Handshake = func(ctx context.Context, c net.Conn) error {
		<-ctx.Done()
		return ctx.Err()
	}
	s := startAcceptServer(t, f, echoHandler)

	c := dialAcceptServer(t, s)
	defer c.Close()

	select {
	case <-s.firstDone:
	case <-time.After(2 * time.Second):
		t.Fatal("onConn did not exit after handshake timeout")
	}
	if s.stats.HandlerCount.Load() != 0 {
		t.Fatalf("HandlerCount=%d want 0", s.stats.HandlerCount.Load())
	}
	if s.stats.HandshakeFail.Load() != 1 {
		t.Fatalf("HandshakeFail=%d want 1", s.stats.HandshakeFail.Load())
	}
}

// ---------------------------------------------------------------------------
// 步骤 5b：握手完成后空闲超过 HandshakeTimeout，连接仍可用
// ---------------------------------------------------------------------------

func TestConnLifetimeNotBoundByHandshakeTimeout(t *testing.T) {
	f := NewSeqFraming()
	f.HandshakeTimeout = 80 * time.Millisecond
	f.MaxInboundConnIdle = 5 * time.Second
	s := startAcceptServer(t, f, echoHandler)

	c := dialAcceptServer(t, s)
	defer c.Close()

	var buf []byte
	if _, _, err := unaryCall(c, &buf, 1, "echo.Echo", []byte("a")); err != nil {
		t.Fatal(err)
	}
	// 空闲超过 HandshakeTimeout
	time.Sleep(200 * time.Millisecond)
	code, _, err := unaryCall(c, &buf, 2, "echo.Echo", []byte("b"))
	if err != nil {
		t.Fatalf("call after idle past HandshakeTimeout: %v (timeout wrongly on conn ctx?)", err)
	}
	if code != 0 {
		t.Fatalf("status=%d", code)
	}
}

// ---------------------------------------------------------------------------
// 步骤 5c：空闲与 slowloris
// ---------------------------------------------------------------------------

func TestIdleAndSlowloris(t *testing.T) {
	t.Run("a_idle_closes", func(t *testing.T) {
		f := NewSeqFraming()
		f.MaxInboundConnIdle = 80 * time.Millisecond
		f.OpenTimeout = 5 * time.Second
		s := startAcceptServer(t, f, echoHandler)

		c := dialAcceptServer(t, s)
		defer c.Close()
		// 握手后不发任何字节
		select {
		case <-s.firstDone:
		case <-time.After(2 * time.Second):
			t.Fatal("MaxInboundConnIdle did not close idle conn")
		}
	})

	t.Run("b_half_open_opentimeout", func(t *testing.T) {
		f := NewSeqFraming()
		f.MaxInboundConnIdle = 5 * time.Second
		f.OpenTimeout = 80 * time.Millisecond
		s := startAcceptServer(t, f, echoHandler)

		c := dialAcceptServer(t, s)
		defer c.Close()
		// 半个 OPEN：只写长度头的 2 字节
		if _, err := c.Write([]byte{0, 0}); err != nil {
			t.Fatal(err)
		}
		select {
		case <-s.firstDone:
		case <-time.After(2 * time.Second):
			t.Fatal("OpenTimeout did not close half-OPEN slowloris")
		}
	})

	t.Run("c_idle_past_opentimeout_alive", func(t *testing.T) {
		f := NewSeqFraming()
		f.MaxInboundConnIdle = 5 * time.Second
		f.OpenTimeout = 80 * time.Millisecond
		s := startAcceptServer(t, f, echoHandler)

		c := dialAcceptServer(t, s)
		defer c.Close()

		// 完全空闲超过 OpenTimeout，连接应仍存活
		time.Sleep(200 * time.Millisecond)
		select {
		case <-s.firstDone:
			t.Fatal("fully idle conn killed by OpenTimeout; timeout must start at first byte")
		default:
		}
		var buf []byte
		code, _, err := unaryCall(c, &buf, 1, "echo.Echo", []byte("still"))
		if err != nil {
			t.Fatalf("call after idle past OpenTimeout: %v", err)
		}
		if code != 0 {
			t.Fatalf("status=%d", code)
		}
	})
}

// ---------------------------------------------------------------------------
// 步骤 5d：残余帧跳过；超过 MaxDrainBytes → 连接级错误
// ---------------------------------------------------------------------------

func TestResidualFrameSkip(t *testing.T) {
	t.Run("skip_and_next_call", func(t *testing.T) {
		var methods []string
		var mu sync.Mutex
		s := startAcceptServer(t, NewSeqFraming(), func(ctx context.Context, call *ServerSeqCall) error {
			mu.Lock()
			methods = append(methods, call.Method())
			mu.Unlock()
			// client-streaming：读一条就返回，留下残余
			_, err := call.Recv(ctx)
			if err != nil && err != io.EOF {
				return err
			}
			return nil
		})

		c := dialAcceptServer(t, s)
		defer c.Close()

		var buf []byte
		// call 1：OPEN + DATA + DATA + END（handler 只读第一条 DATA）
		if err := clientOpen(c, 1, "m1"); err != nil {
			t.Fatal(err)
		}
		if err := clientData(c, 1, []byte("d1")); err != nil {
			t.Fatal(err)
		}
		if err := clientData(c, 1, []byte("d2-residual")); err != nil {
			t.Fatal(err)
		}
		if err := clientEnd(c, 1); err != nil {
			t.Fatal(err)
		}
		id, code, _, err := clientReadStatus(c, &buf)
		if err != nil || id != 1 || code != 0 {
			t.Fatalf("call1 status id=%d code=%d err=%v", id, code, err)
		}

		// call 2：应被正确 Accept
		if err := clientOpen(c, 2, "m2"); err != nil {
			t.Fatal(err)
		}
		if err := clientData(c, 2, []byte("ok")); err != nil {
			t.Fatal(err)
		}
		if err := clientEnd(c, 2); err != nil {
			t.Fatal(err)
		}
		id, code, _, err = clientReadStatus(c, &buf)
		if err != nil || id != 2 || code != 0 {
			t.Fatalf("call2 status id=%d code=%d err=%v", id, code, err)
		}

		mu.Lock()
		defer mu.Unlock()
		if len(methods) != 2 || methods[0] != "m1" || methods[1] != "m2" {
			t.Fatalf("methods=%v want [m1 m2]", methods)
		}
	})

	t.Run("drain_exceeded", func(t *testing.T) {
		f := NewSeqFraming()
		f.MaxDrainBytes = 64 // 很小，易超限
		s := startAcceptServer(t, f, func(ctx context.Context, call *ServerSeqCall) error {
			_, _ = call.Recv(ctx)
			return nil
		})

		c := dialAcceptServer(t, s)
		defer c.Close()

		var buf []byte
		if err := clientOpen(c, 1, "m"); err != nil {
			t.Fatal(err)
		}
		if err := clientData(c, 1, []byte("one")); err != nil {
			t.Fatal(err)
		}
		// 大量残余 DATA
		big := make([]byte, 40)
		for i := 0; i < 10; i++ {
			if err := clientData(c, 1, big); err != nil {
				t.Fatal(err)
			}
		}
		if err := clientEnd(c, 1); err != nil {
			t.Fatal(err)
		}
		_, _, _, _ = clientReadStatus(c, &buf)

		// 下一轮 AcceptCall 排空时应超限并关掉连接
		select {
		case <-s.firstDone:
		case <-time.After(2 * time.Second):
			t.Fatal("expected connection-level error after MaxDrainBytes")
		}
		if s.stats.HandlerCount.Load() != 1 {
			t.Fatalf("HandlerCount=%d want 1", s.stats.HandlerCount.Load())
		}
	})
}

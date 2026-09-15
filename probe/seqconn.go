// Package probe — Task 0.13：顺序复用借还的最小探针。
//
// 一条 TCP 连接 + 4 字节大端长度前缀分帧；会话池按引用计数记账，
// Sequential 承载力为 1。Session 持有跨调用读缓冲与唯一接收 goroutine
// 的起停权；Call.Close 同步 join 接收 goroutine，且不关闭 Conn。
package probe

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// 会话池 / Framing 哨兵（探针本地；正式码在 status 包）。
var (
	ErrSessionBusy       = errors.New("session busy")
	ErrSessionsExhausted = errors.New("sessions exhausted")
)

// resourceExhaustedError 模拟 status.ResourceExhausted 包装本地哨兵。
type resourceExhaustedError struct {
	err error
}

func (e *resourceExhaustedError) Error() string {
	return "ResourceExhausted: " + e.err.Error()
}

func (e *resourceExhaustedError) Unwrap() error { return e.err }

// ResourceExhausted 返回可 errors.Is 到内嵌哨兵的 ResourceExhausted。
func ResourceExhausted(err error) error {
	return &resourceExhaustedError{err: err}
}

// ---------------------------------------------------------------------------
// 长度前缀分帧
// ---------------------------------------------------------------------------

func writeFrame(w io.Writer, payload []byte) error {
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(payload)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	if len(payload) == 0 {
		return nil
	}
	_, err := w.Write(payload)
	return err
}

// readFrameFrom 先消耗 buf 中的跨调用残留，不够再从 r 读；返回帧与新 buf。
func readFrameFrom(r io.Reader, buf []byte) (payload []byte, rest []byte, err error) {
	need := func(n int) error {
		for len(buf) < n {
			tmp := make([]byte, 4096)
			nr, e := r.Read(tmp)
			if nr > 0 {
				buf = append(buf, tmp[:nr]...)
			}
			if e != nil {
				if len(buf) >= n {
					return nil
				}
				if e == io.EOF && len(buf) == 0 {
					return io.EOF
				}
				if e == io.EOF {
					return io.ErrUnexpectedEOF
				}
				return e
			}
		}
		return nil
	}
	if err := need(4); err != nil {
		return nil, buf, err
	}
	n := int(binary.BigEndian.Uint32(buf[:4]))
	buf = buf[4:]
	if err := need(n); err != nil {
		// 把头放回，避免半帧丢字节（故障路径会丢弃整会话）。
		hdr := make([]byte, 4)
		binary.BigEndian.PutUint32(hdr, uint32(n))
		buf = append(hdr, buf...)
		return nil, buf, err
	}
	payload = make([]byte, n)
	copy(payload, buf[:n])
	rest = buf[n:]
	if len(rest) == 0 {
		rest = nil
	} else {
		// 拷贝，避免底层大切片别名。
		cp := make([]byte, len(rest))
		copy(cp, rest)
		rest = cp
	}
	return payload, rest, nil
}

// ---------------------------------------------------------------------------
// Conn 包装：关闭探测 / Read 重入探测
// ---------------------------------------------------------------------------

// closeFlagConn 在 Close 时置位，用于断言 Call.Close 不关 Conn。
type closeFlagConn struct {
	net.Conn
	closed atomic.Bool
}

func (c *closeFlagConn) Close() error {
	c.closed.Store(true)
	return c.Conn.Close()
}

func (c *closeFlagConn) Closed() bool { return c.closed.Load() }

// reentryConn 检测并发 Read 重入（-race 抓不到两边都合法的并发 Read）。
type reentryConn struct {
	net.Conn
	reading atomic.Int32
	hits    atomic.Int64
}

func (c *reentryConn) Read(p []byte) (int, error) {
	if c.reading.Add(1) != 1 {
		c.hits.Add(1)
	}
	defer c.reading.Add(-1)
	return c.Conn.Read(p)
}

func (c *reentryConn) ConcurrentHits() int64 { return c.hits.Load() }

// scriptedReader 按脚本返回预置分片，用于跨调用读缓冲（5d）。
type scriptedConn struct {
	net.Conn
	mu    sync.Mutex
	reads [][]byte // 依次返回的 Read 结果；耗尽后走底层 Conn
	err   error    // 脚本耗尽且无底层时的错误
}

func (c *scriptedConn) Read(p []byte) (int, error) {
	c.mu.Lock()
	if len(c.reads) > 0 {
		chunk := c.reads[0]
		c.reads = c.reads[1:]
		c.mu.Unlock()
		n := copy(p, chunk)
		return n, nil
	}
	c.mu.Unlock()
	if c.Conn != nil {
		return c.Conn.Read(p)
	}
	if c.err != nil {
		return 0, c.err
	}
	return 0, io.EOF
}

func (c *scriptedConn) Write(p []byte) (int, error) { return len(p), nil }

func (c *scriptedConn) Close() error {
	if c.Conn != nil {
		return c.Conn.Close()
	}
	return nil
}

func (c *scriptedConn) LocalAddr() net.Addr                { return dummyAddr{} }
func (c *scriptedConn) RemoteAddr() net.Addr               { return dummyAddr{} }
func (c *scriptedConn) SetDeadline(t time.Time) error      { return nil }
func (c *scriptedConn) SetReadDeadline(t time.Time) error  { return nil }
func (c *scriptedConn) SetWriteDeadline(t time.Time) error { return nil }

type dummyAddr struct{}

func (dummyAddr) Network() string { return "script" }
func (dummyAddr) String() string  { return "script" }

// ---------------------------------------------------------------------------
// Sequential Session / Call
// ---------------------------------------------------------------------------

// SeqFraming 声明 Sequential 复用；握手可注入。
type SeqFraming struct {
	HandshakeTimeout time.Duration
	// Handshake 若非 nil，在 NewClientSession 的握手子 ctx 上调用。
	Handshake func(ctx context.Context, c net.Conn) error
	// Hygiene 为 false 时关闭承载卫生（仅 5b 反证用）。
	Hygiene bool
	// OpenCallHook 可注入 ErrSessionBusy 等（仅 3b）。
	OpenCallHook func(sessionID int, callSeq int) error
}

func NewSeqFraming() *SeqFraming {
	return &SeqFraming{
		HandshakeTimeout: 10 * time.Second,
		Hygiene:          true,
	}
}

func (f *SeqFraming) Reuse() string { return "Sequential" }

// SeqSession 是一条顺序复用连接上的客户端会话。
type SeqSession struct {
	framing *SeqFraming
	conn    net.Conn
	id      int

	// connCtx 归属 Client 生命周期，不是调用 ctx 的子。
	connCtx    context.Context
	connCancel context.CancelFunc

	mu       sync.Mutex
	buf      []byte // 跨调用读缓冲
	reusable bool
	closed   bool
	inFlight bool // Sequential：同一时刻最多一个 Call
	callSeq  int  // OpenCall 次数（供 Busy hook）

	// 接收 goroutine 由 Session 起停；同时最多一个。
	recvWG sync.WaitGroup
}

func (f *SeqFraming) NewClientSession(parent context.Context, c net.Conn) (*SeqSession, error) {
	// 连接 ctx ⊂ Client（此处用 parent 作为 Client 生命周期），不挂 HandshakeTimeout。
	connCtx, connCancel := context.WithCancel(parent)
	s := &SeqSession{
		framing:    f,
		conn:       c,
		connCtx:    connCtx,
		connCancel: connCancel,
		reusable:   true,
	}
	to := f.HandshakeTimeout
	if to <= 0 {
		to = 10 * time.Second
	}
	hsCtx, hsCancel := context.WithTimeout(connCtx, to)
	defer hsCancel()
	if f.Handshake != nil {
		if err := f.Handshake(hsCtx, c); err != nil {
			connCancel()
			return nil, err
		}
	} else {
		// 无握手协议：仅确认握手子 ctx 仍有效。
		select {
		case <-hsCtx.Done():
			connCancel()
			return nil, hsCtx.Err()
		default:
		}
	}
	return s, nil
}

func (s *SeqSession) ConnCtx() context.Context { return s.connCtx }

func (s *SeqSession) Reusable() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reusable && !s.closed
}

func (s *SeqSession) markBad() {
	s.mu.Lock()
	s.reusable = false
	s.mu.Unlock()
}

func (s *SeqSession) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.reusable = false
	s.mu.Unlock()
	s.connCancel()
	return s.conn.Close()
}

// OpenCall 在本会话上开一次调用。Sequential：已有在途则返回 ErrSessionBusy（兜底）。
func (s *SeqSession) OpenCall(ctx context.Context) (*SeqCall, error) {
	s.mu.Lock()
	if s.closed || !s.reusable {
		s.mu.Unlock()
		return nil, fmt.Errorf("session not reusable")
	}
	if s.inFlight {
		s.mu.Unlock()
		return nil, ErrSessionBusy
	}
	s.callSeq++
	seq := s.callSeq
	hook := s.framing.OpenCallHook
	s.mu.Unlock()

	if hook != nil {
		if err := hook(s.id, seq); err != nil {
			return nil, err
		}
	}

	s.mu.Lock()
	if s.inFlight {
		s.mu.Unlock()
		return nil, ErrSessionBusy
	}
	s.inFlight = true
	s.mu.Unlock()

	call := &SeqCall{
		sess:    s,
		ctx:     ctx,
		inbox:   make(chan recvItem, 8),
		hygiene: s.framing.Hygiene,
	}
	call.startRecv()
	return call, nil
}

type recvItem struct {
	payload []byte
	err     error
}

// SeqCall 一次顺序调用。空帧（len=0）为接收方向终态。
type SeqCall struct {
	sess    *SeqSession
	ctx     context.Context
	inbox   chan recvItem
	hygiene bool

	mu        sync.Mutex
	terminal  bool // 已读到终态（空帧或对端 EOF 作为正常结束）
	closed    bool
	recvDone  chan struct{}
	stopRecv  context.CancelFunc
}

func (c *SeqCall) startRecv() {
	ctx, cancel := context.WithCancel(context.Background())
	c.stopRecv = cancel
	c.recvDone = make(chan struct{})
	c.sess.recvWG.Add(1)
	go func() {
		defer c.sess.recvWG.Done()
		defer close(c.recvDone)
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			c.sess.mu.Lock()
			buf := c.sess.buf
			c.sess.buf = nil
			conn := c.sess.conn
			c.sess.mu.Unlock()

			payload, rest, err := readFrameFrom(conn, buf)
			c.sess.mu.Lock()
			// 无论成败，rest 归 Session（跨调用缓冲）。
			if len(rest) > 0 {
				c.sess.buf = append(c.sess.buf, rest...)
			} else if err == nil {
				// 读干净时保持 buf 为 rest（可能 nil）。
				if c.sess.buf == nil {
					c.sess.buf = rest
				}
			}
			c.sess.mu.Unlock()

			if err != nil {
				// Close 用 deadline 叫醒 Read 时不把取消当连接故障。
				select {
				case <-ctx.Done():
					return
				default:
				}
				select {
				case c.inbox <- recvItem{err: err}:
				case <-ctx.Done():
				}
				return
			}
			if len(payload) == 0 {
				// 终态空帧。
				select {
				case c.inbox <- recvItem{payload: payload}:
				case <-ctx.Done():
				}
				return
			}
			select {
			case c.inbox <- recvItem{payload: payload}:
			case <-ctx.Done():
				// 未投递的帧写回 Session 缓冲，避免丢失下一调用头部。
				c.sess.mu.Lock()
				frame := make([]byte, 4+len(payload))
				binary.BigEndian.PutUint32(frame[:4], uint32(len(payload)))
				copy(frame[4:], payload)
				c.sess.buf = append(frame, c.sess.buf...)
				c.sess.mu.Unlock()
				return
			}
		}
	}()
}

func (c *SeqCall) Send(payload []byte) error {
	c.sess.mu.Lock()
	conn := c.sess.conn
	c.sess.mu.Unlock()
	if err := writeFrame(conn, payload); err != nil {
		c.sess.markBad()
		return err
	}
	return nil
}

func (c *SeqCall) Recv() ([]byte, error) {
	select {
	case <-c.ctx.Done():
		return nil, c.ctx.Err()
	case it, ok := <-c.inbox:
		if !ok {
			return nil, io.EOF
		}
		if it.err != nil {
			if it.err != io.EOF {
				c.sess.markBad()
			}
			c.mu.Lock()
			if it.err == io.EOF {
				c.terminal = true
			}
			c.mu.Unlock()
			return nil, it.err
		}
		if len(it.payload) == 0 {
			c.mu.Lock()
			c.terminal = true
			c.mu.Unlock()
			return nil, io.EOF
		}
		return it.payload, nil
	}
}

// Close 同步 join 接收 goroutine；不关闭 Conn。
// 未读到终态且开启卫生时，Session.Reusable() → false。
func (c *SeqCall) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	terminal := c.terminal
	c.mu.Unlock()

	if c.stopRecv != nil {
		c.stopRecv()
	}
	// 解除可能堵在 Read 上的接收 goroutine（唤醒 ≠ 退出；下面必须 join）。
	_ = c.sess.conn.SetReadDeadline(time.Now())
	<-c.recvDone
	_ = c.sess.conn.SetReadDeadline(time.Time{})

	c.sess.mu.Lock()
	c.sess.inFlight = false
	if c.hygiene && !terminal {
		c.sess.reusable = false
	}
	c.sess.mu.Unlock()
	return nil
}

// PeekBuf 测试用：跨调用缓冲内容。
func (s *SeqSession) PeekBuf() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.buf) == 0 {
		return nil
	}
	out := make([]byte, len(s.buf))
	copy(out, s.buf)
	return out
}

// ---------------------------------------------------------------------------
// 会话池（引用计数）
// ---------------------------------------------------------------------------

// pooled 包装会话与引用计数。
type pooled struct {
	sess *SeqSession
	refs int
}

// SeqPool 按 endpoint 的最小会话池：借还不排队，Sequential 承载力 1。
type SeqPool struct {
	MaxSessions        int
	Framing            *SeqFraming
	Dial               func(ctx context.Context) (net.Conn, error)
	ClientCtx          context.Context // 连接 ctx 的父；默认 Background
	HandshakeTimeout   time.Duration
	DisableHygiene     bool // 仅测试

	mu       sync.Mutex
	sessions []*pooled
	nextID   int

	// 统计（FINDINGS）
	DialCount   int
	BorrowCount int
	ReturnCount int
	BusyRetry   int
}

func (p *SeqPool) framing() *SeqFraming {
	// 仅在首次借用时初始化；之后不再写 Framing 字段，避免与 OpenCall 并发读竞态。
	if p.Framing == nil {
		f := NewSeqFraming()
		if p.HandshakeTimeout > 0 {
			f.HandshakeTimeout = p.HandshakeTimeout
		}
		if p.DisableHygiene {
			f.Hygiene = false
		}
		p.Framing = f
	}
	return p.Framing
}

func (p *SeqPool) max() int {
	if p.MaxSessions <= 0 {
		return 64
	}
	return p.MaxSessions
}

func (p *SeqPool) clientCtx() context.Context {
	if p.ClientCtx != nil {
		return p.ClientCtx
	}
	return context.Background()
}

// Borrow 取一个 refs==0 且 Reusable 的会话，或新建；达上限立即 ResourceExhausted。
func (p *SeqPool) Borrow(ctx context.Context) (*SeqSession, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, e := range p.sessions {
		if e.refs == 0 && e.sess.Reusable() {
			e.refs++
			p.BorrowCount++
			return e.sess, nil
		}
	}
	if len(p.sessions) >= p.max() {
		return nil, ResourceExhausted(ErrSessionsExhausted)
	}
	// 在锁外拨号会竞态超限；探针规模小，锁内 dial 可接受。
	conn, err := p.Dial(ctx)
	if err != nil {
		return nil, err
	}
	f := p.framing()
	sess, err := f.NewClientSession(p.clientCtx(), conn)
	if err != nil {
		_ = conn.Close()
		return nil, err
	}
	p.nextID++
	sess.id = p.nextID
	p.sessions = append(p.sessions, &pooled{sess: sess, refs: 1})
	p.DialCount++
	p.BorrowCount++
	return sess, nil
}

// Return 归还；不可复用则关闭并从池中移除。
func (p *SeqPool) Return(sess *SeqSession) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.ReturnCount++
	for i, e := range p.sessions {
		if e.sess == sess {
			e.refs--
			if e.refs < 0 {
				e.refs = 0
			}
			if !sess.Reusable() {
				_ = sess.Close()
				p.sessions = append(p.sessions[:i], p.sessions[i+1:]...)
			}
			return
		}
	}
}

func (p *SeqPool) SessionCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.sessions)
}

func (p *SeqPool) LiveConns() int {
	return p.SessionCount()
}

// Open 借会话并 OpenCall；遇 ErrSessionBusy 换会话，重试 ≤ MaxSessions。
// 原始 Busy 哨兵绝不外泄。
func (p *SeqPool) Open(ctx context.Context) (*SeqCall, *SeqSession, error) {
	max := p.max()
	var lastBusy bool
	for attempt := 0; attempt < max; attempt++ {
		sess, err := p.Borrow(ctx)
		if err != nil {
			// Borrow 的 ResourceExhausted 原样返回。
			if lastBusy && errors.Is(err, ErrSessionsExhausted) {
				return nil, nil, err
			}
			return nil, nil, err
		}
		call, err := sess.OpenCall(ctx)
		if err == nil {
			return call, sess, nil
		}
		if errors.Is(err, ErrSessionBusy) {
			lastBusy = true
			p.BusyRetry++
			// 释放引用但不因 Busy 关连接：refs--，会话仍在池中。
			p.returnKeep(sess)
			continue
		}
		// 其他错误：归还（可能 markBad）。
		sess.markBad()
		p.Return(sess)
		return nil, nil, err
	}
	return nil, nil, ResourceExhausted(ErrSessionsExhausted)
}

func (p *SeqPool) returnKeep(sess *SeqSession) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.ReturnCount++
	for _, e := range p.sessions {
		if e.sess == sess {
			e.refs--
			if e.refs < 0 {
				e.refs = 0
			}
			return
		}
	}
}

// CloseCall 关闭调用并归还会话。
func (p *SeqPool) CloseCall(call *SeqCall, sess *SeqSession) {
	_ = call.Close()
	p.Return(sess)
}

// Close 关闭池中全部会话。
func (p *SeqPool) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, e := range p.sessions {
		_ = e.sess.Close()
	}
	p.sessions = nil
}

// ---------------------------------------------------------------------------
// 测试用 echo 服务端
// ---------------------------------------------------------------------------

// SeqServerOpt 控制 echo 服务行为。
type SeqServerOpt struct {
	// MultiResponseFor 若匹配请求 payload，则先发该 payload 再发 "EXTRA" 再发终态空帧。
	MultiResponseFor string
	// DropAfter 在处理完该序号（1-based）的请求后关闭连接（读完请求、写一半响应时断开）。
	DropOnCall int
	// DropMidWrite 为 true 时写响应中途断开。
	DropMidWrite bool
	// SlowTailAfter 在匹配请求后先写一帧，再故意拖延写剩余（配合客户端提前 Close）。
	SlowTailFor string
	SlowTail    time.Duration
}

// ServeSeqEcho 在 ln 上接受一条连接并顺序 echo；每请求一帧，响应为 payload 回显 + 空终态帧。
func ServeSeqEcho(ln net.Listener, opt SeqServerOpt) error {
	c, err := ln.Accept()
	if err != nil {
		return err
	}
	defer c.Close()
	return serveSeqConn(c, opt)
}

func serveSeqConn(c net.Conn, opt SeqServerOpt) error {
	var buf []byte
	callNo := 0
	for {
		payload, rest, err := readFrameFrom(c, buf)
		buf = rest
		if err != nil {
			if err == io.EOF || errors.Is(err, net.ErrClosed) {
				return nil
			}
			return err
		}
		callNo++
		if opt.DropOnCall > 0 && callNo == opt.DropOnCall {
			if opt.DropMidWrite {
				// 写半个长度头后断开。
				_, _ = c.Write([]byte{0, 0})
			}
			_ = c.Close()
			return nil
		}
		body := string(payload)
		if opt.MultiResponseFor != "" && body == opt.MultiResponseFor {
			if err := writeFrame(c, payload); err != nil {
				return err
			}
			if opt.SlowTailFor == body && opt.SlowTail > 0 {
				time.Sleep(opt.SlowTail)
			}
			if err := writeFrame(c, []byte("EXTRA")); err != nil {
				return err
			}
			if err := writeFrame(c, nil); err != nil {
				return err
			}
			continue
		}
		if err := writeFrame(c, payload); err != nil {
			return err
		}
		if err := writeFrame(c, nil); err != nil { // 终态
			return err
		}
	}
}

// StartSeqServer 起 loopback 服务，每条入站连接一个 echo 循环。
func StartSeqServer(opt SeqServerOpt) (addr string, closeFn func(), err error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", nil, err
	}
	var wg sync.WaitGroup
	done := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			c, err := ln.Accept()
			if err != nil {
				select {
				case <-done:
					return
				default:
					return
				}
			}
			go func(c net.Conn) {
				_ = serveSeqConn(c, opt)
			}(c)
		}
	}()
	closeFn = func() {
		close(done)
		_ = ln.Close()
		wg.Wait()
	}
	return ln.Addr().String(), closeFn, nil
}

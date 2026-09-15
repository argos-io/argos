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
	// ErrCallRejected：AcceptCall 专用——本次调用非法但连接仍可用。
	ErrCallRejected = errors.New("call rejected")
	// ErrDrainExceeded：残余帧丢弃超过 MaxDrainBytes，连接级错误。
	ErrDrainExceeded = errors.New("max drain bytes exceeded")
	// ErrInboundIdle：MaxInboundConnIdle 到期。
	ErrInboundIdle = errors.New("inbound connection idle timeout")
	// ErrOpenTimeout：读到首字节后 OpenTimeout 到期。
	ErrOpenTimeout = errors.New("open timeout")
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
	// OpenTimeout：服务端 AcceptCall 从本次调用首字节起算（§2.1 义务 b）。
	OpenTimeout time.Duration
	// MaxInboundConnIdle：两次调用之间的空闲上限；必须为正（测试可缩短）。
	MaxInboundConnIdle time.Duration
	// MaxDrainBytes：AcceptCall 丢弃已结束调用残余帧的上限。
	MaxDrainBytes int
	// Handshake 若非 nil，在 NewClientSession / NewServerSession 的握手子 ctx 上调用。
	Handshake func(ctx context.Context, c net.Conn) error
	// Hygiene 为 false 时关闭承载卫生（仅 5b 反证用）。
	Hygiene bool
	// OpenCallHook 可注入 ErrSessionBusy 等（仅 3b）。
	OpenCallHook func(sessionID int, callSeq int) error
}

func NewSeqFraming() *SeqFraming {
	return &SeqFraming{
		HandshakeTimeout:   10 * time.Second,
		OpenTimeout:        10 * time.Second,
		MaxInboundConnIdle: 50 * time.Second,
		MaxDrainBytes:      1 << 20,
		Hygiene:            true,
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

// ---------------------------------------------------------------------------
// 服务端 envelope-lite：长度前缀 + type + callID + payload（Task 0.14）
// ---------------------------------------------------------------------------

// seqFrame* 与 udp_test 的 frame* 常量区分（探针包内多协议并存）。
const (
	seqFrameOpen   byte = 1
	seqFrameData   byte = 2
	seqFrameEnd    byte = 3
	seqFrameStatus byte = 4
)

func encodeEnvFrame(typ byte, callID uint32, payload []byte) []byte {
	body := make([]byte, 1+4+len(payload))
	body[0] = typ
	binary.BigEndian.PutUint32(body[1:5], callID)
	copy(body[5:], payload)
	return body
}

func writeEnvFrame(w io.Writer, typ byte, callID uint32, payload []byte) error {
	return writeFrame(w, encodeEnvFrame(typ, callID, payload))
}

func parseEnvFrame(raw []byte) (typ byte, callID uint32, payload []byte, err error) {
	if len(raw) < 5 {
		return 0, 0, nil, io.ErrUnexpectedEOF
	}
	typ = raw[0]
	callID = binary.BigEndian.Uint32(raw[1:5])
	payload = raw[5:]
	return typ, callID, payload, nil
}

func writeStatusFrame(w io.Writer, callID uint32, code uint32, msg string) error {
	p := make([]byte, 4+len(msg))
	binary.BigEndian.PutUint32(p[:4], code)
	copy(p[4:], msg)
	return writeEnvFrame(w, seqFrameStatus, callID, p)
}

func parseStatusPayload(p []byte) (code uint32, msg string, err error) {
	if len(p) < 4 {
		return 0, "", io.ErrUnexpectedEOF
	}
	return binary.BigEndian.Uint32(p[:4]), string(p[4:]), nil
}

// ServerSeqSession 是一条顺序复用连接上的服务端会话。
type ServerSeqSession struct {
	framing *SeqFraming
	conn    net.Conn

	connCtx    context.Context
	connCancel context.CancelFunc

	mu         sync.Mutex
	buf        []byte
	closed     bool
	closeCount int
	lastCallID uint32 // 上一次已结束调用的 ID，用于有界丢弃
	needDrain  bool
}

// NewServerSession 在连接 ctx 下完成握手；HandshakeTimeout 只挂在握手子 ctx。
func (f *SeqFraming) NewServerSession(parent context.Context, c net.Conn) (*ServerSeqSession, error) {
	connCtx, connCancel := context.WithCancel(parent)
	s := &ServerSeqSession{
		framing:    f,
		conn:       c,
		connCtx:    connCtx,
		connCancel: connCancel,
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
		select {
		case <-hsCtx.Done():
			connCancel()
			return nil, hsCtx.Err()
		default:
		}
	}
	return s, nil
}

func (s *ServerSeqSession) ConnCtx() context.Context { return s.connCtx }

func (s *ServerSeqSession) CloseCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closeCount
}

func (s *ServerSeqSession) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.closeCount++
	s.mu.Unlock()
	s.connCancel()
	return s.conn.Close()
}

func (s *ServerSeqSession) openTimeout() time.Duration {
	if s.framing.OpenTimeout > 0 {
		return s.framing.OpenTimeout
	}
	return 10 * time.Second
}

func (s *ServerSeqSession) idleTimeout() time.Duration {
	if s.framing.MaxInboundConnIdle > 0 {
		return s.framing.MaxInboundConnIdle
	}
	return 50 * time.Second
}

func (s *ServerSeqSession) maxDrain() int {
	if s.framing.MaxDrainBytes > 0 {
		return s.framing.MaxDrainBytes
	}
	return 1 << 20
}

// wakeOnCancel 在 ctx 取消时用读 deadline 叫醒阻塞中的 Read（AcceptCall / Shutdown）。
func (s *ServerSeqSession) wakeOnCancel(ctx context.Context) (stop func()) {
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = s.conn.SetReadDeadline(time.Now())
		case <-done:
		}
	}()
	return func() {
		close(done)
		_ = s.conn.SetReadDeadline(time.Time{})
	}
}

// readRawFrame 读一帧。
//   idle>0：无缓冲时先按空闲超时等首字节，见到字节后改用 open；
//   idle==0 && open>0：整段使用 open；
//   两者皆 0：无超时，仅受 ctx 取消叫醒（在途调用 Recv）。
// acceptCancelAsEOF：ctx 取消映射为 io.EOF（AcceptCall / Shutdown）；否则返回 ctx.Err()。
func (s *ServerSeqSession) readRawFrame(ctx context.Context, idle, open time.Duration, acceptCancelAsEOF bool) (raw []byte, err error) {
	stop := s.wakeOnCancel(ctx)
	defer stop()

	s.mu.Lock()
	buf := s.buf
	s.buf = nil
	conn := s.conn
	s.mu.Unlock()

	waitingFirst := idle > 0 && len(buf) == 0
	if waitingFirst {
		_ = conn.SetReadDeadline(time.Now().Add(idle))
	} else if open > 0 {
		_ = conn.SetReadDeadline(time.Now().Add(open))
	} else {
		_ = conn.SetReadDeadline(time.Time{})
	}

	sawNetwork := false
	need := func(n int) error {
		for len(buf) < n {
			tmp := make([]byte, 4096)
			nr, e := conn.Read(tmp)
			if nr > 0 {
				sawNetwork = true
				if waitingFirst {
					waitingFirst = false
					if open > 0 {
						_ = conn.SetReadDeadline(time.Now().Add(open))
					} else {
						_ = conn.SetReadDeadline(time.Time{})
					}
				}
				buf = append(buf, tmp[:nr]...)
			}
			if e != nil {
				if len(buf) >= n {
					return nil
				}
				if ctx.Err() != nil {
					if acceptCancelAsEOF {
						return io.EOF
					}
					return ctx.Err()
				}
				if ne, ok := e.(net.Error); ok && ne.Timeout() {
					if idle > 0 && !sawNetwork && len(buf) == 0 {
						return ErrInboundIdle
					}
					if open > 0 {
						return ErrOpenTimeout
					}
					return e
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
		s.mu.Lock()
		s.buf = buf
		s.mu.Unlock()
		return nil, err
	}
	n := int(binary.BigEndian.Uint32(buf[:4]))
	buf = buf[4:]
	if err := need(n); err != nil {
		hdr := make([]byte, 4)
		binary.BigEndian.PutUint32(hdr, uint32(n))
		s.mu.Lock()
		s.buf = append(hdr, buf...)
		s.mu.Unlock()
		return nil, err
	}
	raw = make([]byte, n)
	copy(raw, buf[:n])
	rest := buf[n:]
	s.mu.Lock()
	if len(rest) > 0 {
		cp := make([]byte, len(rest))
		copy(cp, rest)
		s.buf = cp
	} else {
		s.buf = nil
	}
	s.mu.Unlock()
	_ = conn.SetReadDeadline(time.Time{})
	return raw, nil
}

// AcceptCall 取下一个调用。acceptCtx 取消 → io.EOF（优雅关闭叫醒）。
func (s *ServerSeqSession) AcceptCall(acceptCtx context.Context) (*ServerSeqCall, error) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, io.EOF
	}
	drainID := s.lastCallID
	needDrain := s.needDrain
	s.mu.Unlock()

	drained := 0
	idle := s.idleTimeout()
	open := s.openTimeout()
	seenByte := false

	for {
		if err := acceptCtx.Err(); err != nil {
			return nil, io.EOF
		}
		var (
			raw []byte
			err error
		)
		if !seenByte {
			raw, err = s.readRawFrame(acceptCtx, idle, open, true)
		} else {
			raw, err = s.readRawFrame(acceptCtx, 0, open, true)
		}
		if err != nil {
			return nil, err
		}
		seenByte = true

		typ, callID, payload, err := parseEnvFrame(raw)
		if err != nil {
			return nil, err
		}

		if needDrain && callID == drainID {
			frameBytes := 4 + len(raw)
			drained += frameBytes
			if drained > s.maxDrain() {
				return nil, ErrDrainExceeded
			}
			continue
		}

		if typ != seqFrameOpen {
			return nil, fmt.Errorf("expected OPEN, got type=%d callID=%d", typ, callID)
		}

		s.mu.Lock()
		s.needDrain = false
		s.lastCallID = callID
		s.mu.Unlock()

		return &ServerSeqCall{
			sess:   s,
			callID: callID,
			method: string(payload),
		}, nil
	}
}

// ServerSeqCall 一次入站调用（顺序 Recv，无独立读 goroutine，避免吃掉残余帧）。
type ServerSeqCall struct {
	sess   *ServerSeqSession
	callID uint32
	method string

	mu       sync.Mutex
	finished bool
	closed   bool
	sawEnd   bool
}

func (c *ServerSeqCall) Method() string { return c.method }
func (c *ServerSeqCall) CallID() uint32 { return c.callID }

// Recv 读一条 DATA；END → io.EOF。在途调用不受 OpenTimeout / Idle 约束。
func (c *ServerSeqCall) Recv(ctx context.Context) ([]byte, error) {
	c.mu.Lock()
	if c.closed || c.sawEnd {
		c.mu.Unlock()
		return nil, io.EOF
	}
	c.mu.Unlock()

	raw, err := c.sess.readRawFrame(ctx, 0, 0, false)
	if err != nil {
		return nil, err
	}
	typ, callID, payload, err := parseEnvFrame(raw)
	if err != nil {
		return nil, err
	}
	if callID != c.callID {
		// 下一调用帧：写回 Session，本调用视为结束。
		c.sess.mu.Lock()
		frame := make([]byte, 4+len(raw))
		binary.BigEndian.PutUint32(frame[:4], uint32(len(raw)))
		copy(frame[4:], raw)
		c.sess.buf = append(frame, c.sess.buf...)
		c.sess.mu.Unlock()
		c.mu.Lock()
		c.sawEnd = true
		c.mu.Unlock()
		return nil, io.EOF
	}
	switch typ {
	case seqFrameData:
		return payload, nil
	case seqFrameEnd:
		c.mu.Lock()
		c.sawEnd = true
		c.mu.Unlock()
		return nil, io.EOF
	default:
		return nil, fmt.Errorf("unexpected frame type %d in call", typ)
	}
}

// Finish 写 STATUS 结束响应方向。
func (c *ServerSeqCall) Finish(err error) error {
	c.mu.Lock()
	if c.finished {
		c.mu.Unlock()
		return nil
	}
	c.finished = true
	c.mu.Unlock()

	code := uint32(0)
	msg := ""
	if err != nil {
		code = 2 // Unknown（探针用）；对端用非零判定失败
		msg = err.Error()
	}
	return writeStatusFrame(c.sess.conn, c.callID, code, msg)
}

// Close 结束本次调用；标记需排空残余帧。不关闭 Conn。
func (c *ServerSeqCall) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.mu.Unlock()

	c.sess.mu.Lock()
	c.sess.lastCallID = c.callID
	c.sess.needDrain = true
	c.sess.mu.Unlock()
	return nil
}

// ---------------------------------------------------------------------------
// 组合层 onConn 骨架（探针）：握手 → AcceptCall 循环 → Session.Close 恰好一次
// ---------------------------------------------------------------------------

// AcceptLoopStats 供测试断言。
type AcceptLoopStats struct {
	OnConnID      atomic.Uint64
	HandlerCount  atomic.Int64
	HandlerOnConn atomic.Uint64 // 所有 handler 看到的 onConn 世代
	SessionCloses atomic.Int64
	HandshakeFail atomic.Int64
}

// RunAcceptLoop 模拟 server.onConn：acceptCtx 取消只停止接受，不影响在途调用 ctx。
func RunAcceptLoop(
	connCtx context.Context,
	acceptCtx context.Context,
	f *SeqFraming,
	c net.Conn,
	stats *AcceptLoopStats,
	handler func(ctx context.Context, call *ServerSeqCall) error,
) {
	id := stats.OnConnID.Add(1)
	sess, err := f.NewServerSession(connCtx, c)
	if err != nil {
		stats.HandshakeFail.Add(1)
		_ = c.Close()
		return
	}
	defer func() {
		_ = sess.Close()
		stats.SessionCloses.Add(1)
	}()

	for {
		call, err := sess.AcceptCall(acceptCtx)
		if err != nil {
			if errors.Is(err, ErrCallRejected) {
				continue
			}
			return
		}
		stats.HandlerCount.Add(1)
		stats.HandlerOnConn.Store(id)

		callCtx, cancel := context.WithCancel(connCtx)
		herr := handler(callCtx, call)
		_ = call.Finish(herr)
		_ = call.Close()
		cancel()
	}
}


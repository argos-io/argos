package probe

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"sync"
	"sync/atomic"
)

// §6.1 默认值：本探针只验证有界预读与额度峰值，不引入 Option 面。
const (
	ReadAheadMessages  = 1
	MaxMessageSize     = 4 << 20  // 4 MiB
	MaxConcurrentCalls = 64
	MaxBufferedBytes   = 1 << 30  // 1 GiB
)

// perCall 是默认配置下单调用额度上限（MaxBufferedBytes / MaxConcurrentCalls = 16 MiB）。
const perCall = MaxBufferedBytes / MaxConcurrentCalls

// ErrMessageTooLarge 表示 LPM 长度超过 MaxMessageSize；在分配缓冲之前返回。
var ErrMessageTooLarge = errors.New("probe: message exceeds MaxMessageSize")

// callBudget 跟踪单调用已计费字节与峰值（探针版，非生产 API）。
type callBudget struct {
	mu   sync.Mutex
	used int64
	peak int64
}

func (b *callBudget) charge(n int64) {
	if n == 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.used += n
	if b.used > b.peak {
		b.peak = b.used
	}
}

func (b *callBudget) release(n int64) {
	if n == 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.used -= n
	if b.used < 0 {
		b.used = 0
	}
}

func (b *callBudget) snapshot() (used, peak int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.used, b.peak
}

type queuedMsg struct {
	data []byte
	cost int64 // 按缓冲容量计费的字节数
}

// recvQueue 是 demand 驱动的有界接收队列：单个 goroutine 从 body 读 LPM，
// 推进容量为 ReadAheadMessages 的 channel；队列满时阻塞读，从而把背压传给 Carrier。
type recvQueue struct {
	ch     chan queuedMsg
	budget *callBudget
	done   chan struct{}

	err atomic.Pointer[error]
}

func startRecvQueue(ctx context.Context, body io.Reader, budget *callBudget) *recvQueue {
	q := &recvQueue{
		ch:     make(chan queuedMsg, ReadAheadMessages),
		budget: budget,
		done:   make(chan struct{}),
	}
	go q.loop(ctx, body)
	return q
}

func (q *recvQueue) setErr(err error) {
	if err == nil {
		return
	}
	q.err.CompareAndSwap(nil, &err)
}

func (q *recvQueue) getErr() error {
	if p := q.err.Load(); p != nil {
		return *p
	}
	return io.EOF
}

func (q *recvQueue) loop(ctx context.Context, body io.Reader) {
	defer close(q.done)

	for {
		if err := ctx.Err(); err != nil {
			q.setErr(err)
			return
		}

		var hdr [5]byte
		if _, err := io.ReadFull(body, hdr[:]); err != nil {
			if ctx.Err() != nil {
				q.setErr(ctx.Err())
			} else if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				// 正常流结束，或取消后 body 关闭导致的短读，都落成终态。
				if ctx.Err() != nil {
					q.setErr(ctx.Err())
				} else if errors.Is(err, io.EOF) {
					q.setErr(io.EOF)
				} else {
					q.setErr(err)
				}
			} else {
				q.setErr(err)
			}
			return
		}
		if hdr[0] != 0 {
			q.setErr(io.ErrUnexpectedEOF) // 探针不处理压缩
			return
		}
		n := binary.BigEndian.Uint32(hdr[1:])
		if n > MaxMessageSize {
			// 先拒后分：绝不分配超限缓冲。
			q.setErr(ErrMessageTooLarge)
			return
		}

		cost := int64(n)
		q.budget.charge(cost)
		buf := make([]byte, n)
		if _, err := io.ReadFull(body, buf); err != nil {
			q.budget.release(cost)
			if ctx.Err() != nil {
				q.setErr(ctx.Err())
			} else {
				q.setErr(err)
			}
			return
		}

		select {
		case q.ch <- queuedMsg{data: buf, cost: cost}:
		case <-ctx.Done():
			q.budget.release(cost)
			q.setErr(ctx.Err())
			return
		}
	}
}

// Recv 取出一条已预读消息并归还其额度（已交付载荷不再计入框架缓冲）。
func (q *recvQueue) Recv(ctx context.Context) ([]byte, error) {
	for {
		select {
		case msg := <-q.ch:
			q.budget.release(msg.cost)
			return msg.data, nil
		case <-q.done:
			select {
			case msg := <-q.ch:
				q.budget.release(msg.cost)
				return msg.data, nil
			default:
				return nil, q.getErr()
			}
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// Wait 阻塞到接收 goroutine 退出。
func (q *recvQueue) Wait() { <-q.done }

// Drain 归还仍停在队列里的额度（取消/关闭路径）。
func (q *recvQueue) Drain() {
	for {
		select {
		case msg := <-q.ch:
			q.budget.release(msg.cost)
		default:
			return
		}
	}
}

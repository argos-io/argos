// Package probe 是里程碑 ⓪ 的一次性探针，用来在冻结公开契约之前验证
// README §2.4、§4.2 里那些"做不到就得改接口"的假设。里程碑 ① 删除整个目录。
package probe

import (
	"context"
	"io"
	"net/http"
	"sync"
)

// h2Carrier 对齐 §2.1 的 ByteStreamCarrier + SendCloser + ResponseHeaderReader，
// 只实现探针需要的方法。请求体走 io.Pipe，RoundTrip 在独立 goroutine 上跑——
// 这正是 v1 transport/http2 的做法，也是"不确定窗口"的来源。
type h2Carrier struct {
	pw    *io.PipeWriter
	ready chan struct{} // RoundTrip 返回后关闭

	mu      sync.Mutex
	resp    *http.Response
	respErr error
}

func dialH2(ctx context.Context, cl *http.Client, url string, hdr http.Header) *h2Carrier {
	pr, pw := io.Pipe()
	c := &h2Carrier{pw: pw, ready: make(chan struct{})}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, pr)
	if err != nil {
		c.finish(nil, err)
		return c
	}
	for k, vs := range hdr {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	go func() {
		resp, err := cl.Do(req)
		if err != nil {
			// 让已经阻塞在 Write 上的发送方立刻失败，而不是永远等下去。
			_ = pr.CloseWithError(err)
		}
		c.finish(resp, err)
	}()
	return c
}

func (c *h2Carrier) finish(resp *http.Response, err error) {
	c.mu.Lock()
	c.resp, c.respErr = resp, err
	c.mu.Unlock()
	close(c.ready)
}

func (c *h2Carrier) Write(p []byte) (int, error) { return c.pw.Write(p) }

func (c *h2Carrier) CloseSend() error { return c.pw.Close() }

// responded 报告 RoundTrip 是否已经返回。§2.4 的"不确定窗口"就是
// 写已经失败、而它仍为 false 的那一段。
func (c *h2Carrier) responded() bool {
	select {
	case <-c.ready:
		return true
	default:
		return false
	}
}

func (c *h2Carrier) ResponseHeaders(ctx context.Context) (http.Header, error) {
	select {
	case <-c.ready:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.respErr != nil {
		return nil, c.respErr
	}
	return c.resp.Header, nil
}

// Body 供需要读 DATA 的探针使用。
func (c *h2Carrier) Body() (io.ReadCloser, error) {
	if _, err := c.ResponseHeaders(context.Background()); err != nil {
		return nil, err
	}
	return c.resp.Body, nil
}

// Trailers 只有在 Body 读到 EOF 之后才完整。
func (c *h2Carrier) Trailers() http.Header {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.resp.Trailer
}

package probe

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

// http1FinishProbe 模拟 wholebody/http1 的 Send→Finish 路径：Send 只缓冲 body，
// 不在线上提交；Finish 通过 WriteResponse 一次性提交状态、headers 与 body。
// 对应 §4.6 wholebody × http1 与 UnaryResponseWriter 契约。
type http1FinishProbe struct {
	buf     bytes.Buffer
	headers http.Header
}

func (p *http1FinishProbe) send(body []byte) {
	p.buf.Write(body)
}

func (p *http1FinishProbe) writeResponse(w http.ResponseWriter, status int, body []byte) error {
	if p.headers != nil {
		for k, vs := range p.headers {
			for _, v := range vs {
				w.Header().Add(k, v)
			}
		}
	}
	w.WriteHeader(status)
	_, err := w.Write(body)
	return err
}

// §4.6：handler 在 Send 之后仍能返回 error，Finish 必须提交错误状态而非提前落线的 200。
func TestHTTP1FinishOnError(t *testing.T) {
	const (
		successBody = `{"result":"ok"}`
		errorBody   = `{"error":"handler failed"}`
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		probe := &http1FinishProbe{
			headers: http.Header{"Content-Type": {"application/json"}},
		}

		// handler Send：写入缓冲，不 WriteHeader。
		probe.send([]byte(successBody))

		// handler 返回 error；若 Send 时已提交 200，客户端将永远看不到 500。
		_ = errors.New("handler failed")

		// 组合层 Finish：无条件一次性 WriteResponse(500, …)。
		if err := probe.writeResponse(w, http.StatusInternalServerError, []byte(errorBody)); err != nil {
			t.Errorf("WriteResponse: %v", err)
		}
	}))
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	gotBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}

	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 (must not be 200)", resp.StatusCode)
	}
	if !bytes.Equal(gotBody, []byte(errorBody)) {
		t.Fatalf("body = %q, want %q", gotBody, errorBody)
	}
	if bytes.Contains(gotBody, []byte(successBody)) {
		t.Fatalf("client saw buffered success body %q after handler error", successBody)
	}
}

// §4.6 对照：标准 ResponseWriter 一旦 WriteHeader(200) 就无法再改状态码，
// 这就是 wholebody/http1 需要 UnaryResponseWriter、缓冲至 Finish 的原因。
func TestHTTP1CannotChangeAfter200(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		// 模拟 handler 事后发现 error、想改 500——net/http 会忽略第二次 WriteHeader。
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"too late"}`))
	}))
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 (second WriteHeader must be ignored)", resp.StatusCode)
	}

	gotBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if !bytes.Equal(gotBody, []byte(`{"error":"too late"}`)) {
		t.Fatalf("body = %q, want error JSON (body still writes, but status stays 200)", gotBody)
	}
}

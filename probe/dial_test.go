package probe

import (
	"io"
	"net/http"
	"testing"
)

// §4.2 要求 Dial 在收到响应 headers 之前就返回可写承载。如果这条不成立，
// 客户端"先 Dial 再 Send"的顺序无法表达，请求永远发不出去。
func TestDialReturnsBeforeResponseHeaders(t *testing.T) {
	gotFirstByte := make(chan struct{})
	release := make(chan struct{})
	srv := newH2CServer(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadFull(r.Body, make([]byte, 1))
		close(gotFirstByte)
		<-release
		w.Header().Set("Content-Type", "application/grpc+proto")
		w.WriteHeader(http.StatusOK)
	})

	c := dialH2(t.Context(), srv.client, srv.url, grpcHeaders())

	if c.responded() {
		t.Fatal("RoundTrip 在写入任何请求字节之前就返回了：请求方向被阻塞")
	}
	if _, err := c.Write([]byte{0}); err != nil {
		t.Fatalf("响应 headers 之前写入失败: %v", err)
	}
	<-gotFirstByte
	if c.responded() {
		t.Fatal("服务端尚未写响应头，RoundTrip 不应已经返回")
	}
	close(release)

	if _, err := c.ResponseHeaders(t.Context()); err != nil {
		t.Fatalf("响应 headers: %v", err)
	}
}

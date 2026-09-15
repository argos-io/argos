package probe

import (
	"io"
	"net/http"
	"testing"
)

// §2.4：写失败时 ReceiveOpen 可能落进"写已失败、RoundTrip 尚未返回"的窗口。
// 契约是此时必须返回 true，由随后的 Recv 给出确定结果。本探针断言两件事：
//  1. 窗口真实存在（否则契约里那段说明就是多余的）；
//  2. 无论是否落在窗口里，继续接收都能拿到远端的真实状态。
func TestSendWindowReceiveOpen(t *testing.T) {
	// 服务端读到一个字节就以 trailers-only 拒绝，制造"客户端还在写、
	// 服务端已经结束流"的时序。
	srv := newH2CServer(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadFull(r.Body, make([]byte, 1))
		h := w.Header()
		h.Set("Content-Type", "application/grpc+proto")
		h.Set("Grpc-Status", "16") // Unauthenticated
		h.Set("Grpc-Message", "no token")
		w.WriteHeader(http.StatusOK) // 无 DATA：HEADERS + END_STREAM
	})

	const attempts = 50
	var inWindowCount int

	for i := range attempts {
		c := newH2Endpoint(srv.client, srv.url).openH2Stream(t.Context(), grpcHeaders())

		var writeErr error
		inWindow := false
		chunk := make([]byte, 16<<10)
		for range 4096 { // 最多 64 MiB，远超默认流控窗口
			if _, writeErr = c.Write(chunk); writeErr != nil {
				// 关键采样点：必须紧挨着写失败，中间不能有任何同步。
				inWindow = !c.responded()
				break
			}
		}
		if writeErr == nil {
			t.Fatalf("第 %d 次：写始终成功，服务端没有提前结束流", i)
		}
		if inWindow {
			inWindowCount++
		}

		hdr, err := c.ResponseHeaders(t.Context())
		if err != nil {
			t.Fatalf("第 %d 次：窗口=%v，写失败后接收也失败: %v", i, inWindow, err)
		}
		if got := hdr.Get("Grpc-Status"); got != "16" {
			t.Fatalf("第 %d 次：窗口=%v，grpc-status = %q, want 16", i, inWindow, got)
		}
	}

	if inWindowCount == 0 {
		t.Fatal("50 次都没落入不确定窗口：探针没验到目标时序，" +
			"加大请求体或让服务端延后读取后重试")
	}
	t.Logf("命中不确定窗口 %d/%d；全部 %d 次都读到了远端状态",
		inWindowCount, attempts, attempts)
}

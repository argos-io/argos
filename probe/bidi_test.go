package probe

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// §2.2：服务端可主动提交 initial headers（不先读 body），客户端可先 Header() 再 Send。
// 双方都不死等——否则 bidi 的"先 headers 后首条消息"顺序无法表达。
func TestHandshakeOrderActiveInitialHeaders(t *testing.T) {
	clientWrote := make(chan struct{})
	serverFlushed := make(chan struct{})

	srv := newH2CServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/grpc+proto")
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush() // 不读 body，主动推 headers
		close(serverFlushed)

		select {
		case <-clientWrote:
		case <-time.After(5 * time.Second):
			t.Error("服务端在 Flush 后等客户端 Write 超时：握手顺序死锁")
			return
		}
		_, _ = io.Copy(io.Discard, r.Body)
	})

	c := newH2Endpoint(srv.client, srv.url).openH2Stream(t.Context(), grpcHeaders())

	errCh := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		if _, err := c.ResponseHeaders(ctx); err != nil {
			errCh <- fmt.Errorf("ResponseHeaders: %w", err)
			return
		}
		if _, err := c.Write([]byte{0}); err != nil {
			errCh <- fmt.Errorf("Write after headers: %w", err)
			return
		}
		close(clientWrote)
		errCh <- c.CloseSend()
	}()

	select {
	case <-serverFlushed:
	case <-time.After(5 * time.Second):
		t.Fatal("服务端未能在不读 body 的情况下 Flush headers：被请求方向阻塞")
	}

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("客户端 ResponseHeaders→Write 阻塞：无法表达主动 initial headers")
	}
}

// 空 initial metadata 也必须能唤醒客户端的 ResponseHeaders（不能靠"有自定义头"才返回）。
func TestEmptyMetadataWakesResponseHeaders(t *testing.T) {
	flushed := make(chan struct{})
	srv := newH2CServer(t, func(w http.ResponseWriter, r *http.Request) {
		// 故意不设任何自定义头，只提交 200 并 Flush。
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		close(flushed)
		_, _ = io.Copy(io.Discard, r.Body)
	})

	c := newH2Endpoint(srv.client, srv.url).openH2Stream(t.Context(), grpcHeaders())

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if _, err := c.ResponseHeaders(ctx); err != nil {
		t.Fatalf("空 metadata 的 ResponseHeaders: %v", err)
	}

	select {
	case <-flushed:
	case <-time.After(time.Second):
		t.Fatal("服务端未 Flush")
	}
	_ = c.CloseSend()
}

const (
	bidiServiceName = "probe.Bidi"
	bidiFullMethod  = "/probe.Bidi/Echo"
	bidiMsgCount    = 100
)

type bidiEchoServer interface {
	Echo(grpc.ServerStream) error
}

func registerBidiEcho(s grpc.ServiceRegistrar, srv bidiEchoServer) {
	s.RegisterService(&grpc.ServiceDesc{
		ServiceName: bidiServiceName,
		HandlerType: (*bidiEchoServer)(nil),
		Streams: []grpc.StreamDesc{{
			StreamName:    "Echo",
			Handler:       bidiEchoHandler,
			ServerStreams: true,
			ClientStreams: true,
		}},
	}, srv)
}

func bidiEchoHandler(srv any, stream grpc.ServerStream) error {
	return srv.(bidiEchoServer).Echo(stream)
}

type interleavedBidiServer struct {
	t *testing.T
}

func (s *interleavedBidiServer) Echo(stream grpc.ServerStream) error {
	errCh := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for i := range bidiMsgCount {
			msg := &wrapperspb.Int32Value{Value: int32(i)}
			if err := stream.SendMsg(msg); err != nil {
				errCh <- fmt.Errorf("server send %d: %w", i, err)
				return
			}
		}
	}()

	go func() {
		defer wg.Done()
		for i := range bidiMsgCount {
			msg := new(wrapperspb.Int32Value)
			if err := stream.RecvMsg(msg); err != nil {
				errCh <- fmt.Errorf("server recv %d: %w", i, err)
				return
			}
			if got, want := msg.GetValue(), int32(i); got != want {
				errCh <- fmt.Errorf("server recv order: got %d, want %d", got, want)
				return
			}
		}
	}()

	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return err
		}
	}
	return nil
}

// 真实 bidi：两侧各发各收 100 条，交错进行，全部按序到达。
func TestInterleavedBidiStreaming(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	gs := grpc.NewServer()
	registerBidiEcho(gs, &interleavedBidiServer{t: t})
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)

	cc, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	t.Cleanup(func() { _ = cc.Close() })

	desc := &grpc.StreamDesc{
		StreamName:    "Echo",
		ServerStreams: true,
		ClientStreams: true,
	}
	stream, err := cc.NewStream(t.Context(), desc, bidiFullMethod)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}

	errCh := make(chan error, 2)
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for i := range bidiMsgCount {
			msg := &wrapperspb.Int32Value{Value: int32(i)}
			if err := stream.SendMsg(msg); err != nil {
				errCh <- fmt.Errorf("client send %d: %w", i, err)
				return
			}
		}
		if err := stream.CloseSend(); err != nil {
			errCh <- fmt.Errorf("CloseSend: %w", err)
		}
	}()

	go func() {
		defer wg.Done()
		for i := range bidiMsgCount {
			msg := new(wrapperspb.Int32Value)
			if err := stream.RecvMsg(msg); err != nil {
				errCh <- fmt.Errorf("client recv %d: %w", i, err)
				return
			}
			if got, want := msg.GetValue(), int32(i); got != want {
				errCh <- fmt.Errorf("client recv order: got %d, want %d", got, want)
				return
			}
		}
	}()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("交错 bidi 超时")
	}
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
}

type earlyErrorBidiServer struct{}

func (earlyErrorBidiServer) Echo(stream grpc.ServerStream) error {
	// 读一条证明流已开，然后在客户端仍可发送时立刻以 PermissionDenied 结束。
	msg := new(wrapperspb.Int32Value)
	if err := stream.RecvMsg(msg); err != nil {
		return err
	}
	return status.Error(codes.PermissionDenied, "denied")
}

// 服务端在客户端还在发送时写出 Grpc-Status:7；客户端必须能读到它。
func TestBidiEarlyErrorWhileClientSending(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	gs := grpc.NewServer()
	registerBidiEcho(gs, earlyErrorBidiServer{})
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)

	cc, err := grpc.NewClient(lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	t.Cleanup(func() { _ = cc.Close() })

	desc := &grpc.StreamDesc{
		StreamName:    "Echo",
		ServerStreams: true,
		ClientStreams: true,
	}
	stream, err := cc.NewStream(t.Context(), desc, bidiFullMethod)
	if err != nil {
		t.Fatalf("NewStream: %v", err)
	}

	sendErr := make(chan error, 1)
	go func() {
		// 持续发送，模拟客户端尚在上行时服务端已结束。
		for i := 0; ; i++ {
			if err := stream.SendMsg(&wrapperspb.Int32Value{Value: int32(i)}); err != nil {
				sendErr <- err
				return
			}
		}
	}()

	msg := new(wrapperspb.Int32Value)
	recvErr := stream.RecvMsg(msg)
	st, ok := status.FromError(recvErr)
	if !ok {
		t.Fatalf("RecvMsg error type = %T (%v), want status", recvErr, recvErr)
	}
	if st.Code() != codes.PermissionDenied {
		t.Fatalf("grpc code = %v, want PermissionDenied (7)", st.Code())
	}
	if int(st.Code()) != 7 {
		t.Fatalf("numeric code = %d, want 7", st.Code())
	}

	select {
	case <-sendErr:
		// 发送侧最终会因流结束而失败，属预期。
	case <-time.After(5 * time.Second):
		t.Fatal("发送 goroutine 未因对端提前错误而退出")
	}
}

// 纯 h2 路径的提前错误：服务端在客户端仍在写时提交 Grpc-Status:7。
func TestEarlyErrorGrpcStatusHeader(t *testing.T) {
	srv := newH2CServer(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadFull(r.Body, make([]byte, 1))
		h := w.Header()
		h.Set("Content-Type", "application/grpc+proto")
		h.Set("Grpc-Status", "7") // PermissionDenied
		h.Set("Grpc-Message", "denied")
		w.WriteHeader(http.StatusOK) // trailers-only：HEADERS + END_STREAM
	})

	c := newH2Endpoint(srv.client, srv.url).openH2Stream(t.Context(), grpcHeaders())

	sendDone := make(chan error, 1)
	go func() {
		chunk := make([]byte, 16<<10)
		for {
			if _, err := c.Write(chunk); err != nil {
				sendDone <- err
				return
			}
		}
	}()

	hdr, err := c.ResponseHeaders(t.Context())
	if err != nil {
		t.Fatalf("ResponseHeaders: %v", err)
	}
	if got := hdr.Get("Grpc-Status"); got != "7" {
		t.Fatalf("Grpc-Status = %q, want 7", got)
	}

	select {
	case <-sendDone:
	case <-time.After(5 * time.Second):
		t.Fatal("客户端发送未因对端提前结束而失败")
	}
}

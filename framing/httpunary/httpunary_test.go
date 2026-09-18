package httpunary_test

import (
	"context"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/framing"
	"github.com/argos-io/argos/framing/httpunary"
	"github.com/argos-io/argos/internal/httpstatus"
	"github.com/argos-io/argos/metadata"
	"github.com/argos-io/argos/status"
	"github.com/argos-io/argos/transport"
	argoshttp1 "github.com/argos-io/argos/transport/http1"
)

func testCfg() framing.Config {
	return framing.Config{
		MaxMessageSize:    1 << 20,
		MaxFrameSize:      1 << 20,
		MaxMetadataSize:   64 << 10,
		ReadAheadMessages: 1,
	}
}

func waitAddr(t *testing.T, tr *argoshttp1.Transport) net.Addr {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if a := tr.Addr(); a != nil {
			return a
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("listener Addr not set")
	return nil
}

func startHTTP1(t *testing.T, onConn func(context.Context, transport.Conn)) string {
	t.Helper()
	raw := argoshttp1.New()
	tr := raw.(*argoshttp1.Transport)
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- tr.Serve(ctx, onConn, transport.WithListenAddress("127.0.0.1:0"))
	}()
	a := waitAddr(t, tr)
	t.Cleanup(func() {
		cancel()
		_ = tr.Close()
		select {
		case <-errCh:
		case <-time.After(2 * time.Second):
			t.Error("Serve did not return")
		}
	})
	return a.String()
}

func dialStream(t *testing.T, addr string) transport.StreamConn {
	t.Helper()
	clientTr := argoshttp1.New()
	t.Cleanup(func() { _ = clientTr.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	conn, err := clientTr.Dial(ctx, transport.DialSpec{Endpoint: addr})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	sc, ok := conn.(transport.StreamConn)
	if !ok {
		t.Fatal("Dial did not return StreamConn")
	}
	return sc
}

func TestReuseConcurrent(t *testing.T) {
	if got := httpunary.NewRPC().Reuse(); got != framing.Concurrent {
		t.Fatalf("Reuse() = %v, want Concurrent", got)
	}
}

// Test 1: unary echo over http1 transport (client + server).
func TestUnaryEchoHTTP1(t *testing.T) {
	method := descriptor.MustMethod("echo.v1.EchoService.Echo", descriptor.Unary)
	fr := httpunary.NewRPC()
	spec := framing.SessionSpec{CodecName: "json", Config: testCfg()}

	addr := startHTTP1(t, func(ctx context.Context, c transport.Conn) {
		sess, err := fr.NewServerSession(ctx, c, spec)
		if err != nil {
			t.Errorf("NewServerSession: %v", err)
			return
		}
		defer func() { _ = sess.Close() }()

		md := metadata.New(metadata.RoleResponder, nil)
		call, err := sess.AcceptCall(ctx, framing.CallSpec{Metadata: md})
		if err != nil {
			t.Errorf("AcceptCall: %v", err)
			return
		}
		defer func() { _ = call.Close() }()

		if err := call.Accept(method); err != nil {
			t.Errorf("Accept: %v", err)
			_ = call.Finish(err)
			return
		}
		payload, release, err := call.Recv()
		if err != nil {
			t.Errorf("server Recv: %v", err)
			_ = call.Finish(err)
			return
		}
		defer release()
		if err := call.Send(append([]byte("echo:"), payload...)); err != nil {
			t.Errorf("server Send: %v", err)
			_ = call.Finish(err)
			return
		}
		if err := call.Finish(nil); err != nil {
			t.Errorf("Finish: %v", err)
		}
	})

	sc := dialStream(t, addr)
	cliSess, err := fr.NewClientSession(context.Background(), sc, spec)
	if err != nil {
		t.Fatalf("NewClientSession: %v", err)
	}
	defer func() { _ = cliSess.Close() }()

	md := metadata.New(metadata.RoleInitiator, nil)
	call, err := cliSess.OpenCall(context.Background(), method, framing.CallSpec{Metadata: md})
	if err != nil {
		t.Fatalf("OpenCall: %v", err)
	}
	defer func() { _ = call.Close() }()

	req := []byte(`{"msg":"hi"}`)
	if err := call.Send(req); err != nil {
		t.Fatalf("client Send: %v", err)
	}
	if err := call.HalfClose(); err != nil {
		t.Fatalf("HalfClose: %v", err)
	}
	got, release, err := call.Recv()
	if err != nil {
		t.Fatalf("client Recv: %v", err)
	}
	defer release()
	want := append([]byte("echo:"), req...)
	if string(got) != string(want) {
		t.Fatalf("response = %q, want %q", got, want)
	}
	_, _, err = call.Recv()
	if !errors.Is(err, io.EOF) {
		t.Fatalf("second Recv = %v, want EOF", err)
	}
}

func TestRESTPostHTTP1(t *testing.T) {
	method := descriptor.MustMethod("echo.v1.EchoService.Echo", descriptor.Unary)
	fr, err := httpunary.NewREST(httpunary.RESTConfig{
		Bindings: []httpunary.Binding{
			{Method: method, Verb: "POST", Pattern: "/v1/echo"},
		},
	})
	if err != nil {
		t.Fatalf("NewREST: %v", err)
	}
	spec := framing.SessionSpec{CodecName: "json", Config: testCfg()}

	addr := startHTTP1(t, func(ctx context.Context, c transport.Conn) {
		sess, err := fr.NewServerSession(ctx, c, spec)
		if err != nil {
			t.Errorf("NewServerSession: %v", err)
			return
		}
		defer func() { _ = sess.Close() }()

		md := metadata.New(metadata.RoleResponder, nil)
		call, err := sess.AcceptCall(ctx, framing.CallSpec{Metadata: md})
		if err != nil {
			t.Errorf("AcceptCall: %v", err)
			return
		}
		defer func() { _ = call.Close() }()
		if call.Method() != method.FullName() {
			t.Errorf("method = %q", call.Method())
		}
		payload, release, err := call.Recv()
		if err != nil {
			t.Errorf("Recv: %v", err)
			return
		}
		release()
		if err := call.Finish(nil); err != nil {
			t.Errorf("Finish: %v", err)
		}
		_ = payload
	})

	sc := dialStream(t, addr)
	cliSess, err := fr.NewClientSession(context.Background(), sc, spec)
	if err != nil {
		t.Fatalf("NewClientSession: %v", err)
	}
	defer func() { _ = cliSess.Close() }()

	md := metadata.New(metadata.RoleInitiator, nil)
	call, err := cliSess.OpenCall(context.Background(), method, framing.CallSpec{Metadata: md})
	if err != nil {
		t.Fatalf("OpenCall: %v", err)
	}
	defer func() { _ = call.Close() }()
	if err := call.Send([]byte(`{}`)); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if err := call.HalfClose(); err != nil {
		t.Fatalf("HalfClose: %v", err)
	}
	_, release, err := call.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	release()
}

// AcceptCall must return a finishable ServerCall alongside ErrCallRejected so
// the composition layer can write the HTTP error (server.handleRejected).
func TestAcceptCallRejectReturnsFinishableCall(t *testing.T) {
	fr := httpunary.NewRPC()
	spec := framing.SessionSpec{CodecName: "json", Config: testCfg()}

	addr := startHTTP1(t, func(ctx context.Context, c transport.Conn) {
		sess, err := fr.NewServerSession(ctx, c, spec)
		if err != nil {
			t.Errorf("NewServerSession: %v", err)
			return
		}
		defer func() { _ = sess.Close() }()

		md := metadata.New(metadata.RoleResponder, nil)
		call, err := sess.AcceptCall(ctx, framing.CallSpec{Metadata: md})
		if !errors.Is(err, framing.ErrCallRejected) {
			t.Errorf("AcceptCall err = %v, want ErrCallRejected", err)
			return
		}
		if call == nil {
			t.Fatal("AcceptCall returned nil call on reject")
		}
		defer func() { _ = call.Close() }()

		finishErr := status.Error(status.InvalidArgument, err.Error())
		var se *status.StatusError
		if errors.As(err, &se) {
			finishErr = se
		}
		if err := call.Finish(finishErr); err != nil {
			t.Errorf("Finish: %v", err)
		}
	})

	sc := dialStream(t, addr)
	car, err := sc.OpenStream(context.Background(), transport.RequestPreface{RequestTarget: "/not-valid"})
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	defer func() { _ = car.Abort() }()
	if err := car.(transport.SendCloser).CloseSend(); err != nil {
		t.Fatalf("CloseSend: %v", err)
	}
	rh := car.(transport.ResponseHeaderReader)
	httpSt, err := rh.ResponseStatus()
	if err != nil {
		t.Fatalf("ResponseStatus: %v", err)
	}
	if httpSt != httpstatus.ToHTTP(status.InvalidArgument) {
		t.Fatalf("HTTP status = %d, want InvalidArgument mapping", httpSt)
	}
}

// Test 2: non-unary Accept returns Unimplemented; handler not called.
func TestAcceptRejectsNonUnary(t *testing.T) {
	unary := descriptor.MustMethod("echo.v1.EchoService.Echo", descriptor.Unary)
	stream := descriptor.MustMethod("echo.v1.EchoService.Watch", descriptor.ServerStreaming)
	fr := httpunary.NewRPC()
	spec := framing.SessionSpec{CodecName: "json", Config: testCfg()}

	var handlerCalled atomic.Bool

	addr := startHTTP1(t, func(ctx context.Context, c transport.Conn) {
		sess, err := fr.NewServerSession(ctx, c, spec)
		if err != nil {
			t.Errorf("NewServerSession: %v", err)
			return
		}
		defer func() { _ = sess.Close() }()

		md := metadata.New(metadata.RoleResponder, nil)
		call, err := sess.AcceptCall(ctx, framing.CallSpec{Metadata: md})
		if err != nil {
			t.Errorf("AcceptCall: %v", err)
			return
		}
		defer func() { _ = call.Close() }()

		// Simulate composition: Accept before Filter/handler.
		err = call.Accept(stream)
		if status.CodeOf(err) != status.Unimplemented {
			t.Errorf("Accept(non-unary) CodeOf=%v err=%v, want Unimplemented", status.CodeOf(err), err)
		}
		if handlerCalled.Load() {
			t.Error("handler called before Accept rejection")
		}
		// Would-be handler — must not run on Accept failure.
		if err == nil {
			handlerCalled.Store(true)
			_, release, rerr := call.Recv()
			if rerr == nil {
				release()
			}
			_ = call.Send([]byte("nope"))
			_ = call.Finish(nil)
			return
		}
		_ = call.Finish(err)
	})

	sc := dialStream(t, addr)
	cliSess, err := fr.NewClientSession(context.Background(), sc, spec)
	if err != nil {
		t.Fatalf("NewClientSession: %v", err)
	}
	defer func() { _ = cliSess.Close() }()

	md := metadata.New(metadata.RoleInitiator, nil)
	// Client OpenCall also rejects non-unary; use unary to reach the server Accept.
	call, err := cliSess.OpenCall(context.Background(), unary, framing.CallSpec{Metadata: md})
	if err != nil {
		t.Fatalf("OpenCall: %v", err)
	}
	defer func() { _ = call.Close() }()
	_ = call.HalfClose()

	_, _, err = call.Recv()
	if status.CodeOf(err) != status.Unimplemented {
		t.Fatalf("client Recv CodeOf=%v err=%v, want Unimplemented", status.CodeOf(err), err)
	}
	if handlerCalled.Load() {
		t.Fatal("handler was called despite Accept rejection")
	}
}

// Test 3: SendHeaders returns Unimplemented; response not committed as 200;
// metadata is not frozen.
func TestSendHeadersUnimplementedNoCommit(t *testing.T) {
	method := descriptor.MustMethod("echo.v1.EchoService.Echo", descriptor.Unary)
	fr := httpunary.NewRPC()
	spec := framing.SessionSpec{CodecName: "json", Config: testCfg()}

	hold := make(chan struct{})
	sawSendHeaders := make(chan error, 1)

	addr := startHTTP1(t, func(ctx context.Context, c transport.Conn) {
		sess, err := fr.NewServerSession(ctx, c, spec)
		if err != nil {
			t.Errorf("NewServerSession: %v", err)
			return
		}
		defer func() { _ = sess.Close() }()

		var call framing.ServerCall
		md := metadata.New(metadata.RoleResponder, func(metadata.Metadata) error {
			if call == nil {
				return status.Error(status.Unimplemented, "no call")
			}
			return call.SendHeaders()
		})

		call, err = sess.AcceptCall(ctx, framing.CallSpec{Metadata: md})
		if err != nil {
			t.Errorf("AcceptCall: %v", err)
			return
		}
		defer func() { _ = call.Close() }()
		if err := call.Accept(method); err != nil {
			t.Errorf("Accept: %v", err)
			_ = call.Finish(err)
			return
		}
		_, release, err := call.Recv()
		if err != nil {
			t.Errorf("Recv: %v", err)
			_ = call.Finish(err)
			return
		}
		release()

		err = call.SendHeaders()
		sawSendHeaders <- err
		if status.CodeOf(err) != status.Unimplemented {
			t.Errorf("SendHeaders CodeOf=%v, want Unimplemented", status.CodeOf(err))
		}
		// Must not freeze metadata on Unimplemented.
		if addErr := md.AddOutgoingHeader("x-still", "ok"); addErr != nil {
			t.Errorf("AddOutgoingHeader after SendHeaders: %v (must not freeze)", addErr)
		}
		// Also via CallMetadata.SendHeaders path.
		if mdErr := md.SendHeaders(); status.CodeOf(mdErr) != status.Unimplemented {
			t.Errorf("md.SendHeaders CodeOf=%v, want Unimplemented", status.CodeOf(mdErr))
		}
		if addErr := md.AddOutgoingHeader("x-after-md", "ok"); addErr != nil {
			t.Errorf("AddOutgoingHeader after md.SendHeaders: %v", addErr)
		}

		<-hold // keep response uncommitted until client has observed no headers
		_ = call.Finish(status.Error(status.Internal, "late"))
	})

	sc := dialStream(t, addr)
	cliSess, err := fr.NewClientSession(context.Background(), sc, spec)
	if err != nil {
		t.Fatalf("NewClientSession: %v", err)
	}
	defer func() { _ = cliSess.Close() }()

	md := metadata.New(metadata.RoleInitiator, nil)
	call, err := cliSess.OpenCall(context.Background(), method, framing.CallSpec{Metadata: md})
	if err != nil {
		t.Fatalf("OpenCall: %v", err)
	}
	defer func() { _ = call.Close() }()
	_ = call.HalfClose()

	select {
	case err := <-sawSendHeaders:
		if status.CodeOf(err) != status.Unimplemented {
			t.Fatalf("server SendHeaders: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for server SendHeaders")
	}

	// Response must not be committed yet (no early 200 from SendHeaders).
	done := make(chan error, 1)
	go func() {
		_, _, err := call.Recv()
		done <- err
	}()
	select {
	case err := <-done:
		t.Fatalf("Recv returned while server held response: %v (SendHeaders must not commit)", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(hold)
	select {
	case err := <-done:
		if status.CodeOf(err) != status.Internal {
			t.Fatalf("Recv after Finish: CodeOf=%v err=%v, want Internal", status.CodeOf(err), err)
		}
		if httpstatus.ToHTTP(status.CodeOf(err)) == 200 {
			t.Fatal("mapped HTTP status is 200; want error status")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Recv timed out after Finish")
	}
}

// Test 4: handler Send then return error → client sees error status, not 200.
func TestFinishAfterSendCommitsErrorNot200(t *testing.T) {
	method := descriptor.MustMethod("echo.v1.EchoService.Echo", descriptor.Unary)
	fr := httpunary.NewRPC()
	spec := framing.SessionSpec{CodecName: "json", Config: testCfg()}

	const successBody = `{"result":"ok"}`

	addr := startHTTP1(t, func(ctx context.Context, c transport.Conn) {
		sess, err := fr.NewServerSession(ctx, c, spec)
		if err != nil {
			t.Errorf("NewServerSession: %v", err)
			return
		}
		defer func() { _ = sess.Close() }()

		md := metadata.New(metadata.RoleResponder, nil)
		call, err := sess.AcceptCall(ctx, framing.CallSpec{Metadata: md})
		if err != nil {
			t.Errorf("AcceptCall: %v", err)
			return
		}
		defer func() { _ = call.Close() }()
		if err := call.Accept(method); err != nil {
			_ = call.Finish(err)
			return
		}
		_, release, err := call.Recv()
		if err != nil {
			_ = call.Finish(err)
			return
		}
		release()

		// Handler "Send"s success body into the framing buffer (no HTTP commit).
		if err := call.Send([]byte(successBody)); err != nil {
			t.Errorf("Send: %v", err)
			_ = call.Finish(err)
			return
		}
		// Handler returns error; Finish must commit error status once, not 200.
		if err := call.Finish(status.Error(status.Internal, "handler failed")); err != nil {
			t.Errorf("Finish: %v", err)
		}
	})

	sc := dialStream(t, addr)
	cliSess, err := fr.NewClientSession(context.Background(), sc, spec)
	if err != nil {
		t.Fatalf("NewClientSession: %v", err)
	}
	defer func() { _ = cliSess.Close() }()

	md := metadata.New(metadata.RoleInitiator, nil)
	call, err := cliSess.OpenCall(context.Background(), method, framing.CallSpec{Metadata: md})
	if err != nil {
		t.Fatalf("OpenCall: %v", err)
	}
	defer func() { _ = call.Close() }()
	_ = call.HalfClose()

	_, _, err = call.Recv()
	if status.CodeOf(err) != status.Internal {
		t.Fatalf("Recv CodeOf=%v err=%v, want Internal (not success/200)", status.CodeOf(err), err)
	}
	if httpstatus.ToHTTP(status.CodeOf(err)) == 200 {
		t.Fatal("status maps to HTTP 200")
	}
}

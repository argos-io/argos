package fake

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/internal/session"
	"github.com/argos-io/argos/internal/sessionpool"
	"github.com/argos-io/argos/metadata"
	"github.com/argos-io/argos/transport"
)

func testMethod(t *testing.T) descriptor.Method {
	t.Helper()
	m, err := descriptor.NewMethod("echo.v1.Echo.Echo", descriptor.Unary)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func callSpec(role metadata.Role) session.CallSpec {
	return session.CallSpec{Metadata: metadata.New(role, nil)}
}

func TestSequentialUnaryRoundTrip(t *testing.T) {
	t.Parallel()
	cliConn, srvConn := BytePipe()
	defer cliConn.Close()
	defer srvConn.Close()

	f := NewFraming(session.Sequential)
	ctx := context.Background()
	spec := session.SessionSpec{CodecName: "fake"}

	cs, err := f.NewClientSession(ctx, cliConn, spec)
	if err != nil {
		t.Fatalf("NewClientSession: %v", err)
	}
	defer cs.Close()

	ss, err := f.NewServerSession(ctx, srvConn, spec)
	if err != nil {
		t.Fatalf("NewServerSession: %v", err)
	}
	defer ss.Close()

	method := testMethod(t)
	errCh := make(chan error, 1)
	go func() {
		sc, err := ss.AcceptCall(ctx, callSpec(metadata.RoleResponder))
		if err != nil {
			errCh <- err
			return
		}
		if err := sc.Accept(method); err != nil {
			errCh <- err
			return
		}
		payload, release, err := sc.Recv()
		if err != nil {
			errCh <- err
			return
		}
		release()
		// Drain half-close terminal.
		if _, _, err := sc.Recv(); err != io.EOF {
			errCh <- err
			return
		}
		if err := sc.Send(payload); err != nil {
			errCh <- err
			return
		}
		if err := sc.Finish(nil); err != nil {
			errCh <- err
			return
		}
		errCh <- sc.Close()
	}()

	call, err := cs.OpenCall(ctx, method, callSpec(metadata.RoleInitiator))
	if err != nil {
		t.Fatalf("OpenCall: %v", err)
	}
	if err := call.Send([]byte("ping")); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if err := call.HalfClose(); err != nil {
		t.Fatalf("HalfClose: %v", err)
	}
	got, release, err := call.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	release()
	if string(got) != "ping" {
		t.Fatalf("Recv = %q, want ping", got)
	}
	if _, _, err := call.Recv(); err != io.EOF {
		t.Fatalf("Recv terminal: %v, want EOF", err)
	}
	if err := call.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("server: %v", err)
	}
}

func TestSequentialCallCloseDoesNotCloseConn(t *testing.T) {
	t.Parallel()
	cliConn, srvConn := BytePipe()
	defer cliConn.Close()
	defer srvConn.Close()

	f := NewFraming(session.Sequential)
	ctx := context.Background()
	spec := session.SessionSpec{}
	cs, err := f.NewClientSession(ctx, cliConn, spec)
	if err != nil {
		t.Fatal(err)
	}
	defer cs.Close()
	ss, err := f.NewServerSession(ctx, srvConn, spec)
	if err != nil {
		t.Fatal(err)
	}
	defer ss.Close()

	method := testMethod(t)
	serveOne := func() error {
		sc, err := ss.AcceptCall(ctx, callSpec(metadata.RoleResponder))
		if err != nil {
			return err
		}
		payload, release, err := sc.Recv()
		if err != nil {
			return err
		}
		release()
		if _, _, err := sc.Recv(); err != io.EOF {
			return err
		}
		if err := sc.Send(append([]byte(nil), payload...)); err != nil {
			return err
		}
		if err := sc.Finish(nil); err != nil {
			return err
		}
		return sc.Close()
	}

	var wg sync.WaitGroup
	errCh := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errCh <- serveOne()
		}()

		call, err := cs.OpenCall(ctx, method, callSpec(metadata.RoleInitiator))
		if err != nil {
			t.Fatalf("OpenCall #%d: %v", i+1, err)
		}
		msg := []byte{byte('a' + i)}
		if err := call.Send(msg); err != nil {
			t.Fatalf("Send #%d: %v", i+1, err)
		}
		if err := call.HalfClose(); err != nil {
			t.Fatalf("HalfClose #%d: %v", i+1, err)
		}
		got, release, err := call.Recv()
		if err != nil {
			t.Fatalf("Recv #%d: %v", i+1, err)
		}
		release()
		if string(got) != string(msg) {
			t.Fatalf("Recv #%d = %q", i+1, got)
		}
		_, _, _ = call.Recv()
		if err := call.Close(); err != nil {
			t.Fatalf("Call.Close #%d: %v", i+1, err)
		}
		if cliConn.Closed() {
			t.Fatalf("after Call.Close #%d: client Conn was Closed", i+1)
		}
		if srvConn.Closed() {
			t.Fatalf("after Call.Close #%d: server Conn was Closed", i+1)
		}
		if err := <-errCh; err != nil {
			t.Fatalf("server #%d: %v", i+1, err)
		}
	}
	wg.Wait()

	if !cs.Reusable() {
		t.Fatal("client session should still be Reusable after two clean calls")
	}
	if cliConn.CloseCount() != 0 {
		t.Fatalf("client CloseCount = %d, want 0 before Session.Close", cliConn.CloseCount())
	}
}

func TestReentryDetector(t *testing.T) {
	t.Parallel()
	pr, pw := io.Pipe()
	defer pr.Close()
	defer pw.Close()

	d := &ReentryDetector{R: pr}
	started := make(chan struct{})
	blocked := make(chan struct{})

	go func() {
		buf := make([]byte, 1)
		close(started)
		_, _ = d.Read(buf) // blocks until Write
		close(blocked)
	}()
	<-started
	time.Sleep(20 * time.Millisecond) // ensure first Read is inside

	done := make(chan struct{})
	go func() {
		buf := make([]byte, 1)
		_, _ = d.Read(buf)
		close(done)
	}()
	time.Sleep(20 * time.Millisecond)
	if d.ConcurrentHits() < 1 {
		t.Fatalf("ConcurrentHits = %d, want >= 1", d.ConcurrentHits())
	}
	_, _ = pw.Write([]byte("ab"))
	<-blocked
	<-done
}

func TestOpenCallBusyScript(t *testing.T) {
	t.Parallel()
	cliConn, srvConn := BytePipe()
	defer cliConn.Close()
	defer srvConn.Close()

	f := NewFraming(session.Sequential)
	f.OpenCallHook = func(callSeq int) error {
		if callSeq >= 1 {
			return session.ErrSessionBusy
		}
		return nil
	}
	ctx := context.Background()
	cs, err := f.NewClientSession(ctx, cliConn, session.SessionSpec{})
	if err != nil {
		t.Fatal(err)
	}
	defer cs.Close()
	_ = srvConn // peer present so pipe stays open

	_, err = cs.OpenCall(ctx, testMethod(t), callSpec(metadata.RoleInitiator))
	if !errors.Is(err, session.ErrSessionBusy) {
		t.Fatalf("OpenCall error = %v, want ErrSessionBusy", err)
	}
}

func TestAxisOpenCallReusesDial(t *testing.T) {
	t.Parallel()
	var dials atomic.Int64
	axis := New(session.Sequential, WithDial(func(ctx context.Context, endpoint string) (transport.Conn, error) {
		dials.Add(1)
		cli, srv := BytePipe()
		go axisEchoServer(t, srv)
		return cli, nil
	}))
	axis.AttachPool(sessionpool.New(axis.DialClientConn(), sessionpool.Options{
		MaxSessionsPerEndpoint: 8,
		MaxIdleSessions:        8,
		MaxCallsPerConn:        MaxCallsPerConn(session.Sequential),
	}))
	defer axis.Close()

	ctx := context.Background()
	method := testMethod(t)
	const n = 20
	for i := 0; i < n; i++ {
		call, err := axis.OpenCall(ctx, "ep", method, transport.CallSpec{
			Metadata: metadata.New(metadata.RoleInitiator, nil),
		})
		if err != nil {
			t.Fatalf("OpenCall #%d: %v", i, err)
		}
		if err := call.Send([]byte("x")); err != nil {
			t.Fatalf("Send #%d: %v", i, err)
		}
		if err := call.HalfClose(); err != nil {
			t.Fatalf("HalfClose #%d: %v", i, err)
		}
		_, release, err := call.Recv()
		if err != nil {
			t.Fatalf("Recv #%d: %v", i, err)
		}
		release()
		if _, _, err := call.Recv(); err != io.EOF {
			t.Fatalf("Recv terminal #%d: %v", i, err)
		}
		if err := call.Close(); err != nil {
			t.Fatalf("Close #%d: %v", i, err)
		}
	}
	if got := dials.Load(); got != 1 {
		t.Fatalf("dial count = %d, want 1", got)
	}
}

func axisEchoServer(t *testing.T, conn transport.Conn) {
	t.Helper()
	f := NewFraming(session.Sequential)
	sess, err := f.NewServerSession(context.Background(), conn, session.SessionSpec{})
	if err != nil {
		_ = conn.Close()
		return
	}
	defer sess.Close()
	ctx := context.Background()
	for {
		sc, err := sess.AcceptCall(ctx, callSpec(metadata.RoleResponder))
		if err != nil {
			return
		}
		payload, release, err := sc.Recv()
		if err != nil {
			_ = sc.Close()
			return
		}
		release()
		if _, _, err := sc.Recv(); err != io.EOF {
			_ = sc.Close()
			return
		}
		if err := sc.Send(payload); err != nil {
			_ = sc.Close()
			return
		}
		if err := sc.Finish(nil); err != nil {
			_ = sc.Close()
			return
		}
		_ = sc.Close()
	}
}

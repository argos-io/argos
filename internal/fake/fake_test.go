package fake

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/framing"
	"github.com/argos-io/argos/metadata"
)

func testMethod(t *testing.T) descriptor.Method {
	t.Helper()
	m, err := descriptor.NewMethod("echo.v1.Echo.Echo", descriptor.Unary)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func callSpec(role metadata.Role) framing.CallSpec {
	return framing.CallSpec{Metadata: metadata.New(role, nil)}
}

func TestSequentialUnaryRoundTrip(t *testing.T) {
	t.Parallel()
	cliConn, srvConn := BytePipe()
	defer cliConn.Close()
	defer srvConn.Close()

	f := NewFraming(framing.Sequential)
	ctx := context.Background()
	spec := framing.SessionSpec{CodecName: "fake"}

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

	f := NewFraming(framing.Sequential)
	ctx := context.Background()
	spec := framing.SessionSpec{}
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

	f := NewFraming(framing.Sequential)
	f.OpenCallHook = func(callSeq int) error {
		if callSeq >= 1 {
			return framing.ErrSessionBusy
		}
		return nil
	}
	ctx := context.Background()
	cs, err := f.NewClientSession(ctx, cliConn, framing.SessionSpec{})
	if err != nil {
		t.Fatal(err)
	}
	defer cs.Close()
	_ = srvConn // peer present so pipe stays open

	_, err = cs.OpenCall(ctx, testMethod(t), callSpec(metadata.RoleInitiator))
	if !errors.Is(err, framing.ErrSessionBusy) {
		t.Fatalf("OpenCall error = %v, want ErrSessionBusy", err)
	}
}

package server

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/argos-io/argos"
	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/filter"
	"github.com/argos-io/argos/internal/fake"
	"github.com/argos-io/argos/internal/session"
	"github.com/argos-io/argos/internal/teststack"
	"github.com/argos-io/argos/stream"
)

func TestProtocolAssembleOncePerServerStart(t *testing.T) {
	tr := newTestTransport()
	fr := fake.NewFraming(session.Sequential)
	h := func(ctx context.Context, m descriptor.Method, st stream.Stream) error {
		var req []byte
		if err := st.Recv(&req); err != nil {
			return err
		}
		drainRecv(st)
		return st.Send(req)
	}
	srv := New(argos.WithServerService(svcName,
		testServiceBindListen(t, tr, fr, "127.0.0.1:0"),
	))
	if err := srv.Register(echoService(), map[string]filter.Handler{methodEcho: h}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(startRun(t, srv))
	for i := 0; i < 3; i++ {
		client, serverConn := fake.BytePipe()
		tr.Offer(serverConn)
		method := descriptor.MustMethod(fullMethod, descriptor.Unary)
		call := openClientCall(t, client, fr, method)
		_ = unaryRoundTrip(t, call, "x")
		_ = call.Close()
	}
}

func TestShutdownIdempotent(t *testing.T) {
	tr := newTestTransport()
	fr := fake.NewFraming(session.Sequential)
	h := func(ctx context.Context, m descriptor.Method, st stream.Stream) error {
		var req []byte
		if err := st.Recv(&req); err != nil {
			return err
		}
		drainRecv(st)
		return st.Send(req)
	}
	srv := startServer(t, tr, fr, h)
	if err := srv.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if err := srv.Shutdown(context.Background()); err != nil {
		t.Fatalf("second Shutdown: %v", err)
	}
	// A stopped server stays stopped: Run must refuse rather than start serving
	// again behind the caller's back.
	if err := srv.Run(context.Background()); err == nil {
		t.Fatal("Run after Shutdown succeeded")
	}
	// The stop is the server's own: it must not have closed the caller's axis.
	if tr.isClosed() {
		t.Error("Shutdown closed the axis the surface served on")
	}
}

func TestFactoryIsolationAcrossListeners(t *testing.T) {
	tr1, tr2 := newTestTransport(), newTestTransport()
	fr1, fr2 := fake.NewFraming(session.Sequential), fake.NewFraming(session.Sequential)
	srv := New(argos.WithServerService(svcName,
		testServiceBindListen(t, tr1, fr1, "127.0.0.1:1"),
		testServiceBindListen(t, tr2, fr2, "127.0.0.1:2"),
	))
	h := func(ctx context.Context, m descriptor.Method, st stream.Stream) error { return nil }
	if err := srv.Register(echoService(), map[string]filter.Handler{methodEcho: h}); err != nil {
		t.Fatal(err)
	}
	stop := startRun(t, srv)
	stop()
	// Two surfaces means two axis instances: stopping the server that served on
	// them must leave both where it found them.
	if tr1.isClosed() || tr2.isClosed() {
		t.Errorf("stop left axes closed: first=%v second=%v", tr1.isClosed(), tr2.isClosed())
	}
}

// newAbortingServer returns a Server whose second listen surface cannot be
// assembled, so Run fails part-way through the surface list, after the first
// surface has already been assembled, plus both surfaces' Transport.
func newAbortingServer(t *testing.T) (*Server, *testTransport, *testTransport) {
	t.Helper()
	tr1, tr2 := newTestTransport(), newTestTransport()
	fr := fake.NewFraming(session.Sequential)
	srv := New(argos.WithServerService(svcName,
		testServiceBindListen(t, tr1, fr, "127.0.0.1:1"),
		argos.ServiceBindListen("127.0.0.1:2", teststack.TransportName(t, testServiceAxis(tr2, fr)), testRefusingCodecName),
	))
	if err := srv.Register(echoService(), map[string]filter.Handler{
		methodEcho: func(context.Context, descriptor.Method, stream.Stream) error { return nil },
	}); err != nil {
		t.Fatal(err)
	}
	return srv, tr1, tr2
}

// An aborted start is not a stop: the axes it touched belong to the caller, and
// Run must leave them exactly where it found them.
func TestStartFailureLeavesAxesOpen(t *testing.T) {
	srv, tr1, tr2 := newAbortingServer(t)

	if err := srv.Run(context.Background()); !errors.Is(err, errCodecFactory) {
		t.Fatalf("Run: %v, want %v", err, errCodecFactory)
	}
	if tr1.isClosed() {
		t.Error("aborted Run closed the axis it had already assembled")
	}
	if tr2.isClosed() {
		t.Error("aborted Run closed the axis whose assembly failed")
	}
}

func TestStartFailureLeavesServerRestartable(t *testing.T) {
	srv, _, _ := newAbortingServer(t)

	if err := srv.Run(context.Background()); !errors.Is(err, errCodecFactory) {
		t.Fatalf("first Run: %v, want %v", err, errCodecFactory)
	}
	// No listen surface ever went live, so the second Run must report why the
	// start failed rather than reject the call as a restart.
	if err := srv.Run(context.Background()); !errors.Is(err, errCodecFactory) {
		t.Fatalf("second Run: %v, want %v", err, errCodecFactory)
	}
	other := descriptor.MustService("test.v1.Other",
		descriptor.MustMethod("test.v1.Other."+methodEcho, descriptor.Unary))
	if err := srv.Register(other, map[string]filter.Handler{
		methodEcho: func(context.Context, descriptor.Method, stream.Stream) error { return nil },
	}); err != nil {
		t.Fatalf("Register after a failed start: %v", err)
	}
}

func TestRejectedOptionsSurfaceFromRun(t *testing.T) {
	tr := newTestTransport()
	fr := fake.NewFraming(session.Sequential)
	srv := New(
		argos.WithServerMaxConcurrentCalls(-5),
		argos.WithServerService(svcName, testServiceBindListen(t, tr, fr, "127.0.0.1:0")),
	)
	if err := srv.Register(echoService(), map[string]filter.Handler{methodEcho: func(context.Context, descriptor.Method, stream.Stream) error {
		return nil
	}}); err != nil {
		t.Fatal(err)
	}
	runErr := srv.Run(context.Background())
	if runErr == nil {
		t.Fatal("Run succeeded on a Server built from a rejected option set")
	}
	if !strings.Contains(runErr.Error(), "MaxConcurrentCalls") {
		t.Fatalf("Run: %v, want the error to name MaxConcurrentCalls", runErr)
	}
}

// echoHandler reads one message and echoes it back.
func echoHandler(ctx context.Context, m descriptor.Method, st stream.Stream) error {
	var req []byte
	if err := st.Recv(&req); err != nil {
		return err
	}
	drainRecv(st)
	return st.Send(req)
}

// An axis belongs to whoever constructed it, and the server only ever serves on
// it. Neither stop path may close it, and the same instance has to keep serving
// afterwards.
func TestStopLeavesAxisUsable(t *testing.T) {
	tr := newTestTransport()
	srvFr := fake.NewFraming(session.Sequential)
	cliFr := fake.NewFraming(session.Sequential)
	// One axis instance, three servers: every stop has to leave it serving.
	axis := testServiceAxis(tr, srvFr)
	trName := teststack.TransportName(t, axis)
	serve := func() *Server {
		srv := New(argos.WithServerService(svcName,
			argos.ServiceBindListen("127.0.0.1:0", trName, testServerCodecName),
		))
		if err := srv.Register(echoService(), map[string]filter.Handler{methodEcho: echoHandler}); err != nil {
			t.Fatal(err)
		}
		return srv
	}

	// Stop path 1: Run's ctx is canceled.
	srv := serve()
	stop := startRun(t, srv)
	if out := echoOnce(t, tr, cliFr, "one"); out != "one" {
		t.Fatalf("first server echoed %q, want %q", out, "one")
	}
	stop()
	if tr.isClosed() {
		t.Fatal("canceling Run's ctx closed the axis the surface served on")
	}

	// Stop path 2: Shutdown.
	srv = serve()
	stop = startRun(t, srv)
	if out := echoOnce(t, tr, cliFr, "two"); out != "two" {
		t.Fatalf("second server echoed %q, want %q", out, "two")
	}
	if err := srv.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if tr.isClosed() {
		t.Fatal("Shutdown closed the axis the surface served on")
	}
	stop()

	// The same instance serves a third server: same transport, same framing.
	srv = serve()
	t.Cleanup(startRun(t, srv))
	if out := echoOnce(t, tr, cliFr, "three"); out != "three" {
		t.Fatalf("third server echoed %q, want %q", out, "three")
	}
	if tr.isClosed() {
		t.Fatal("stopping the servers closed the axis they served on")
	}
}

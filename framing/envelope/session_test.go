package envelope_test

import (
	"context"
	"errors"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/framing"
	"github.com/argos-io/argos/framing/envelope"
	"github.com/argos-io/argos/internal/fake"
	"github.com/argos-io/argos/metadata"
	"github.com/argos-io/argos/status"
)

func TestReuseIsSequential(t *testing.T) {
	t.Parallel()
	f := envelope.New()
	if f.Reuse() != framing.Sequential {
		t.Fatalf("Reuse = %v, want Sequential", f.Reuse())
	}
}

func TestUnaryRoundTrip(t *testing.T) {
	t.Parallel()
	cliConn, srvConn := fake.BytePipe()
	defer cliConn.Close()
	defer srvConn.Close()

	fr := envelope.New()
	cliSess, err := fr.NewClientSession(context.Background(), cliConn, framing.SessionSpec{})
	if err != nil {
		t.Fatal(err)
	}
	defer cliSess.Close()
	srvSess, err := fr.NewServerSession(context.Background(), srvConn, framing.SessionSpec{})
	if err != nil {
		t.Fatal(err)
	}
	defer srvSess.Close()

	method := descriptor.MustMethod("echo.v1.EchoService.Echo", descriptor.Unary)
	cliMD := metadata.New(metadata.RoleInitiator, nil)
	srvMD := metadata.New(metadata.RoleResponder, nil)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		sc, err := srvSess.AcceptCall(context.Background(), framing.CallSpec{Metadata: srvMD})
		if err != nil {
			t.Errorf("AcceptCall: %v", err)
			return
		}
		defer sc.Close()
		if sc.Method() != method.FullName() {
			t.Errorf("Method = %q", sc.Method())
		}
		payload, release, err := sc.Recv()
		if err != nil {
			t.Errorf("server Recv: %v", err)
			return
		}
		if string(payload) != "ping" {
			t.Errorf("server Recv = %q", payload)
		}
		release()
		if _, _, err := sc.Recv(); !errors.Is(err, io.EOF) {
			t.Errorf("server Recv after END = %v, want EOF", err)
		}
		if err := sc.Send([]byte("pong")); err != nil {
			t.Errorf("server Send: %v", err)
			return
		}
		if err := sc.Finish(nil); err != nil {
			t.Errorf("Finish: %v", err)
		}
	}()

	call, err := cliSess.OpenCall(context.Background(), method, framing.CallSpec{Metadata: cliMD})
	if err != nil {
		t.Fatal(err)
	}
	defer call.Close()
	if err := call.Send([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	if err := call.HalfClose(); err != nil {
		t.Fatal(err)
	}
	payload, release, err := call.Recv()
	if err != nil {
		t.Fatalf("client Recv: %v", err)
	}
	if string(payload) != "pong" {
		t.Fatalf("client Recv = %q, want pong", payload)
	}
	release()
	if _, _, err := call.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("client Recv status = %v, want EOF", err)
	}
	wg.Wait()
	if !cliSess.Reusable() {
		t.Fatal("client session should remain reusable after clean unary")
	}
}

func TestZeroMessageOpenEnd(t *testing.T) {
	t.Parallel()
	cliConn, srvConn := fake.BytePipe()
	defer cliConn.Close()
	defer srvConn.Close()

	fr := envelope.New()
	cliSess, err := fr.NewClientSession(context.Background(), cliConn, framing.SessionSpec{})
	if err != nil {
		t.Fatal(err)
	}
	defer cliSess.Close()
	srvSess, err := fr.NewServerSession(context.Background(), srvConn, framing.SessionSpec{})
	if err != nil {
		t.Fatal(err)
	}
	defer srvSess.Close()

	method := descriptor.MustMethod("svc.Empty", descriptor.Unary)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		md := metadata.New(metadata.RoleResponder, nil)
		sc, err := srvSess.AcceptCall(context.Background(), framing.CallSpec{Metadata: md})
		if err != nil {
			t.Errorf("AcceptCall: %v", err)
			return
		}
		defer sc.Close()
		if _, _, err := sc.Recv(); !errors.Is(err, io.EOF) {
			t.Errorf("first Recv = %v, want EOF", err)
		}
		_ = sc.Finish(nil)
	}()

	call, err := cliSess.OpenCall(context.Background(), method, framing.CallSpec{
		Metadata: metadata.New(metadata.RoleInitiator, nil),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer call.Close()
	if err := call.HalfClose(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := call.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("client Recv = %v, want EOF", err)
	}
	wg.Wait()
}

func TestEmptyDataMessage(t *testing.T) {
	t.Parallel()
	cliConn, srvConn := fake.BytePipe()
	defer cliConn.Close()
	defer srvConn.Close()

	fr := envelope.New()
	cliSess, _ := fr.NewClientSession(context.Background(), cliConn, framing.SessionSpec{})
	defer cliSess.Close()
	srvSess, _ := fr.NewServerSession(context.Background(), srvConn, framing.SessionSpec{})
	defer srvSess.Close()

	method := descriptor.MustMethod("svc.EmptyData", descriptor.Unary)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		md := metadata.New(metadata.RoleResponder, nil)
		sc, err := srvSess.AcceptCall(context.Background(), framing.CallSpec{Metadata: md})
		if err != nil {
			t.Errorf("AcceptCall: %v", err)
			return
		}
		defer sc.Close()
		payload, release, err := sc.Recv()
		if err != nil {
			t.Errorf("Recv: %v", err)
			return
		}
		if payload == nil || len(payload) != 0 {
			t.Errorf("empty DATA: payload=%v nil=%v", payload, payload == nil)
		}
		release()
		_, _, _ = sc.Recv() // END → EOF
		_ = sc.Finish(nil)
	}()

	call, err := cliSess.OpenCall(context.Background(), method, framing.CallSpec{
		Metadata: metadata.New(metadata.RoleInitiator, nil),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer call.Close()
	if err := call.Send([]byte{}); err != nil {
		t.Fatal(err)
	}
	if err := call.HalfClose(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := call.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("Recv = %v, want EOF", err)
	}
	wg.Wait()
}

func TestSendHeadersEmptyDelivers(t *testing.T) {
	t.Parallel()
	cliConn, srvConn := fake.BytePipe()
	defer cliConn.Close()
	defer srvConn.Close()

	fr := envelope.New()
	cliSess, _ := fr.NewClientSession(context.Background(), cliConn, framing.SessionSpec{})
	defer cliSess.Close()
	srvSess, _ := fr.NewServerSession(context.Background(), srvConn, framing.SessionSpec{})
	defer srvSess.Close()

	method := descriptor.MustMethod("svc.Headers", descriptor.Unary)
	cliMD := metadata.New(metadata.RoleInitiator, nil)
	srvMD := metadata.New(metadata.RoleResponder, nil)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		sc, err := srvSess.AcceptCall(context.Background(), framing.CallSpec{Metadata: srvMD})
		if err != nil {
			t.Errorf("AcceptCall: %v", err)
			return
		}
		defer sc.Close()
		// Empty SendHeaders must still write a HEADERS frame.
		if err := sc.SendHeaders(); err != nil {
			t.Errorf("SendHeaders: %v", err)
			return
		}
		if err := sc.Send([]byte("after-headers")); err != nil {
			t.Errorf("Send: %v", err)
			return
		}
		_, _, _ = sc.Recv()
		_ = sc.Finish(nil)
	}()

	call, err := cliSess.OpenCall(context.Background(), method, framing.CallSpec{Metadata: cliMD})
	if err != nil {
		t.Fatal(err)
	}
	defer call.Close()
	if err := call.HalfClose(); err != nil {
		t.Fatal(err)
	}

	payload, release, err := call.Recv()
	if err != nil {
		t.Fatalf("Recv: %v", err)
	}
	release()
	if string(payload) != "after-headers" {
		t.Fatalf("Recv = %q", payload)
	}
	// HEADERS (even empty) must have been applied before DATA.
	if cliMD.IncomingHeaders() == nil {
		t.Fatal("IncomingHeaders nil after explicit empty SendHeaders")
	}
	if _, _, err := call.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("status Recv = %v, want EOF", err)
	}
	wg.Wait()
}

func TestFourShapesSmoke(t *testing.T) {
	t.Parallel()
	shapes := []struct {
		name  string
		shape descriptor.Shape
		run   func(t *testing.T, cli framing.Call, srv framing.ServerCall)
	}{
		{
			name:  "unary",
			shape: descriptor.Unary,
			run: func(t *testing.T, cli framing.Call, srv framing.ServerCall) {
				go func() {
					p, rel, err := srv.Recv()
					if err != nil {
						t.Errorf("srv Recv: %v", err)
						return
					}
					rel()
					_, _, _ = srv.Recv()
					_ = srv.Send(append([]byte("echo:"), p...))
					_ = srv.Finish(nil)
				}()
				if err := cli.Send([]byte("u")); err != nil {
					t.Fatal(err)
				}
				if err := cli.HalfClose(); err != nil {
					t.Fatal(err)
				}
				p, rel, err := cli.Recv()
				if err != nil {
					t.Fatal(err)
				}
				rel()
				if string(p) != "echo:u" {
					t.Fatalf("got %q", p)
				}
				_, _, _ = cli.Recv()
			},
		},
		{
			name:  "server-stream",
			shape: descriptor.ServerStreaming,
			run: func(t *testing.T, cli framing.Call, srv framing.ServerCall) {
				go func() {
					_, _, _ = srv.Recv()
					_, _, _ = srv.Recv()
					_ = srv.Send([]byte("a"))
					_ = srv.Send([]byte("b"))
					_ = srv.Finish(nil)
				}()
				if err := cli.Send([]byte("req")); err != nil {
					t.Fatal(err)
				}
				if err := cli.HalfClose(); err != nil {
					t.Fatal(err)
				}
				for _, want := range []string{"a", "b"} {
					p, rel, err := cli.Recv()
					if err != nil {
						t.Fatal(err)
					}
					if string(p) != want {
						t.Fatalf("got %q want %q", p, want)
					}
					rel()
				}
				_, _, _ = cli.Recv()
			},
		},
		{
			name:  "client-stream",
			shape: descriptor.ClientStreaming,
			run: func(t *testing.T, cli framing.Call, srv framing.ServerCall) {
				go func() {
					var n int
					for {
						p, rel, err := srv.Recv()
						if errors.Is(err, io.EOF) {
							break
						}
						if err != nil {
							t.Errorf("srv Recv: %v", err)
							return
						}
						n += len(p)
						rel()
					}
					_ = srv.Send([]byte{byte(n)})
					_ = srv.Finish(nil)
				}()
				if err := cli.Send([]byte("ab")); err != nil {
					t.Fatal(err)
				}
				if err := cli.Send([]byte("cd")); err != nil {
					t.Fatal(err)
				}
				if err := cli.HalfClose(); err != nil {
					t.Fatal(err)
				}
				p, rel, err := cli.Recv()
				if err != nil {
					t.Fatal(err)
				}
				if len(p) != 1 || p[0] != 4 {
					t.Fatalf("got %v", p)
				}
				rel()
				_, _, _ = cli.Recv()
			},
		},
		{
			name:  "bidi",
			shape: descriptor.BidiStreaming,
			run: func(t *testing.T, cli framing.Call, srv framing.ServerCall) {
				go func() {
					p, rel, err := srv.Recv()
					if err != nil {
						t.Errorf("srv Recv: %v", err)
						return
					}
					rel()
					_ = srv.Send(append([]byte("R"), p...))
					p2, rel2, err := srv.Recv()
					if err != nil {
						t.Errorf("srv Recv2: %v", err)
						return
					}
					rel2()
					_ = srv.Send(append([]byte("R"), p2...))
					_, _, _ = srv.Recv()
					_ = srv.Finish(nil)
				}()
				if err := cli.Send([]byte("1")); err != nil {
					t.Fatal(err)
				}
				p, rel, err := cli.Recv()
				if err != nil {
					t.Fatal(err)
				}
				if string(p) != "R1" {
					t.Fatalf("got %q", p)
				}
				rel()
				if err := cli.Send([]byte("2")); err != nil {
					t.Fatal(err)
				}
				p, rel, err = cli.Recv()
				if err != nil {
					t.Fatal(err)
				}
				if string(p) != "R2" {
					t.Fatalf("got %q", p)
				}
				rel()
				if err := cli.HalfClose(); err != nil {
					t.Fatal(err)
				}
				_, _, _ = cli.Recv()
			},
		},
	}

	for _, tc := range shapes {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			cliConn, srvConn := fake.BytePipe()
			defer cliConn.Close()
			defer srvConn.Close()
			fr := envelope.New()
			cliSess, err := fr.NewClientSession(context.Background(), cliConn, framing.SessionSpec{})
			if err != nil {
				t.Fatal(err)
			}
			defer cliSess.Close()
			srvSess, err := fr.NewServerSession(context.Background(), srvConn, framing.SessionSpec{})
			if err != nil {
				t.Fatal(err)
			}
			defer srvSess.Close()

			method := descriptor.MustMethod("svc."+tc.name, tc.shape)
			accepted := make(chan framing.ServerCall, 1)
			go func() {
				md := metadata.New(metadata.RoleResponder, nil)
				sc, err := srvSess.AcceptCall(context.Background(), framing.CallSpec{Metadata: md})
				if err != nil {
					t.Errorf("AcceptCall: %v", err)
					close(accepted)
					return
				}
				accepted <- sc
			}()

			call, err := cliSess.OpenCall(context.Background(), method, framing.CallSpec{
				Metadata: metadata.New(metadata.RoleInitiator, nil),
			})
			if err != nil {
				t.Fatal(err)
			}
			sc := <-accepted
			if sc == nil {
				t.Fatal("no server call")
			}
			defer call.Close()
			defer sc.Close()
			if err := sc.Accept(method); err != nil {
				t.Fatal(err)
			}
			tc.run(t, call, sc)
		})
	}
}

func TestHalfCloseDoesNotCloseConn(t *testing.T) {
	t.Parallel()
	cliConn, srvConn := fake.BytePipe()
	defer cliConn.Close()
	defer srvConn.Close()

	fr := envelope.New()
	cliSess, _ := fr.NewClientSession(context.Background(), cliConn, framing.SessionSpec{})
	defer cliSess.Close()
	srvSess, _ := fr.NewServerSession(context.Background(), srvConn, framing.SessionSpec{})
	defer srvSess.Close()

	method := descriptor.MustMethod("svc.HC", descriptor.Unary)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		md := metadata.New(metadata.RoleResponder, nil)
		sc, err := srvSess.AcceptCall(context.Background(), framing.CallSpec{Metadata: md})
		if err != nil {
			t.Errorf("AcceptCall: %v", err)
			return
		}
		defer sc.Close()
		_, _, _ = sc.Recv()
		_ = sc.Finish(nil)
	}()

	call, err := cliSess.OpenCall(context.Background(), method, framing.CallSpec{
		Metadata: metadata.New(metadata.RoleInitiator, nil),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := call.Send([]byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := call.HalfClose(); err != nil {
		t.Fatal(err)
	}
	if cliConn.CloseCount() != 0 {
		t.Fatalf("client CloseCount = %d after HalfClose, want 0", cliConn.CloseCount())
	}
	if srvConn.CloseCount() != 0 {
		t.Fatalf("server CloseCount = %d after HalfClose, want 0", srvConn.CloseCount())
	}
	_, _, _ = call.Recv()
	_ = call.Close()
	wg.Wait()
	if cliConn.CloseCount() != 0 {
		t.Fatalf("client CloseCount = %d after Call.Close, want 0", cliConn.CloseCount())
	}
	if srvConn.CloseCount() != 0 {
		t.Fatalf("server CloseCount = %d after Call.Close, want 0", srvConn.CloseCount())
	}
}

func TestStatusErrorThenEOF(t *testing.T) {
	t.Parallel()
	cliConn, srvConn := fake.BytePipe()
	defer cliConn.Close()
	defer srvConn.Close()

	fr := envelope.New()
	cliSess, _ := fr.NewClientSession(context.Background(), cliConn, framing.SessionSpec{})
	defer cliSess.Close()
	srvSess, _ := fr.NewServerSession(context.Background(), srvConn, framing.SessionSpec{})
	defer srvSess.Close()

	method := descriptor.MustMethod("svc.Err", descriptor.Unary)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		md := metadata.New(metadata.RoleResponder, nil)
		sc, err := srvSess.AcceptCall(context.Background(), framing.CallSpec{Metadata: md})
		if err != nil {
			t.Errorf("AcceptCall: %v", err)
			return
		}
		defer sc.Close()
		_, _, _ = sc.Recv()
		_ = sc.Finish(status.Error(status.NotFound, "missing"))
	}()

	call, err := cliSess.OpenCall(context.Background(), method, framing.CallSpec{
		Metadata: metadata.New(metadata.RoleInitiator, nil),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer call.Close()
	_ = call.HalfClose()
	_, _, err = call.Recv()
	if status.CodeOf(err) != status.NotFound {
		t.Fatalf("Recv = %v, want NotFound", err)
	}
	_, _, err = call.Recv()
	if !errors.Is(err, io.EOF) {
		t.Fatalf("second Recv = %v, want EOF", err)
	}
	wg.Wait()
}

// TestIdleWatchdogPeerFINNotReusable verifies §2.6 Sequential idle watchdog:
// after a clean unary, peer FIN makes Reusable() false without another OpenCall.
func TestIdleWatchdogPeerFINNotReusable(t *testing.T) {
	t.Parallel()
	cliConn, srvConn := fake.BytePipe()
	defer cliConn.Close()
	defer srvConn.Close()

	fr := envelope.New()
	cliSess, err := fr.NewClientSession(context.Background(), cliConn, framing.SessionSpec{})
	if err != nil {
		t.Fatal(err)
	}
	defer cliSess.Close()
	srvSess, err := fr.NewServerSession(context.Background(), srvConn, framing.SessionSpec{})
	if err != nil {
		t.Fatal(err)
	}

	method := descriptor.MustMethod("svc.IdleFIN", descriptor.Unary)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		sc, err := srvSess.AcceptCall(context.Background(), framing.CallSpec{
			Metadata: metadata.New(metadata.RoleResponder, nil),
		})
		if err != nil {
			t.Errorf("AcceptCall: %v", err)
			return
		}
		defer sc.Close()
		payload, release, err := sc.Recv()
		if err != nil {
			t.Errorf("server Recv: %v", err)
			return
		}
		release()
		_ = payload
		if _, _, err := sc.Recv(); !errors.Is(err, io.EOF) {
			t.Errorf("server Recv after END = %v", err)
		}
		if err := sc.Send([]byte("pong")); err != nil {
			t.Errorf("server Send: %v", err)
			return
		}
		if err := sc.Finish(nil); err != nil {
			t.Errorf("Finish: %v", err)
		}
	}()

	call, err := cliSess.OpenCall(context.Background(), method, framing.CallSpec{
		Metadata: metadata.New(metadata.RoleInitiator, nil),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := call.Send([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	if err := call.HalfClose(); err != nil {
		t.Fatal(err)
	}
	payload, release, err := call.Recv()
	if err != nil {
		t.Fatal(err)
	}
	if string(payload) != "pong" {
		t.Fatalf("Recv = %q, want pong", payload)
	}
	release()
	_, _, err = call.Recv()
	if !errors.Is(err, io.EOF) {
		t.Fatalf("status Recv = %v, want EOF", err)
	}
	if err := call.Close(); err != nil {
		t.Fatal(err)
	}
	wg.Wait()

	if !cliSess.Reusable() {
		t.Fatal("client session should be reusable after clean unary")
	}

	// Peer closes while client session is idle (pooled between calls).
	_ = srvSess.Close()

	deadline := time.Now().Add(2 * time.Second)
	for cliSess.Reusable() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if cliSess.Reusable() {
		t.Fatal("Reusable() still true after peer FIN; idle watchdog should clear it")
	}
}

// TestIdleWatchdogUnexpectedByteNotReusable: idle traffic is a protocol violation.
func TestIdleWatchdogUnexpectedByteNotReusable(t *testing.T) {
	t.Parallel()
	cliConn, srvConn := fake.BytePipe()
	defer cliConn.Close()
	defer srvConn.Close()

	fr := envelope.New()
	cliSess, err := fr.NewClientSession(context.Background(), cliConn, framing.SessionSpec{})
	if err != nil {
		t.Fatal(err)
	}
	defer cliSess.Close()
	srvSess, err := fr.NewServerSession(context.Background(), srvConn, framing.SessionSpec{})
	if err != nil {
		t.Fatal(err)
	}
	defer srvSess.Close()

	method := descriptor.MustMethod("svc.IdleByte", descriptor.Unary)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		sc, err := srvSess.AcceptCall(context.Background(), framing.CallSpec{
			Metadata: metadata.New(metadata.RoleResponder, nil),
		})
		if err != nil {
			t.Errorf("AcceptCall: %v", err)
			return
		}
		defer sc.Close()
		_, release, err := sc.Recv()
		if err != nil {
			t.Errorf("server Recv: %v", err)
			return
		}
		release()
		_, _, _ = sc.Recv()
		_ = sc.Send([]byte("ok"))
		_ = sc.Finish(nil)
	}()

	call, err := cliSess.OpenCall(context.Background(), method, framing.CallSpec{
		Metadata: metadata.New(metadata.RoleInitiator, nil),
	})
	if err != nil {
		t.Fatal(err)
	}
	_ = call.Send([]byte("x"))
	_ = call.HalfClose()
	_, release, err := call.Recv()
	if err != nil {
		t.Fatal(err)
	}
	release()
	_, _, _ = call.Recv()
	_ = call.Close()
	wg.Wait()

	if !cliSess.Reusable() {
		t.Fatal("want reusable after clean unary")
	}

	// Junk byte on an idle client session.
	if _, err := srvConn.Write([]byte{0x7f}); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for cliSess.Reusable() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if cliSess.Reusable() {
		t.Fatal("Reusable() still true after unexpected idle byte")
	}
}

// TestOpenTimeoutHalfOpenFrame: OpenTimeout fires after a partial OPEN, but a
// fully idle AcceptCall is not killed by OpenTimeout alone (§2.6).
func TestOpenTimeoutHalfOpenFrame(t *testing.T) {
	t.Parallel()
	openTO := 80 * time.Millisecond
	fr := envelope.New(envelope.WithOpenTimeout(openTO))
	cliConn, srvConn := fake.BytePipe()
	defer cliConn.Close()
	defer srvConn.Close()

	srvSess, err := fr.NewServerSession(context.Background(), srvConn, framing.SessionSpec{})
	if err != nil {
		t.Fatal(err)
	}
	defer srvSess.Close()

	errCh := make(chan error, 1)
	go func() {
		_, err := srvSess.AcceptCall(context.Background(), framing.CallSpec{
			Metadata: metadata.New(metadata.RoleResponder, nil),
		})
		errCh <- err
	}()

	// Fully idle: OpenTimeout must not fire (no first byte yet).
	select {
	case err := <-errCh:
		t.Fatalf("AcceptCall returned while fully idle: %v", err)
	case <-time.After(3 * openTO):
	}

	// Incomplete length-prefix (2 of 4 bytes) then hang — OpenTimeout starts
	// after the first byte and must fail AcceptCall.
	if _, err := cliConn.Write([]byte{0x00, 0x00}); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("AcceptCall succeeded on half-OPEN; want timeout error")
		}
		if !errors.Is(err, os.ErrDeadlineExceeded) && !isNetTimeout(err) {
			t.Fatalf("AcceptCall = %v, want read deadline/timeout after half-OPEN", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("AcceptCall did not return after half-OPEN OpenTimeout")
	}
}

func isNetTimeout(err error) bool {
	var ne interface{ Timeout() bool }
	return errors.As(err, &ne) && ne.Timeout()
}

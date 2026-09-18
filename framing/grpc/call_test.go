package grpc

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/framing"
	"github.com/argos-io/argos/internal/fake"
	"github.com/argos-io/argos/metadata"
	"github.com/argos-io/argos/status"
	"github.com/argos-io/argos/transport"
)

func newTestFraming(t *testing.T, opts ...Option) framing.Framing {
	t.Helper()
	f, err := New(opts...)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func TestFourShapesSmokeHTTPLoopback(t *testing.T) {
	t.Parallel()
	shapes := []descriptor.Shape{
		descriptor.Unary,
		descriptor.ServerStreaming,
		descriptor.ClientStreaming,
		descriptor.BidiStreaming,
	}
	for _, shape := range shapes {
		shape := shape
		t.Run(shapeName(shape), func(t *testing.T) {
			t.Parallel()
			runShapeSmoke(t, shape)
		})
	}
}

func shapeName(s descriptor.Shape) string {
	switch s {
	case descriptor.Unary:
		return "Unary"
	case descriptor.ServerStreaming:
		return "ServerStreaming"
	case descriptor.ClientStreaming:
		return "ClientStreaming"
	case descriptor.BidiStreaming:
		return "BidiStreaming"
	default:
		return "unknown"
	}
}

func runShapeSmoke(t *testing.T, shape descriptor.Shape) {
	t.Helper()
	cli, lis := fake.HTTPLoopback()
	t.Cleanup(func() { _ = cli.Close() })

	f := newTestFraming(t)
	method := descriptor.MustMethod("echo.v1.Echo.Echo", shape)
	spec := framing.SessionSpec{CodecName: "proto"}

	errCh := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srvConn, err := lis.Accept(ctx)
		if err != nil {
			errCh <- err
			return
		}
		ss, err := f.NewServerSession(ctx, srvConn, spec)
		if err != nil {
			errCh <- err
			return
		}
		defer ss.Close()

		md := metadata.New(metadata.RoleResponder, nil)
		sc, err := ss.AcceptCall(ctx, framing.CallSpec{Metadata: md})
		if err != nil {
			errCh <- err
			return
		}
		if err := sc.Accept(method); err != nil {
			errCh <- err
			return
		}

		switch shape {
		case descriptor.Unary, descriptor.ClientStreaming:
			var last []byte
			for {
				p, release, err := sc.Recv()
				if err == io.EOF {
					break
				}
				if err != nil {
					errCh <- err
					return
				}
				last = append([]byte(nil), p...)
				release()
			}
			if err := sc.Send([]byte("resp:" + string(last))); err != nil {
				errCh <- err
				return
			}
			errCh <- sc.Finish(nil)
		case descriptor.ServerStreaming:
			p, release, err := sc.Recv()
			if err != nil {
				errCh <- err
				return
			}
			req := append([]byte(nil), p...)
			release()
			_, _, err = sc.Recv()
			if err != io.EOF {
				errCh <- err
				return
			}
			if err := sc.Send([]byte(string(req) + "-1")); err != nil {
				errCh <- err
				return
			}
			if err := sc.Send([]byte(string(req) + "-2")); err != nil {
				errCh <- err
				return
			}
			errCh <- sc.Finish(nil)
		case descriptor.BidiStreaming:
			p, release, err := sc.Recv()
			if err != nil {
				errCh <- err
				return
			}
			req := append([]byte(nil), p...)
			release()
			if err := sc.Send([]byte("echo:" + string(req))); err != nil {
				errCh <- err
				return
			}
			_, _, err = sc.Recv()
			if err != io.EOF {
				errCh <- err
				return
			}
			errCh <- sc.Finish(nil)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cs, err := f.NewClientSession(ctx, cli, spec)
	if err != nil {
		t.Fatalf("NewClientSession: %v", err)
	}
	defer cs.Close()

	md := metadata.New(metadata.RoleInitiator, nil)
	call, err := cs.OpenCall(ctx, method, framing.CallSpec{Metadata: md})
	if err != nil {
		t.Fatalf("OpenCall: %v", err)
	}
	defer call.Close()

	switch shape {
	case descriptor.Unary:
		if err := call.Send([]byte("ping")); err != nil {
			t.Fatalf("Send: %v", err)
		}
		if err := call.HalfClose(); err != nil {
			t.Fatalf("HalfClose: %v", err)
		}
		p, release, err := call.Recv()
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		if string(p) != "resp:ping" {
			t.Fatalf("got %q", p)
		}
		release()
		_, _, err = call.Recv()
		if err != io.EOF {
			t.Fatalf("Recv EOF: %v", err)
		}
	case descriptor.ClientStreaming:
		if err := call.Send([]byte("a")); err != nil {
			t.Fatal(err)
		}
		if err := call.Send([]byte("b")); err != nil {
			t.Fatal(err)
		}
		if err := call.HalfClose(); err != nil {
			t.Fatal(err)
		}
		p, release, err := call.Recv()
		if err != nil {
			t.Fatal(err)
		}
		if string(p) != "resp:b" {
			t.Fatalf("got %q", p)
		}
		release()
		_, _, err = call.Recv()
		if err != io.EOF {
			t.Fatalf("want EOF, got %v", err)
		}
	case descriptor.ServerStreaming:
		if err := call.Send([]byte("x")); err != nil {
			t.Fatal(err)
		}
		if err := call.HalfClose(); err != nil {
			t.Fatal(err)
		}
		for _, want := range []string{"x-1", "x-2"} {
			p, release, err := call.Recv()
			if err != nil {
				t.Fatalf("Recv %s: %v", want, err)
			}
			if string(p) != want {
				t.Fatalf("got %q want %q", p, want)
			}
			release()
		}
		_, _, err = call.Recv()
		if err != io.EOF {
			t.Fatalf("want EOF, got %v", err)
		}
	case descriptor.BidiStreaming:
		if err := call.Send([]byte("z")); err != nil {
			t.Fatal(err)
		}
		p, release, err := call.Recv()
		if err != nil {
			t.Fatal(err)
		}
		if string(p) != "echo:z" {
			t.Fatalf("got %q", p)
		}
		release()
		if err := call.HalfClose(); err != nil {
			t.Fatal(err)
		}
		_, _, err = call.Recv()
		if err != io.EOF {
			t.Fatalf("want EOF, got %v", err)
		}
	}

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("server: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server timed out")
	}
}

func TestTrailersOnlyError(t *testing.T) {
	t.Parallel()
	cli, lis := fake.HTTPLoopback()
	t.Cleanup(func() { _ = cli.Close() })
	f := newTestFraming(t)
	method := descriptor.MustMethod("echo.v1.Echo.Echo", descriptor.Unary)
	spec := framing.SessionSpec{CodecName: "proto"}

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srvConn, err := lis.Accept(ctx)
		if err != nil {
			return
		}
		ss, err := f.NewServerSession(ctx, srvConn, spec)
		if err != nil {
			return
		}
		defer ss.Close()
		sc, err := ss.AcceptCall(ctx, framing.CallSpec{
			Metadata: metadata.New(metadata.RoleResponder, nil),
		})
		if err != nil {
			return
		}
		_ = sc.Accept(method)
		_, _, _ = sc.Recv() // wait for client half-close / empty body EOF
		_ = sc.Finish(status.Error(status.NotFound, "missing resource"))
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cs, err := f.NewClientSession(ctx, cli, spec)
	if err != nil {
		t.Fatal(err)
	}
	defer cs.Close()
	call, err := cs.OpenCall(ctx, method, framing.CallSpec{
		Metadata: metadata.New(metadata.RoleInitiator, nil),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer call.Close()
	if err := call.HalfClose(); err != nil {
		t.Fatal(err)
	}
	_, _, err = call.Recv()
	if status.CodeOf(err) != status.NotFound {
		t.Fatalf("CodeOf = %v, want NotFound; err=%v", status.CodeOf(err), err)
	}
	if err.Error() != "missing resource" {
		t.Fatalf("message = %q", err.Error())
	}
}

func TestHTTPFallbackMissingGrpcStatus(t *testing.T) {
	t.Parallel()
	cli, lis := fake.HTTPLoopback()
	t.Cleanup(func() { _ = cli.Close() })

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srvConn, err := lis.Accept(ctx)
		if err != nil {
			return
		}
		car := srvConn.Carrier().(interface {
			transport.ByteStreamCarrier
			transport.ResponseWriter
			transport.SendCloser
		})
		_, _ = io.Copy(io.Discard, car)
		// Plain HTTP response: headers + body, no grpc-status trailers.
		_ = car.WriteHeaders(404, transport.Headers{
			{Name: "content-type", Value: "text/plain"},
		})
		_, _ = car.Write([]byte("not found"))
		_ = car.Finish(404, nil, nil)
	}()

	f := newTestFraming(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cs, err := f.NewClientSession(ctx, cli, framing.SessionSpec{CodecName: "proto"})
	if err != nil {
		t.Fatal(err)
	}
	defer cs.Close()
	method := descriptor.MustMethod("echo.v1.Echo.Echo", descriptor.Unary)
	call, err := cs.OpenCall(ctx, method, framing.CallSpec{
		Metadata: metadata.New(metadata.RoleInitiator, nil),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer call.Close()
	_ = call.HalfClose()

	_, _, err = call.Recv()
	if status.CodeOf(err) != status.NotFound {
		t.Fatalf("CodeOf = %v, want NotFound (HTTP 404 fallback); err=%v", status.CodeOf(err), err)
	}
}

func TestHTTPFallback200WithoutGrpcStatus(t *testing.T) {
	t.Parallel()
	cli, lis := fake.HTTPLoopback()
	t.Cleanup(func() { _ = cli.Close() })

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srvConn, err := lis.Accept(ctx)
		if err != nil {
			return
		}
		car := srvConn.Carrier().(interface {
			transport.ByteStreamCarrier
			transport.ResponseWriter
		})
		_, _ = io.Copy(io.Discard, car)
		_ = car.WriteHeaders(200, transport.Headers{
			{Name: "content-type", Value: "application/grpc"},
		})
		_ = car.Finish(200, nil, nil) // no grpc-status
	}()

	f := newTestFraming(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cs, err := f.NewClientSession(ctx, cli, framing.SessionSpec{})
	if err != nil {
		t.Fatal(err)
	}
	defer cs.Close()
	call, err := cs.OpenCall(ctx, descriptor.MustMethod("svc.M", descriptor.Unary), framing.CallSpec{
		Metadata: metadata.New(metadata.RoleInitiator, nil),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer call.Close()
	_ = call.HalfClose()
	_, _, err = call.Recv()
	if status.CodeOf(err) != status.Unknown {
		t.Fatalf("CodeOf = %v, want Unknown for HTTP 200 without grpc-status; err=%v", status.CodeOf(err), err)
	}
	if !errors.Is(err, status.Error(status.Unknown, "")) && status.CodeOf(err) != status.Unknown {
		t.Fatalf("err = %v", err)
	}
}

func TestEmptyLPMMessageOnCall(t *testing.T) {
	t.Parallel()
	cli, lis := fake.HTTPLoopback()
	t.Cleanup(func() { _ = cli.Close() })
	f := newTestFraming(t)
	method := descriptor.MustMethod("echo.v1.Echo.Echo", descriptor.Unary)
	spec := framing.SessionSpec{CodecName: "proto"}

	errCh := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srvConn, err := lis.Accept(ctx)
		if err != nil {
			errCh <- err
			return
		}
		ss, _ := f.NewServerSession(ctx, srvConn, spec)
		defer ss.Close()
		sc, err := ss.AcceptCall(ctx, framing.CallSpec{
			Metadata: metadata.New(metadata.RoleResponder, nil),
		})
		if err != nil {
			errCh <- err
			return
		}
		_ = sc.Accept(method)
		p, release, err := sc.Recv()
		if err != nil {
			errCh <- err
			return
		}
		if len(p) != 0 {
			errCh <- errors.New("want empty payload")
			return
		}
		release()
		_, _, err = sc.Recv()
		if err != io.EOF {
			errCh <- err
			return
		}
		if err := sc.Send(nil); err != nil { // empty response message
			errCh <- err
			return
		}
		errCh <- sc.Finish(nil)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cs, _ := f.NewClientSession(ctx, cli, spec)
	defer cs.Close()
	call, err := cs.OpenCall(ctx, method, framing.CallSpec{
		Metadata: metadata.New(metadata.RoleInitiator, nil),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer call.Close()
	if err := call.Send(nil); err != nil {
		t.Fatal(err)
	}
	if err := call.HalfClose(); err != nil {
		t.Fatal(err)
	}
	p, release, err := call.Recv()
	if err != nil {
		t.Fatalf("Recv empty response: %v", err)
	}
	if len(p) != 0 {
		t.Fatalf("len = %d", len(p))
	}
	release()
	_, _, err = call.Recv()
	if err != io.EOF {
		t.Fatalf("want EOF, got %v", err)
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
}

func TestPercentEncodedStatusMessage(t *testing.T) {
	t.Parallel()
	cli, lis := fake.HTTPLoopback()
	t.Cleanup(func() { _ = cli.Close() })
	f := newTestFraming(t)
	method := descriptor.MustMethod("echo.v1.Echo.Echo", descriptor.Unary)

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srvConn, _ := lis.Accept(ctx)
		ss, _ := f.NewServerSession(ctx, srvConn, framing.SessionSpec{})
		defer ss.Close()
		sc, _ := ss.AcceptCall(ctx, framing.CallSpec{
			Metadata: metadata.New(metadata.RoleResponder, nil),
		})
		_, _, _ = sc.Recv()
		_ = sc.Finish(status.Error(status.InvalidArgument, "bad%value\tand café"))
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cs, _ := f.NewClientSession(ctx, cli, framing.SessionSpec{})
	defer cs.Close()
	call, _ := cs.OpenCall(ctx, method, framing.CallSpec{
		Metadata: metadata.New(metadata.RoleInitiator, nil),
	})
	defer call.Close()
	_ = call.HalfClose()
	_, _, err := call.Recv()
	if status.CodeOf(err) != status.InvalidArgument {
		t.Fatalf("code = %v err=%v", status.CodeOf(err), err)
	}
	if err.Error() != "bad%value\tand café" {
		t.Fatalf("message = %q", err.Error())
	}
}

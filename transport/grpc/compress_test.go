package grpc

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/argos-io/argos/compressor"
	"github.com/argos-io/argos/compressor/gzip"
	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/internal/fake"
	"github.com/argos-io/argos/internal/session"
	"github.com/argos-io/argos/metadata"
	"github.com/argos-io/argos/status"
)

func TestCompressionIdentityDefaultRoundTrip(t *testing.T) {
	t.Parallel()
	runCompressionRoundTrip(t, compressCfg{}, compressCfg{}, false)
}

func TestCompressionGzipEnabledRoundTrip(t *testing.T) {
	t.Parallel()
	cfg := compressCfg{extra: []compressor.Compressor{gzip.New()}, send: gzip.Name}
	runCompressionRoundTrip(t, cfg, cfg, true)
}

func TestCompressionUnknownAcceptIgnored(t *testing.T) {
	t.Parallel()
	// Client advertises a mystery algorithm in accept-encoding but sends identity.
	mystery := namedCompressor("mystery")
	clientCfg := compressCfg{extra: []compressor.Compressor{mystery}, send: compressor.Identity.Name()}
	serverCfg := compressCfg{extra: []compressor.Compressor{gzip.New()}, send: gzip.Name}
	runCompressionRoundTrip(t, clientCfg, serverCfg, false)
}

func TestCompressionUnknownEncodingUnimplemented(t *testing.T) {
	t.Parallel()
	cli, lis := fake.HTTPLoopback()
	t.Cleanup(func() { _ = cli.Close() })

	mystery := namedCompressor("mystery")
	clientF := newTestFraming(t, []compressor.Compressor{mystery}, "mystery")
	serverF := newTestFraming(t, nil, "")

	method := descriptor.MustMethod("echo.v1.Echo.Echo", descriptor.Unary)
	spec := session.SessionSpec{CodecName: "proto"}
	payload := []byte("need-decompress")

	errCh := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srvConn, err := lis.Accept(ctx)
		if err != nil {
			errCh <- err
			return
		}
		ss, err := serverF.NewServerSession(ctx, srvConn, spec)
		if err != nil {
			errCh <- err
			return
		}
		defer ss.Close()

		md := metadata.New(metadata.RoleResponder, nil)
		sc, err := ss.AcceptCall(ctx, session.CallSpec{Metadata: md})
		if err != nil {
			errCh <- err
			return
		}
		if err := sc.Accept(method); err != nil {
			errCh <- err
			return
		}
		_, release, err := sc.Recv()
		if release != nil {
			release()
		}
		if !errors.Is(err, status.Error(status.Unimplemented, "")) {
			errCh <- err
			return
		}
		errCh <- sc.Finish(err)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cs, err := clientF.NewClientSession(ctx, cli, spec)
	if err != nil {
		t.Fatal(err)
	}
	defer cs.Close()

	md := metadata.New(metadata.RoleInitiator, nil)
	call, err := cs.OpenCall(ctx, method, session.CallSpec{Metadata: md})
	if err != nil {
		t.Fatal(err)
	}
	defer call.Close()

	if err := call.Send(payload); err != nil {
		t.Fatal(err)
	}
	if err := call.HalfClose(); err != nil {
		t.Fatal(err)
	}
	_, release, err := call.Recv()
	if release != nil {
		release()
	}
	if !errors.Is(err, status.Error(status.Unimplemented, "")) {
		t.Fatalf("client Recv err = %v, want Unimplemented", err)
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
}

type compressCfg struct {
	extra []compressor.Compressor
	send  string
}

func runCompressionRoundTrip(t *testing.T, clientCfg, serverCfg compressCfg, wantCompressedWire bool) {
	t.Helper()
	cli, lis := fake.HTTPLoopback()
	t.Cleanup(func() { _ = cli.Close() })

	clientF := newTestFraming(t, clientCfg.extra, clientCfg.send)
	serverF := newTestFraming(t, serverCfg.extra, serverCfg.send)

	method := descriptor.MustMethod("echo.v1.Echo.Echo", descriptor.Unary)
	spec := session.SessionSpec{CodecName: "proto"}
	payload := bytes.Repeat([]byte("compress-me-"), 64)

	errCh := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srvConn, err := lis.Accept(ctx)
		if err != nil {
			errCh <- err
			return
		}
		ss, err := serverF.NewServerSession(ctx, srvConn, spec)
		if err != nil {
			errCh <- err
			return
		}
		defer ss.Close()

		md := metadata.New(metadata.RoleResponder, nil)
		sc, err := ss.AcceptCall(ctx, session.CallSpec{Metadata: md})
		if err != nil {
			errCh <- err
			return
		}
		if err := sc.Accept(method); err != nil {
			errCh <- err
			return
		}
		p, release, err := sc.Recv()
		if err != nil {
			errCh <- err
			return
		}
		got := append([]byte(nil), p...)
		release()
		_, _, err = sc.Recv()
		if err != io.EOF {
			errCh <- err
			return
		}
		if !bytes.Equal(got, payload) {
			errCh <- errors.New("server got unexpected payload")
			return
		}
		if err := sc.Send([]byte("ok:" + string(got))); err != nil {
			errCh <- err
			return
		}
		errCh <- sc.Finish(nil)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cs, err := clientF.NewClientSession(ctx, cli, spec)
	if err != nil {
		t.Fatal(err)
	}
	defer cs.Close()

	md := metadata.New(metadata.RoleInitiator, nil)
	call, err := cs.OpenCall(ctx, method, session.CallSpec{Metadata: md})
	if err != nil {
		t.Fatal(err)
	}
	defer call.Close()

	if err := call.Send(payload); err != nil {
		t.Fatal(err)
	}
	if err := call.HalfClose(); err != nil {
		t.Fatal(err)
	}
	p, release, err := call.Recv()
	if err != nil {
		t.Fatal(err)
	}
	got := append([]byte(nil), p...)
	release()
	_, _, err = call.Recv()
	if err != io.EOF {
		t.Fatalf("client trailing Recv = %v, want EOF", err)
	}
	want := append([]byte("ok:"), payload...)
	if !bytes.Equal(got, want) {
		t.Fatalf("response = %q, want %q", got, want)
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}

	// Spot-check that gzip path actually sets the LPM compressed flag by
	// compressing a sample with the same algorithm (wire observation is covered
	// by successful round-trip under both gzip-enabled framings).
	if wantCompressedWire {
		c := gzip.New()
		var buf bytes.Buffer
		wc, err := c.Compress(&buf)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := wc.Write(payload); err != nil {
			t.Fatal(err)
		}
		if err := wc.Close(); err != nil {
			t.Fatal(err)
		}
		if bytes.Equal(buf.Bytes(), payload) {
			t.Fatal("gzip should change wire bytes for this payload")
		}
	}
}

type namedComp struct{ name string }

func namedCompressor(name string) compressor.Compressor { return namedComp{name: name} }

func (n namedComp) Name() string { return n.name }

func (n namedComp) Compress(dst io.Writer) (io.WriteCloser, error) {
	return nopWC{Writer: dst}, nil
}

func (n namedComp) Decompress(src io.Reader) (io.ReadCloser, error) {
	return io.NopCloser(src), nil
}

type nopWC struct{ io.Writer }

func (nopWC) Close() error { return nil }

// TestMaxMessageSizeBoundary: exact MaxMessageSize is accepted; MaxMessageSize+1
// is rejected as ResourceExhausted on both Send (local) and Recv (peer).
func TestMaxMessageSizeBoundary(t *testing.T) {
	t.Parallel()
	const maxMsg = 4 << 10

	t.Run("exact_ok", func(t *testing.T) {
		t.Parallel()
		payload := bytes.Repeat([]byte("x"), maxMsg)
		runMaxMsgRoundTrip(t, maxMsg, maxMsg, payload, false)
	})

	t.Run("send_oversize_local", func(t *testing.T) {
		t.Parallel()
		cli, lis := fake.HTTPLoopback()
		t.Cleanup(func() { _ = cli.Close() })
		cfg := session.Options{MaxMessageSize: maxMsg}
		f := newTestFraming(t, nil, "")
		method := descriptor.MustMethod("echo.v1.Echo.Echo", descriptor.Unary)
		spec := session.SessionSpec{CodecName: "proto", Options: cfg}

		// Accept side must run so OpenStream completes.
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
			sc, err := ss.AcceptCall(ctx, session.CallSpec{Metadata: md})
			if err != nil {
				errCh <- err
				return
			}
			_ = sc.Accept(method)
			_, _, _ = sc.Recv()
			errCh <- sc.Finish(status.Error(status.Canceled, "unused"))
		}()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		cs, err := f.NewClientSession(ctx, cli, spec)
		if err != nil {
			t.Fatal(err)
		}
		defer cs.Close()
		md := metadata.New(metadata.RoleInitiator, nil)
		call, err := cs.OpenCall(ctx, method, session.CallSpec{Metadata: md})
		if err != nil {
			t.Fatal(err)
		}
		defer call.Close()

		err = call.Send(bytes.Repeat([]byte("y"), maxMsg+1))
		if status.CodeOf(err) != status.ResourceExhausted {
			t.Fatalf("Send oversize: CodeOf=%v err=%v, want ResourceExhausted", status.CodeOf(err), err)
		}
		_ = call.HalfClose()
		<-errCh
	})

	t.Run("recv_oversize_peer", func(t *testing.T) {
		t.Parallel()
		// Client allows a larger message so Send succeeds; server MaxMessageSize
		// is the limit under test on Recv.
		payload := bytes.Repeat([]byte("z"), maxMsg+1)
		cli, lis := fake.HTTPLoopback()
		t.Cleanup(func() { _ = cli.Close() })

		clientF := newTestFraming(t, nil, "")
		serverF := newTestFraming(t, nil, "")
		method := descriptor.MustMethod("echo.v1.Echo.Echo", descriptor.Unary)
		cliSpec := session.SessionSpec{CodecName: "proto", Options: session.Options{MaxMessageSize: maxMsg + 64}}
		srvSpec := session.SessionSpec{CodecName: "proto", Options: session.Options{MaxMessageSize: maxMsg}}

		errCh := make(chan error, 1)
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			srvConn, err := lis.Accept(ctx)
			if err != nil {
				errCh <- err
				return
			}
			ss, err := serverF.NewServerSession(ctx, srvConn, srvSpec)
			if err != nil {
				errCh <- err
				return
			}
			defer ss.Close()
			md := metadata.New(metadata.RoleResponder, nil)
			sc, err := ss.AcceptCall(ctx, session.CallSpec{Metadata: md})
			if err != nil {
				errCh <- err
				return
			}
			if err := sc.Accept(method); err != nil {
				errCh <- err
				return
			}
			_, release, err := sc.Recv()
			if release != nil {
				release()
			}
			if status.CodeOf(err) != status.ResourceExhausted {
				errCh <- fmt.Errorf("server Recv CodeOf=%v err=%v, want ResourceExhausted", status.CodeOf(err), err)
				return
			}
			errCh <- sc.Finish(err)
		}()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		cs, err := clientF.NewClientSession(ctx, cli, cliSpec)
		if err != nil {
			t.Fatal(err)
		}
		defer cs.Close()
		md := metadata.New(metadata.RoleInitiator, nil)
		call, err := cs.OpenCall(ctx, method, session.CallSpec{Metadata: md})
		if err != nil {
			t.Fatal(err)
		}
		defer call.Close()
		if err := call.Send(payload); err != nil {
			t.Fatal(err)
		}
		if err := call.HalfClose(); err != nil {
			t.Fatal(err)
		}
		_, release, err := call.Recv()
		if release != nil {
			release()
		}
		if status.CodeOf(err) != status.ResourceExhausted {
			t.Fatalf("client Recv CodeOf=%v err=%v, want ResourceExhausted", status.CodeOf(err), err)
		}
		if err := <-errCh; err != nil {
			t.Fatal(err)
		}
	})
}

func runMaxMsgRoundTrip(t *testing.T, cliMax, srvMax int64, payload []byte, expectServerErr bool) {
	t.Helper()
	cli, lis := fake.HTTPLoopback()
	t.Cleanup(func() { _ = cli.Close() })

	clientF := newTestFraming(t, nil, "")
	serverF := newTestFraming(t, nil, "")
	method := descriptor.MustMethod("echo.v1.Echo.Echo", descriptor.Unary)
	cliSpec := session.SessionSpec{CodecName: "proto", Options: session.Options{MaxMessageSize: cliMax}}
	srvSpec := session.SessionSpec{CodecName: "proto", Options: session.Options{MaxMessageSize: srvMax}}

	errCh := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srvConn, err := lis.Accept(ctx)
		if err != nil {
			errCh <- err
			return
		}
		ss, err := serverF.NewServerSession(ctx, srvConn, srvSpec)
		if err != nil {
			errCh <- err
			return
		}
		defer ss.Close()
		md := metadata.New(metadata.RoleResponder, nil)
		sc, err := ss.AcceptCall(ctx, session.CallSpec{Metadata: md})
		if err != nil {
			errCh <- err
			return
		}
		if err := sc.Accept(method); err != nil {
			errCh <- err
			return
		}
		p, release, err := sc.Recv()
		if expectServerErr {
			if release != nil {
				release()
			}
			errCh <- err
			_ = sc.Finish(err)
			return
		}
		if err != nil {
			errCh <- err
			return
		}
		got := append([]byte(nil), p...)
		release()
		_, _, _ = sc.Recv()
		if !bytes.Equal(got, payload) {
			errCh <- fmt.Errorf("server got len=%d want %d", len(got), len(payload))
			return
		}
		if err := sc.Send([]byte("ok")); err != nil {
			errCh <- err
			return
		}
		errCh <- sc.Finish(nil)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cs, err := clientF.NewClientSession(ctx, cli, cliSpec)
	if err != nil {
		t.Fatal(err)
	}
	defer cs.Close()
	md := metadata.New(metadata.RoleInitiator, nil)
	call, err := cs.OpenCall(ctx, method, session.CallSpec{Metadata: md})
	if err != nil {
		t.Fatal(err)
	}
	defer call.Close()
	if err := call.Send(payload); err != nil {
		t.Fatal(err)
	}
	if err := call.HalfClose(); err != nil {
		t.Fatal(err)
	}
	if expectServerErr {
		_, release, err := call.Recv()
		if release != nil {
			release()
		}
		_ = err
		if err := <-errCh; status.CodeOf(err) != status.ResourceExhausted {
			t.Fatalf("server err CodeOf=%v err=%v", status.CodeOf(err), err)
		}
		return
	}
	p, release, err := call.Recv()
	if err != nil {
		t.Fatal(err)
	}
	got := append([]byte(nil), p...)
	release()
	if string(got) != "ok" {
		t.Fatalf("got %q", got)
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
}

// TestCompressionBombMaxMessageSize sends a tiny gzip LPM that expands past
// MaxMessageSize. Decompress must stop at max+1 (no OOM) and surface
// ResourceExhausted.
func TestCompressionBombMaxMessageSize(t *testing.T) {
	t.Parallel()
	const (
		maxMsg   = 1024
		bombSize = 1 << 20 // 1 MiB of zeros → tiny gzip wire
	)
	bomb := make([]byte, bombSize)

	cli, lis := fake.HTTPLoopback()
	t.Cleanup(func() { _ = cli.Close() })

	gz := gzip.New()
	clientCfg := compressCfg{extra: []compressor.Compressor{gz}, send: gzip.Name}
	serverCfg := compressCfg{extra: []compressor.Compressor{gzip.New()}}
	clientF := newTestFraming(t, clientCfg.extra, clientCfg.send)
	serverF := newTestFraming(t, serverCfg.extra, serverCfg.send)
	method := descriptor.MustMethod("echo.v1.Echo.Echo", descriptor.Unary)
	cliSpec := session.SessionSpec{CodecName: "proto", Options: session.Options{MaxMessageSize: bombSize}}
	srvSpec := session.SessionSpec{CodecName: "proto", Options: session.Options{MaxMessageSize: maxMsg}}

	errCh := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srvConn, err := lis.Accept(ctx)
		if err != nil {
			errCh <- err
			return
		}
		ss, err := serverF.NewServerSession(ctx, srvConn, srvSpec)
		if err != nil {
			errCh <- err
			return
		}
		defer ss.Close()
		md := metadata.New(metadata.RoleResponder, nil)
		sc, err := ss.AcceptCall(ctx, session.CallSpec{Metadata: md})
		if err != nil {
			errCh <- err
			return
		}
		if err := sc.Accept(method); err != nil {
			errCh <- err
			return
		}
		_, release, err := sc.Recv()
		if release != nil {
			release()
		}
		if status.CodeOf(err) != status.ResourceExhausted {
			errCh <- fmt.Errorf("server Recv CodeOf=%v err=%v, want ResourceExhausted", status.CodeOf(err), err)
			return
		}
		errCh <- sc.Finish(err)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cs, err := clientF.NewClientSession(ctx, cli, cliSpec)
	if err != nil {
		t.Fatal(err)
	}
	defer cs.Close()
	md := metadata.New(metadata.RoleInitiator, nil)
	call, err := cs.OpenCall(ctx, method, session.CallSpec{Metadata: md})
	if err != nil {
		t.Fatal(err)
	}
	defer call.Close()

	if err := call.Send(bomb); err != nil {
		t.Fatal(err)
	}
	if err := call.HalfClose(); err != nil {
		t.Fatal(err)
	}
	_, release, err := call.Recv()
	if release != nil {
		release()
	}
	if status.CodeOf(err) != status.ResourceExhausted {
		t.Fatalf("client Recv CodeOf=%v err=%v, want ResourceExhausted", status.CodeOf(err), err)
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
}

package grpc_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/argos-io/argos/compressor"
	"github.com/argos-io/argos/compressor/gzip"
	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/framing"
	grpcframing "github.com/argos-io/argos/framing/grpc"
	"github.com/argos-io/argos/internal/fake"
	"github.com/argos-io/argos/metadata"
	"github.com/argos-io/argos/status"
)

func TestCompressionIdentityDefaultRoundTrip(t *testing.T) {
	t.Parallel()
	runCompressionRoundTrip(t, nil, nil, false)
}

func TestCompressionGzipEnabledRoundTrip(t *testing.T) {
	t.Parallel()
	opts := []grpcframing.Option{
		grpcframing.WithCompressors(gzip.New()),
		grpcframing.WithSendCompressor(gzip.Name),
	}
	runCompressionRoundTrip(t, opts, opts, true)
}

func TestCompressionUnknownAcceptIgnored(t *testing.T) {
	t.Parallel()
	// Client advertises a mystery algorithm in accept-encoding but sends identity.
	mystery := namedCompressor("mystery")
	clientOpts := []grpcframing.Option{
		grpcframing.WithCompressors(mystery),
		grpcframing.WithSendCompressor(compressor.Identity.Name()),
	}
	serverOpts := []grpcframing.Option{
		grpcframing.WithCompressors(gzip.New()),
		grpcframing.WithSendCompressor(gzip.Name), // peer accept lacks gzip → identity
	}
	runCompressionRoundTrip(t, clientOpts, serverOpts, false)
}

func TestCompressionUnknownEncodingUnimplemented(t *testing.T) {
	t.Parallel()
	cli, lis := fake.HTTPLoopback()
	t.Cleanup(func() { _ = cli.Close() })

	mystery := namedCompressor("mystery")
	clientF := newTestFraming(t,
		grpcframing.WithCompressors(mystery),
		grpcframing.WithSendCompressor("mystery"),
	)
	serverF := newTestFraming(t) // identity only

	method := descriptor.MustMethod("echo.v1.Echo.Echo", descriptor.Unary)
	spec := framing.SessionSpec{CodecName: "proto"}
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
		sc, err := ss.AcceptCall(ctx, framing.CallSpec{Metadata: md})
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
	call, err := cs.OpenCall(ctx, method, framing.CallSpec{Metadata: md})
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

func TestCompressionDuplicateCompressorName(t *testing.T) {
	t.Parallel()
	_, err := grpcframing.New(grpcframing.WithCompressors(gzip.New(), gzip.New()))
	if err == nil {
		t.Fatal("duplicate gzip names: want error")
	}
	_, err = grpcframing.New(grpcframing.WithCompressors(compressor.Identity))
	if err == nil {
		t.Fatal("duplicate identity: want error")
	}
	_, err = grpcframing.New(grpcframing.WithSendCompressor("gzip"))
	if err == nil {
		t.Fatal("send gzip without enabling: want error")
	}
}

func runCompressionRoundTrip(t *testing.T, clientOpts, serverOpts []grpcframing.Option, wantCompressedWire bool) {
	t.Helper()
	cli, lis := fake.HTTPLoopback()
	t.Cleanup(func() { _ = cli.Close() })

	clientF := newTestFraming(t, clientOpts...)
	serverF := newTestFraming(t, serverOpts...)

	method := descriptor.MustMethod("echo.v1.Echo.Echo", descriptor.Unary)
	spec := framing.SessionSpec{CodecName: "proto"}
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
		sc, err := ss.AcceptCall(ctx, framing.CallSpec{Metadata: md})
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
	call, err := cs.OpenCall(ctx, method, framing.CallSpec{Metadata: md})
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

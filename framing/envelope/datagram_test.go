package envelope_test

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/argos-io/argos"
	"github.com/argos-io/argos/codec"
	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/framing"
	"github.com/argos-io/argos/framing/envelope"
	"github.com/argos-io/argos/internal/fake"
	"github.com/argos-io/argos/metadata"
	"github.com/argos-io/argos/status"
	"github.com/argos-io/argos/transport"
	"github.com/argos-io/argos/transport/udp"
)

func TestReuseOneCallPerConnOption(t *testing.T) {
	t.Parallel()
	f := envelope.New(envelope.WithOneCallPerConn())
	if f.Reuse() != framing.OneCallPerConn {
		t.Fatalf("Reuse = %v, want OneCallPerConn", f.Reuse())
	}
}

// TestDatagramRejectsNonUnary is Task 6.3 / §9-3: UDP/DatagramCarrier streaming
// shapes are rejected before Filter/handler (OpenCall + Accept).
func TestDatagramRejectsNonUnary(t *testing.T) {
	t.Parallel()
	cliConn, srvConn := fake.DatagramPipe()
	defer cliConn.Close()
	defer srvConn.Close()

	fr := envelope.New(envelope.WithOneCallPerConn())
	spec := framing.SessionSpec{Config: framing.Config{
		MaxFrameSize:    udp.MaxDatagramSize,
		MaxMessageSize:  32 << 10,
		MaxMetadataSize: 8 << 10,
	}}
	cliSess, err := fr.NewClientSession(context.Background(), cliConn, spec)
	if err != nil {
		t.Fatal(err)
	}
	defer cliSess.Close()
	srvSess, err := fr.NewServerSession(context.Background(), srvConn, spec)
	if err != nil {
		t.Fatal(err)
	}
	defer srvSess.Close()

	stream := descriptor.MustMethod("echo.v1.EchoService.Watch", descriptor.ServerStreaming)
	cliMD := metadata.New(metadata.RoleInitiator, nil)

	_, err = cliSess.OpenCall(context.Background(), stream, framing.CallSpec{Metadata: cliMD})
	if status.CodeOf(err) != status.Unimplemented {
		t.Fatalf("OpenCall(non-unary) CodeOf=%v err=%v, want Unimplemented", status.CodeOf(err), err)
	}
	if !strings.Contains(err.Error(), "DatagramCarrier") {
		t.Fatalf("OpenCall error %q should mention DatagramCarrier", err)
	}

	// Server Accept path: unary OPEN reaches AcceptCall, then Accept(stream) fails.
	unary := descriptor.MustMethod("echo.v1.EchoService.Echo", descriptor.Unary)
	srvMD := metadata.New(metadata.RoleResponder, nil)
	acceptErr := make(chan error, 1)
	go func() {
		sc, err := srvSess.AcceptCall(context.Background(), framing.CallSpec{Metadata: srvMD})
		if err != nil {
			acceptErr <- err
			return
		}
		defer sc.Close()
		acceptErr <- sc.Accept(stream)
	}()

	okMD := metadata.New(metadata.RoleInitiator, nil)
	call, err := cliSess.OpenCall(context.Background(), unary, framing.CallSpec{Metadata: okMD})
	if err != nil {
		t.Fatalf("OpenCall(unary) after reject: %v", err)
	}
	if err := call.HalfClose(); err != nil {
		t.Fatalf("HalfClose: %v", err)
	}
	_ = call.Close()

	select {
	case err := <-acceptErr:
		if status.CodeOf(err) != status.Unimplemented {
			t.Fatalf("Accept(non-unary) CodeOf=%v err=%v, want Unimplemented", status.CodeOf(err), err)
		}
		if !strings.Contains(err.Error(), "DatagramCarrier") {
			t.Fatalf("Accept error %q should mention DatagramCarrier", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for Accept(non-unary)")
	}
}

func TestDatagramUnaryRoundTrip(t *testing.T) {
	t.Parallel()
	cliConn, srvConn := fake.DatagramPipe()
	defer cliConn.Close()
	defer srvConn.Close()

	fr := envelope.New(envelope.WithOneCallPerConn())
	spec := framing.SessionSpec{Config: framing.Config{
		MaxFrameSize:    udp.MaxDatagramSize,
		MaxMessageSize:  32 << 10,
		MaxMetadataSize: 8 << 10,
	}}
	cliSess, err := fr.NewClientSession(context.Background(), cliConn, spec)
	if err != nil {
		t.Fatal(err)
	}
	defer cliSess.Close()
	srvSess, err := fr.NewServerSession(context.Background(), srvConn, spec)
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
		if err := sc.Accept(method); err != nil {
			t.Errorf("Accept: %v", err)
			return
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

	if cliSess.Reusable() {
		t.Fatal("datagram client session should not be reusable after one call")
	}
	_, err = cliSess.OpenCall(context.Background(), method, framing.CallSpec{Metadata: cliMD})
	if !errors.Is(err, framing.ErrSessionSpent) {
		t.Fatalf("second OpenCall = %v, want ErrSessionSpent", err)
	}
	_, err = srvSess.AcceptCall(context.Background(), framing.CallSpec{Metadata: srvMD})
	if !errors.Is(err, io.EOF) {
		t.Fatalf("second AcceptCall = %v, want EOF", err)
	}
}

func TestDatagramZeroMessageOpenEnd(t *testing.T) {
	t.Parallel()
	cliConn, srvConn := fake.DatagramPipe()
	defer cliConn.Close()
	defer srvConn.Close()

	fr := envelope.New(envelope.WithOneCallPerConn())
	spec := framing.SessionSpec{Config: framing.Config{
		MaxFrameSize:   udp.MaxDatagramSize,
		MaxMessageSize: 32 << 10,
	}}
	cliSess, err := fr.NewClientSession(context.Background(), cliConn, spec)
	if err != nil {
		t.Fatal(err)
	}
	defer cliSess.Close()
	srvSess, err := fr.NewServerSession(context.Background(), srvConn, spec)
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
		_ = sc.Accept(method)
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

func TestDatagramMismatchedCallIDDiscarded(t *testing.T) {
	t.Parallel()
	cliConn, srvConn := fake.DatagramPipe()
	defer cliConn.Close()
	defer srvConn.Close()

	fr := envelope.New(envelope.WithOneCallPerConn())
	spec := framing.SessionSpec{Config: framing.Config{
		MaxFrameSize:   udp.MaxDatagramSize,
		MaxMessageSize: 32 << 10,
	}}
	cliSess, err := fr.NewClientSession(context.Background(), cliConn, spec)
	if err != nil {
		t.Fatal(err)
	}
	defer cliSess.Close()
	srvSess, err := fr.NewServerSession(context.Background(), srvConn, spec)
	if err != nil {
		t.Fatal(err)
	}
	defer srvSess.Close()

	method := descriptor.MustMethod("echo.Mismatch", descriptor.Unary)
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
		_ = sc.Accept(method)
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
	defer call.Close()

	// Inject a response with wrong call ID before the real exchange completes.
	bad, err := envelope.MarshalDatagram([]envelope.Frame{{
		Type:   envelope.TypeStatus,
		CallID: 999,
		Code:   0,
	}})
	if err != nil {
		t.Fatal(err)
	}
	cliConn.InjectDatagram(bad)

	if err := call.Send([]byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := call.HalfClose(); err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	var recvErr error
	var payload []byte
	go func() {
		defer close(done)
		var release func()
		payload, release, recvErr = call.Recv()
		if release != nil {
			release()
		}
	}()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Recv timed out; mismatched call ID may have aborted the call")
	}
	if recvErr != nil {
		t.Fatalf("Recv after mismatch discard: %v", recvErr)
	}
	if string(payload) != "ok" {
		t.Fatalf("payload = %q, want ok", payload)
	}
	wg.Wait()
}

func TestCheckDatagramLimitsStartup(t *testing.T) {
	t.Parallel()
	err := envelope.CheckDatagramLimits(4<<20, 4<<20, udp.MaxDatagramSize)
	if err == nil {
		t.Fatal("want error for oversized MaxFrameSize/MaxMessageSize")
	}
	msg := err.Error()
	if !strings.Contains(msg, "MaxFrameSize") {
		t.Fatalf("error %q should mention MaxFrameSize", msg)
	}
	if !strings.Contains(msg, "MaxMessageSize") {
		t.Fatalf("error %q should mention MaxMessageSize", msg)
	}

	if err := envelope.CheckDatagramLimits(udp.MaxDatagramSize, udp.MaxDatagramSize, udp.MaxDatagramSize); err != nil {
		t.Fatalf("limits at MaxDatagramSize should pass: %v", err)
	}
	if err := envelope.CheckDatagramLimits(0, 0, udp.MaxDatagramSize); err != nil {
		t.Fatalf("zero limits should pass: %v", err)
	}
}

func TestBindingNewRejectsOversizedUDP(t *testing.T) {
	t.Parallel()
	cfg := &argos.Config{
		MaxMessageSize: 4 << 20,
		MaxFrameSize:   4 << 20,
	}
	p := argos.Protocol{
		Transport: func() (transport.Transport, error) { return udp.New(), nil },
		Framing: func() (framing.Framing, error) {
			if err := envelope.CheckDatagramLimits(cfg.MaxFrameSize, cfg.MaxMessageSize, udp.MaxDatagramSize); err != nil {
				return nil, err
			}
			return envelope.New(envelope.WithOneCallPerConn()), nil
		},
		Codec: func() (codec.Codec, error) { return rawCodec{}, nil },
	}
	_, _, _, err := p.Assemble()
	if err == nil {
		t.Fatal("Assemble: want size-gate error")
	}
	if !strings.Contains(err.Error(), "MaxMessageSize") {
		t.Fatalf("error %q should list MaxMessageSize", err)
	}
}

func TestUDPTransportEnvelopeRoundTrip(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	srvTr := udp.New().(*udp.Transport)
	cliTr := udp.New()

	fr := envelope.New(envelope.WithOneCallPerConn())
	spec := framing.SessionSpec{Config: framing.Config{
		MaxFrameSize:   udp.MaxDatagramSize,
		MaxMessageSize: 32 << 10,
	}}

	method := descriptor.MustMethod("udp.Echo.Echo", descriptor.Unary)
	done := make(chan struct{})
	serveErr := make(chan error, 1)
	go func() {
		serveErr <- srvTr.Serve(ctx, func(_ context.Context, c transport.Conn) {
			srvSess, err := fr.NewServerSession(context.Background(), c, spec)
			if err != nil {
				t.Errorf("NewServerSession: %v", err)
				return
			}
			defer srvSess.Close()
			md := metadata.New(metadata.RoleResponder, nil)
			sc, err := srvSess.AcceptCall(context.Background(), framing.CallSpec{Metadata: md})
			if err != nil {
				t.Errorf("AcceptCall: %v", err)
				return
			}
			defer sc.Close()
			_ = sc.Accept(method)
			payload, release, err := sc.Recv()
			if err != nil {
				t.Errorf("server Recv: %v", err)
				return
			}
			msg := append([]byte(nil), payload...)
			release()
			_, _, _ = sc.Recv()
			_ = sc.Send(append([]byte("echo:"), msg...))
			_ = sc.Finish(nil)
			close(done)
		}, transport.WithListenAddress("127.0.0.1:0"))
	}()

	deadline := time.Now().Add(3 * time.Second)
	var addr string
	for time.Now().Before(deadline) {
		if a := srvTr.Addr(); a != nil {
			addr = a.String()
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if addr == "" {
		t.Fatal("udp listener Addr not ready")
	}

	conn, err := cliTr.Dial(ctx, transport.DialSpec{Endpoint: addr})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	cliSess, err := fr.NewClientSession(context.Background(), conn, spec)
	if err != nil {
		t.Fatal(err)
	}
	defer cliSess.Close()

	call, err := cliSess.OpenCall(context.Background(), method, framing.CallSpec{
		Metadata: metadata.New(metadata.RoleInitiator, nil),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer call.Close()
	if err := call.Send([]byte("hi")); err != nil {
		t.Fatal(err)
	}
	if err := call.HalfClose(); err != nil {
		t.Fatal(err)
	}
	payload, release, err := call.Recv()
	if err != nil {
		t.Fatalf("client Recv: %v", err)
	}
	if string(payload) != "echo:hi" {
		t.Fatalf("payload = %q, want echo:hi", payload)
	}
	release()
	if _, _, err := call.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("status Recv = %v, want EOF", err)
	}

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("server handler did not finish")
	}
	cancel()
	select {
	case err := <-serveErr:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("Serve: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Serve did not exit")
	}
}

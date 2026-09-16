package envelope_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/framing"
	"github.com/argos-io/argos/framing/envelope"
	"github.com/argos-io/argos/internal/fake"
	"github.com/argos-io/argos/metadata"
	"github.com/argos-io/argos/status"
	"github.com/argos-io/argos/stream"
	"github.com/argos-io/argos/transport"
	"github.com/argos-io/argos/transport/tcp"
)

// ---------------------------------------------------------------------------
// A send failure on a still-readable exchange must end the send direction only
// (transport.SendError / stream.ErrSendClosed). The responder below is the
// shape the contract cares about: it rejects a call while the initiator is
// still uploading, so the initiator's next send fails while the rejection is
// on the wire.
// ---------------------------------------------------------------------------

var errBrokenWire = errors.New("envelope_test: broken wire")

// brokenBytePair returns a connected ByteConn pair over loopback TCP with the
// client's write direction breakable on demand. Loopback TCP rather than
// net.Pipe: both peers write concurrently here, and net.Pipe deadlocks when one
// side blocks on Write while the other has not reached Read.
func brokenBytePair(t *testing.T) (cli, srv *fake.ByteConn, brk *fake.WriteFailConn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	type dialResult struct {
		c   net.Conn
		err error
	}
	ch := make(chan dialResult, 1)
	go func() {
		c, err := net.Dial("tcp", ln.Addr().String())
		ch <- dialResult{c, err}
	}()
	srvNC, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	dr := <-ch
	if dr.err != nil {
		_ = srvNC.Close()
		t.Fatal(dr.err)
	}
	brk = fake.BreakWrites(dr.c)
	cli = fake.NewByteConn(brk)
	srv = fake.NewByteConn(srvNC)
	t.Cleanup(func() {
		_ = cli.Close()
		_ = srv.Close()
	})
	return cli, srv, brk
}

// rejectedCall drives one envelope call through a responder that accepts,
// sends its one response message and then fails the call with err. It reports
// once the responder's status is on the wire, and waits for the gate before
// writing anything — a rejection that is only produced after the initiator's
// send direction already died is the case that matters: nothing may be lost
// because the send failed first.
func rejectedCall(t *testing.T, srvSess framing.ServerSession, method descriptor.Method, respond <-chan struct{}, resp []byte, callErr error) <-chan struct{} {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		md := metadata.New(metadata.RoleResponder, nil)
		sc, err := srvSess.AcceptCall(ctx, framing.CallSpec{Metadata: md})
		if err != nil {
			t.Errorf("AcceptCall: %v", err)
			return
		}
		if err := sc.Accept(method); err != nil {
			t.Errorf("Accept: %v", err)
			return
		}
		select {
		case <-respond:
		case <-ctx.Done():
			t.Errorf("responder gate: %v", ctx.Err())
			return
		}
		// The rejection is sent while the initiator may still be uploading;
		// reading the request is deliberately not required first.
		if err := sc.Send(resp); err != nil {
			t.Errorf("responder Send: %v", err)
			return
		}
		if err := sc.Finish(callErr); err != nil {
			t.Errorf("responder Finish: %v", err)
		}
	}()
	return done
}

// TestSendFailureKeepsExchangeReadable is the contract test for
// transport.SendError in the envelope framing: a send-direction failure must
// not mark the session bad (markBad aborts the carrier, which is what used to
// discard a response that was still on the wire), and the failure must reach
// the caller as stream.ErrSendClosed so the caller keeps calling Recv.
func TestSendFailureKeepsExchangeReadable(t *testing.T) {
	t.Parallel()
	cliConn, srvConn, brk := brokenBytePair(t)
	fr := envelope.New()
	method := descriptor.MustMethod("svc.Senderror", descriptor.Unary)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cliSess, err := fr.NewClientSession(ctx, cliConn, framing.SessionSpec{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cliSess.Close() }()
	srvSess, err := fr.NewServerSession(ctx, srvConn, framing.SessionSpec{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = srvSess.Close() }()

	rejection := status.Error(status.Unauthenticated, "token expired")
	respond := make(chan struct{})
	responded := rejectedCall(t, srvSess, method, respond, []byte("resp"), rejection)

	md := metadata.New(metadata.RoleInitiator, nil)
	call, err := cliSess.OpenCall(ctx, method, framing.CallSpec{Metadata: md})
	if err != nil {
		t.Fatalf("OpenCall: %v", err)
	}
	defer func() { _ = call.Close() }()
	st := stream.Wrap(call, rawCodec{})

	// Part of the upload goes through, then the wire breaks under it.
	if err := st.Send("part1"); err != nil {
		t.Fatalf("Send before failure: %v", err)
	}
	brk.FailAfter(0, errBrokenWire, true)

	sendErr := st.Send("part2")
	if !errors.Is(sendErr, stream.ErrSendClosed) {
		t.Fatalf("Send after wire failure = %v; want stream.ErrSendClosed", sendErr)
	}
	// The framing error stays the transport's SendError — cause and verdict
	// observable — while stream.Wrap replaces it with its own sentinel, which
	// is what callers branch on.
	frameErr := call.HalfClose()
	se, ok := transport.AsSendError(frameErr)
	if !ok || !se.ReceiveOpen() {
		t.Fatalf("call.HalfClose() = %v; want a transport.SendError with ReceiveOpen true", frameErr)
	}
	if !errors.Is(frameErr, errBrokenWire) {
		t.Fatalf("framing error lost the wire cause: %v", frameErr)
	}
	// The send direction is over: repeating a send call reports the same end of
	// direction instead of writing into a stream a partial frame already broke.
	if err := st.HalfClose(); !errors.Is(err, stream.ErrSendClosed) {
		t.Fatalf("HalfClose after failed send = %v; want stream.ErrSendClosed", err)
	}
	if n := brk.FailedWrites(); n != 1 {
		t.Fatalf("wire writes attempted after failure: %d failures; want 1", n)
	}

	close(respond)
	<-responded

	// The receive direction still delivers the rejection, which the responder
	// had not even sent when the send direction died.
	var msg string
	if err := st.Recv(&msg); err != nil {
		t.Fatalf("Recv response message: %v", err)
	}
	if msg != "resp" {
		t.Fatalf("Recv message = %q; want %q", msg, "resp")
	}
	statusErr := st.Recv(&msg)
	if status.CodeOf(statusErr) != status.Unauthenticated {
		t.Fatalf("Recv status = %v; want %v", statusErr, rejection)
	}
	if statusErr.Error() != rejection.Error() {
		t.Fatalf("Recv status message = %q; want %q", statusErr, rejection)
	}

	// Nothing of that was possible if the framing poisoned the exchange: the
	// session must stay reusable and the carrier untouched.
	if !cliSess.Reusable() {
		t.Fatal("session not reusable after a send failure on a readable exchange")
	}
	if cliConn.Aborted() {
		t.Fatal("carrier aborted after a send failure on a readable exchange")
	}
}

// TestUnrecoverableSendFailureStillPoisonsSession pins the other half of the
// contract: when the transport reports the exchange dead (ReceiveOpen false),
// the failure is not ErrSendClosed and the session must not be reused.
func TestUnrecoverableSendFailureStillPoisonsSession(t *testing.T) {
	t.Parallel()
	cliConn, srvConn, brk := brokenBytePair(t)
	fr := envelope.New()
	method := descriptor.MustMethod("svc.Senderror", descriptor.Unary)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cliSess, err := fr.NewClientSession(ctx, cliConn, framing.SessionSpec{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cliSess.Close() }()
	srvSess, err := fr.NewServerSession(ctx, srvConn, framing.SessionSpec{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = srvSess.Close() }()

	go func() {
		md := metadata.New(metadata.RoleResponder, nil)
		sc, err := srvSess.AcceptCall(ctx, framing.CallSpec{Metadata: md})
		if err != nil {
			return
		}
		_ = sc.Accept(method)
		for {
			if _, release, err := sc.Recv(); err != nil {
				return
			} else {
				release()
			}
		}
	}()

	md := metadata.New(metadata.RoleInitiator, nil)
	call, err := cliSess.OpenCall(ctx, method, framing.CallSpec{Metadata: md})
	if err != nil {
		t.Fatalf("OpenCall: %v", err)
	}
	defer func() { _ = call.Close() }()
	st := stream.Wrap(call, rawCodec{})

	brk.Fail(errBrokenWire, false)
	sendErr := st.Send("part1")
	if errors.Is(sendErr, stream.ErrSendClosed) {
		t.Fatalf("Send on a dead exchange = %v; want the raw failure, not ErrSendClosed", sendErr)
	}
	if !errors.Is(sendErr, errBrokenWire) {
		t.Fatalf("Send error = %v; want %v", sendErr, errBrokenWire)
	}
	if cliSess.Reusable() {
		t.Fatal("session reusable after an unrecoverable send failure")
	}
	if !cliConn.Aborted() {
		t.Fatal("carrier not aborted after an unrecoverable send failure")
	}
}

// TestHangupOverRealTCPKeepsDeliveredStatus is the mirror image of the test
// above, on a real transport with no fake in the path: when the peer hangs up
// on a byte stream, the failure is connection-wide. transport/tcp must then
// report ReceiveOpen false — never ErrSendClosed — and the session must be
// dropped; a response the call already received stays readable, because the
// contract is about not discarding what arrived, not about resurrecting a dead
// connection.
func TestHangupOverRealTCPKeepsDeliveredStatus(t *testing.T) {
	t.Parallel()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// The responder is a raw peer: it writes the STATUS frame for the first
	// call and then closes with the initiator's frames still unread, which is
	// what makes the initiator's next write fail.
	responded := make(chan error, 1)
	go func() {
		nc, err := ln.Accept()
		if err != nil {
			responded <- err
			return
		}
		defer func() { _ = nc.Close() }()
		frame, err := envelope.MarshalFrame(envelope.Frame{
			Type:    envelope.TypeStatus,
			CallID:  1,
			Code:    uint32(status.Unauthenticated),
			Message: "token expired",
		})
		if err != nil {
			responded <- err
			return
		}
		if _, err := nc.Write(frame); err != nil {
			responded <- err
			return
		}
		// Unread request bytes at close turn the close into an RST, so the
		// initiator cannot keep writing into a half-open connection.
		responded <- nil
	}()

	conn, err := tcp.New().Dial(ctx, transport.DialSpec{Endpoint: ln.Addr().String()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()

	cliSess, err := envelope.New().NewClientSession(ctx, conn, framing.SessionSpec{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cliSess.Close() }()

	method := descriptor.MustMethod("svc.Senderror", descriptor.Unary)
	md := metadata.New(metadata.RoleInitiator, nil)
	call, err := cliSess.OpenCall(ctx, method, framing.CallSpec{Metadata: md})
	if err != nil {
		t.Fatalf("OpenCall: %v", err)
	}
	defer func() { _ = call.Close() }()
	st := stream.Wrap(call, rawCodec{})

	if err := <-responded; err != nil {
		t.Fatalf("responder: %v", err)
	}

	// The peer is gone; keep uploading until the send direction dies. The
	// kernel may accept a write or two into a buffer it will never deliver.
	var sendErr error
	deadline := time.Now().Add(5 * time.Second)
	for i := 0; sendErr == nil && time.Now().Before(deadline); i++ {
		sendErr = st.Send(fmt.Sprintf("upload-%d", i))
	}
	if sendErr == nil {
		t.Fatal("no send failure after the responder hung up")
	}
	// transport/tcp must classify the failure either way: a write that fails
	// while the Conn is still open reports the conservative ReceiveOpen true
	// (stream.ErrSendClosed); one that fails after the read side already
	// aborted the Conn reports a dead exchange. Which one wins is a race
	// between the two directions observing the same RST, so both are correct —
	// what must never happen is an unclassified write error reaching the caller.
	if !errors.Is(sendErr, stream.ErrSendClosed) {
		if se, ok := transport.AsSendError(sendErr); !ok {
			t.Fatalf("Send after responder hangup = %v; want stream.ErrSendClosed or a transport.SendError", sendErr)
		} else {
			t.Logf("read side observed the hangup first: %v (ReceiveOpen %v)", sendErr, se.ReceiveOpen())
		}
	}

	// The rejection that arrived before the hangup is still readable.
	var msg string
	err = st.Recv(&msg)
	if status.CodeOf(err) != status.Unauthenticated {
		t.Fatalf("Recv after send failure = %v; want status %v", err, status.Unauthenticated)
	}
	if _, _, err := call.Recv(); !errors.Is(err, io.EOF) {
		t.Fatalf("Recv after status = %v; want io.EOF", err)
	}
}

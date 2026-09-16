package grpc_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/framing"
	"github.com/argos-io/argos/internal/fake"
	"github.com/argos-io/argos/metadata"
	"github.com/argos-io/argos/status"
	"github.com/argos-io/argos/stream"
	"github.com/argos-io/argos/transport"
)

// bytesCodec passes payloads through unchanged: this test is about the send
// direction, not about encoding.
type bytesCodec struct{}

func (bytesCodec) Marshal(v any) ([]byte, error) {
	switch x := v.(type) {
	case []byte:
		return append([]byte(nil), x...), nil
	case string:
		return []byte(x), nil
	default:
		return nil, fmt.Errorf("bytesCodec: unsupported %T", v)
	}
}

func (bytesCodec) Unmarshal(b []byte, v any) error {
	switch x := v.(type) {
	case *[]byte:
		*x = append([]byte(nil), b...)
		return nil
	case *string:
		*x = string(b)
		return nil
	default:
		return fmt.Errorf("bytesCodec: unsupported %T", v)
	}
}

func (bytesCodec) CodecName() string { return "bytes" }

var errBrokenWire = errors.New("grpc_test: broken wire")

// TestSendFailureKeepsStatusReadable is the contract test for
// transport.SendError in the grpc framing. The responder reads one message and
// then rejects without draining the rest, so the initiator's next send fails
// while the rejection is on the wire. That failure must end the send direction
// only: stream.ErrSendClosed from Send, the status still readable, and the
// session — a handle shared by every concurrent stream on the connection — must
// not be dropped because one stream's send died.
func TestSendFailureKeepsStatusReadable(t *testing.T) {
	t.Parallel()
	cli, lis := fake.HTTPLoopback()
	t.Cleanup(func() { _ = cli.Close() })

	f := newTestFraming(t)
	method := descriptor.MustMethod("echo.v1.Echo.Echo", descriptor.Unary)
	spec := framing.SessionSpec{CodecName: "proto"}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	rejection := status.Error(status.Unauthenticated, "token expired")
	srvDone := make(chan struct{})
	releaseSrv := make(chan struct{})
	go func() {
		defer close(srvDone)
		srvConn, err := lis.Accept(ctx)
		if err != nil {
			t.Errorf("Accept: %v", err)
			return
		}
		ss, err := f.NewServerSession(ctx, srvConn, spec)
		if err != nil {
			t.Errorf("NewServerSession: %v", err)
			return
		}
		// Closing the server session aborts the fake carrier, so it waits until
		// the client has read everything the responder sent.
		defer func() {
			select {
			case <-releaseSrv:
			case <-ctx.Done():
			}
			_ = ss.Close()
		}()

		md := metadata.New(metadata.RoleResponder, nil)
		sc, err := ss.AcceptCall(ctx, framing.CallSpec{Metadata: md})
		if err != nil {
			t.Errorf("AcceptCall: %v", err)
			return
		}
		if err := sc.Accept(method); err != nil {
			t.Errorf("Accept: %v", err)
			return
		}
		// One message in, then reject without draining the rest.
		if _, release, err := sc.Recv(); err != nil {
			t.Errorf("responder Recv: %v", err)
			return
		} else {
			release()
		}
		if err := sc.Finish(rejection); err != nil {
			t.Errorf("responder Finish: %v", err)
		}
	}()

	cs, err := f.NewClientSession(ctx, cli, spec)
	if err != nil {
		t.Fatalf("NewClientSession: %v", err)
	}
	defer func() { _ = cs.Close() }()

	md := metadata.New(metadata.RoleInitiator, nil)
	call, err := cs.OpenCall(ctx, method, framing.CallSpec{Metadata: md})
	if err != nil {
		t.Fatalf("OpenCall: %v", err)
	}
	defer func() { _ = call.Close() }()
	st := stream.Wrap(call, bytesCodec{})

	if err := st.Send("part1"); err != nil {
		t.Fatalf("Send before failure: %v", err)
	}

	// The rejection is on the wire (the responder read its one message): the
	// initiator's body will no longer be accepted.
	cli.FailClientWrites(true, errBrokenWire)

	sendErr := st.Send("part2")
	if !errors.Is(sendErr, stream.ErrSendClosed) {
		t.Fatalf("Send after wire failure = %v; want stream.ErrSendClosed", sendErr)
	}
	frameErr := call.HalfClose()
	se, ok := transport.AsSendError(frameErr)
	if !ok || !se.ReceiveOpen() {
		t.Fatalf("call.HalfClose() = %v; want a transport.SendError with ReceiveOpen true", frameErr)
	}
	if !errors.Is(frameErr, errBrokenWire) {
		t.Fatalf("framing error lost the wire cause: %v", frameErr)
	}

	// gRPC's connection is shared by every stream on it: one stream's send
	// failure says nothing about the session.
	if !cs.Reusable() {
		t.Fatal("client session not reusable after one stream's send failure")
	}

	var resp []byte
	recvErr := st.Recv(&resp)
	if status.CodeOf(recvErr) != status.Unauthenticated {
		t.Fatalf("Recv after send failure = %v; want status %v", recvErr, status.Unauthenticated)
	}
	if recvErr.Error() != rejection.Error() {
		t.Fatalf("Recv status = %q; want %q", recvErr, rejection)
	}

	close(releaseSrv)
	<-srvDone
}

// TestUnrecoverableSendFailureDropsSession pins the other half: a transport that
// reports the exchange dead (ReceiveOpen false) is not ErrSendClosed, and the
// grpc client session is dropped rather than reused.
func TestUnrecoverableSendFailureDropsSession(t *testing.T) {
	t.Parallel()
	cli, lis := fake.HTTPLoopback()
	t.Cleanup(func() { _ = cli.Close() })

	f := newTestFraming(t)
	method := descriptor.MustMethod("echo.v1.Echo.Echo", descriptor.Unary)
	spec := framing.SessionSpec{CodecName: "proto"}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	go func() {
		srvConn, err := lis.Accept(ctx)
		if err != nil {
			return
		}
		ss, err := f.NewServerSession(ctx, srvConn, spec)
		if err != nil {
			return
		}
		defer func() { _ = ss.Close() }()
		md := metadata.New(metadata.RoleResponder, nil)
		sc, err := ss.AcceptCall(ctx, framing.CallSpec{Metadata: md})
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

	cs, err := f.NewClientSession(ctx, cli, spec)
	if err != nil {
		t.Fatalf("NewClientSession: %v", err)
	}
	defer func() { _ = cs.Close() }()

	md := metadata.New(metadata.RoleInitiator, nil)
	call, err := cs.OpenCall(ctx, method, framing.CallSpec{Metadata: md})
	if err != nil {
		t.Fatalf("OpenCall: %v", err)
	}
	defer func() { _ = call.Close() }()
	st := stream.Wrap(call, bytesCodec{})

	cli.FailClientWrites(false, errBrokenWire)
	sendErr := st.Send("part1")
	if errors.Is(sendErr, stream.ErrSendClosed) {
		t.Fatalf("Send on a dead exchange = %v; want the raw failure, not ErrSendClosed", sendErr)
	}
	if !errors.Is(sendErr, errBrokenWire) {
		t.Fatalf("Send error = %v; want %v", sendErr, errBrokenWire)
	}
	if cs.Reusable() {
		t.Fatal("session reusable after an unrecoverable send failure")
	}
}

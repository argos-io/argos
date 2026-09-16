package envelope_test

import (
	"context"
	"testing"
	"time"

	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/framing"
	"github.com/argos-io/argos/framing/envelope"
	"github.com/argos-io/argos/metadata"
)

// A handler finishing its call pokes the carrier read deadline (call.Close ->
// wakeRead). If that lands while the NEXT AcceptCall is already reading, the
// accept must not fail: the deadline move is our own control event, not a peer
// fault or an OpenTimeout expiry.
func TestAcceptSurvivesForeignDeadlineWake(t *testing.T) {
	cliConn, srvConn := tcpBytePair(t)
	fr := envelope.New()
	srvSess, err := fr.NewServerSession(context.Background(), srvConn, framing.SessionSpec{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = srvSess.Close() }()

	method := descriptor.MustMethod("svc.Wake", descriptor.Unary)
	frame := func(f envelope.Frame) []byte {
		b, err := envelope.MarshalFrame(f)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}

	// Call 1, served normally.
	if _, err := cliConn.Write(frame(envelope.Frame{
		Type: envelope.TypeOpen, CallID: 1, Method: method.FullName(), Flags: envelope.FlagOpenEnd,
	})); err != nil {
		t.Fatal(err)
	}
	sc1, err := srvSess.AcceptCall(context.Background(), framing.CallSpec{
		Metadata: metadata.New(metadata.RoleResponder, nil),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := sc1.Finish(nil); err != nil {
		t.Fatal(err)
	}

	// Accept #2 starts and blocks part-way into call 2's OPEN.
	type res struct {
		c   framing.ServerCall
		err error
	}
	done := make(chan res, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		c, err := srvSess.AcceptCall(ctx, framing.CallSpec{
			Metadata: metadata.New(metadata.RoleResponder, nil),
		})
		done <- res{c, err}
	}()

	open2 := frame(envelope.Frame{
		Type: envelope.TypeOpen, CallID: 2, Method: method.FullName(), Flags: envelope.FlagOpenEnd,
	})
	time.Sleep(50 * time.Millisecond)
	if _, err := cliConn.Write(open2[:6]); err != nil { // partial frame
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)

	// The previous handler returns now: Close pokes the read deadline.
	_ = sc1.Close()
	time.Sleep(50 * time.Millisecond)

	if _, err := cliConn.Write(open2[6:]); err != nil {
		t.Fatal(err)
	}

	select {
	case r := <-done:
		if r.err != nil {
			t.Fatalf("AcceptCall#2 failed because an unrelated Close moved the read deadline: %v", r.err)
		}
		if r.c.Method() != method.FullName() {
			t.Fatalf("method = %q, want %q", r.c.Method(), method.FullName())
		}
		_ = r.c.Finish(nil)
		_ = r.c.Close()
	case <-time.After(8 * time.Second):
		t.Fatal("AcceptCall#2 never returned")
	}
}

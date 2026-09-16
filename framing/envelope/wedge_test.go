package envelope_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/framing"
	"github.com/argos-io/argos/framing/envelope"
	"github.com/argos-io/argos/metadata"
	"github.com/argos-io/argos/status"
)

// TestEarlyStatusCallDoesNotWedgeNextAccept guards the accept/demux handshake
// against the two defects that used to wedge a Sequential server session: a
// reader parked forever while AcceptCall waits for a frame nobody will hand it.
//
//  1. Lost wakeup. The demux state change was announced over a capacity-1 token
//     channel, and the predicate after an accept handoff compared mode by value.
//     Finish, Close and the next AcceptCall publish three changes in a row, so
//     tokens were dropped; and mode returns to modeAccepting for the next
//     accept, so "call finished plus a fresh accept" was indistinguishable from
//     "nothing happened". Now every change bumps stateVer and closes wakeCh, and
//     waiters capture the channel in the same critical section that evaluates
//     the predicate; the post-handoff wait keys on the accept attempt's identity.
//
//  2. Destroyed bytes. wakeRead() expires the carrier deadline from another
//     goroutine, which aborted frame reads that had already taken the length
//     prefix off the socket. Those bytes were dropped and the timeout was
//     classified as benign, so every later frame boundary was wrong. Frame reads
//     now accumulate in session.frameBuf and resume where they stopped.
//
// Only the two together close it: an early-status call is what puts the three
// state changes back to back, and fixing the wakeup alone just moves the wedge
// to a later round where a deadline happens to land inside a partial read.
//
// 2000 rounds is the instrument's sensitivity, not a round number. A wedge was
// a ~0.2%-per-round event, so 300 rounds caught it in well under half of runs
// and a fix could look clean by luck; at 2000 one run is a verdict. A clean run
// costs a fraction of a second because non-wedged rounds are sub-millisecond.
func TestEarlyStatusCallDoesNotWedgeNextAccept(t *testing.T) {
	cliConn, srvConn := tcpBytePair(t)
	fr := envelope.New()

	srvSess, err := fr.NewServerSession(context.Background(), srvConn, framing.SessionSpec{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = srvSess.Close() }()

	method := descriptor.MustMethod("svc.Wedge", descriptor.Unary)

	// Driven with raw bytes: the wedge is a server-session defect, and a client
	// session would carry its own state across rounds.
	writeFrames := func(t *testing.T, frames ...envelope.Frame) {
		t.Helper()
		var raw []byte
		for _, f := range frames {
			b, err := envelope.MarshalFrame(f)
			if err != nil {
				t.Fatal(err)
			}
			raw = append(raw, b...)
		}
		if _, err := cliConn.Write(raw); err != nil {
			t.Fatal(err)
		}
	}

	// 2000 rounds is the instrument's sensitivity. A wedge is a per-round event
	// at roughly 0.2%, so 300 rounds caught it in well under half of runs and a
	// fix could look clean by luck; at 2000 the detection probability is ~98%
	// for one run, which is what makes a single run a verdict instead of a
	// sample. A clean run costs a few seconds because non-wedged rounds are
	// sub-millisecond.
	const rounds = 2000
	for i := 0; i < rounds; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		badID := uint64(i*2 + 1)
		okID := badID + 1

		// A well-framed OPEN with an empty method, plus the residual DATA/END a
		// peer that already started sending would produce.
		writeFrames(t,
			envelope.Frame{Type: envelope.TypeOpen, CallID: badID, Method: ""},
			envelope.Frame{Type: envelope.TypeData, CallID: badID, Data: []byte("stale")},
			envelope.Frame{Type: envelope.TypeEnd, CallID: badID},
		)

		rejected, err := srvSess.AcceptCall(ctx, framing.CallSpec{
			Metadata: metadata.New(metadata.RoleResponder, nil),
		})
		if errors.Is(err, context.DeadlineExceeded) {
			cancel()
			t.Fatalf("WEDGED at round %d of %d (rejected call); a clean run must "+
				"complete all %d rounds", i, rounds, rounds)
		}
		if !errors.Is(err, framing.ErrCallRejected) {
			cancel()
			t.Fatalf("round %d: err = %v, want ErrCallRejected", i, err)
		}
		if rejected != nil {
			_ = rejected.Finish(status.Error(status.InvalidArgument, "rejected"))
			_ = rejected.Close()
		}

		// A legal call on the same connection must still be served.
		writeFrames(t,
			envelope.Frame{Type: envelope.TypeOpen, CallID: okID, Method: method.FullName()},
			envelope.Frame{Type: envelope.TypeData, CallID: okID, Data: []byte("q")},
			envelope.Frame{Type: envelope.TypeEnd, CallID: okID},
		)

		served, err := srvSess.AcceptCall(ctx, framing.CallSpec{
			Metadata: metadata.New(metadata.RoleResponder, nil),
		})
		if errors.Is(err, context.DeadlineExceeded) {
			cancel()
			t.Fatalf("WEDGED at round %d of %d (after an early-status call): the "+
				"recvLoop is parked while AcceptCall waits; a clean run must "+
				"complete all %d rounds", i, rounds, rounds)
		}
		if err != nil {
			cancel()
			t.Fatalf("round %d: second AcceptCall: %v", i, err)
		}
		if served.Method() != method.FullName() {
			cancel()
			t.Fatalf("round %d: method %q, want %q", i, served.Method(), method.FullName())
		}
		_, _, _ = served.Recv()
		_, _, _ = served.Recv() // END
		_ = served.Send([]byte("r"))
		_ = served.Finish(nil)
		_ = served.Close()
		cancel()
	}
}

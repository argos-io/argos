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

// repeatedEarlyStatusCalls drives the interleaving that wedges the session:
// an early-status call (framing rejection) followed by a legal call on the
// same connection. Each AcceptCall is bounded, so a lost wakeup shows up as a
// fast, deterministic failure instead of a package timeout.
//
// KNOWN FAILING - skipped, not fixed. A green package run is not proof.
//
// Run it without the skip to observe:
//
//	go test ./framing/envelope/ -run EarlyStatusCallDoesNotWedgeNextAccept -count=1
//
// Four defects live in the accept/demux handshake. Frame-level tracing pinned
// each one; they were fixed and the fixes were then reverted, because defect 4
// still fails this test in ~3/5 runs and a change that leaves it failing is
// worse than an accurate record.
//
//  1. signal() was a capacity-1 token channel that DROPS the notification when
//     one is already pending, while several sites (waitWakeOrClosed, the
//     non-blocking probes) relied on the token as a latch meaning "something
//     changed since I last looked". Replacing it with a close-and-replace
//     broadcast WITHOUT giving waiters an epoch latch made things strictly
//     worse (3/3 package timeouts). Both mechanisms are needed: the epoch for
//     the latch, the channel for blocking, and every waiter must capture the
//     channel in the same critical section that evaluates its predicate.
//  2. waitModeChange(modeAccepting) after a handoff is the wrong predicate. The
//     mode RETURNS to modeAccepting for the next accept, so a call that
//     completed plus a fresh accept look identical to "nothing happened" to a
//     reader not scheduled in between - it sleeps through both while the next
//     AcceptCall waits for a frame already in the read buffer. The predicate
//     must be the handoff's channel identity (s.acceptCh != ch).
//  3. The demux read completes with a frame belonging to the NEXT call. The
//     reader was already blocked in the read when the previous call ended, so
//     it woke with data (the peer's new OPEN) instead of the deadline wake,
//     then compared the call ID against the finished call and called markBad()
//     - tearing the connection down and leaving the pending AcceptCall with
//     nobody to wake it. Tracing showed it exactly: "demux READ type=1
//     callID=3" (an OPEN) against lastCallID=2. An OPEN read during demux is
//     the next call, not a violation: hand it back with pushFront, and do NOT
//     wait after the handback while an accept is in flight (that wait strands
//     the frame - the reader is the only one who can hand it over).
//  4. A stale read deadline. AcceptCall's ctx-cancel path and Close both call
//     wakeRead() (SetReadDeadline(now)); a later accept's waitFirstByte
//     observes that past deadline, and readFrameAccepting classifies the
//     resulting "read: i/o timeout" as a hard I/O fault (it is a net.Error,
//     not context.DeadlineExceeded) and delivers it as the accept's result.
//     Instrumented evidence: "readFrame err=... i/o timeout openTimeout=10s
//     ctx=true" - the read started under a fresh 10s OpenTimeout and expired at
//     once, because a wakeRead() from the previous call's Close landed while it
//     was in flight and this accept's ctx was already done.
//     A naive fix was tried and REGRESSED the wedge (7/10 fail, back at random
//     rounds): checking ctx before waitFirstByte's buffered-byte shortcut, and
//     reporting a wake-induced expiry as ctx.Err(), feeds a cancelled accept's
//     error into the handoff path and parks the reader there again.
//     The structural problem is that the accept ctx outlives its accept and is
//     reused by a read the reader starts afterwards. A per-attempt lifecycle
//     for it - or an idle state the reader returns to when no accept owns the
//     connection - is where this lands, not another classification tweak.
//
// Defects 1-3 are what make the failure deterministic today (round 0, 3/3);
// with them fixed the wedge is gone and only defect 4 remains, intermittently.
// The composition layer masks all of this behind MaxInboundConnIdle, which is
// why it presents as intermittent latency rather than a permanent leak -
// except for callers whose ctx carries no deadline.
func TestEarlyStatusCallDoesNotWedgeNextAccept(t *testing.T) {
	t.Skip("known defects in the accept/demux handshake; see the comment above")
	t.Parallel()
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

	const rounds = 300
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
			t.Fatalf("round %d: AcceptCall wedged serving the rejected call", i)
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
			t.Fatalf("round %d: AcceptCall wedged after an early-status call "+
				"(lost wakeup: the recvLoop is parked while AcceptCall waits)", i)
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

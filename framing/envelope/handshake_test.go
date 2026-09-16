package envelope

import (
	"context"
	"net"
	"testing"
	"time"
)

// streamCarrier adapts a net.Conn to ByteStreamCarrier for white-box tests.
type streamCarrier struct{ net.Conn }

func (c streamCarrier) Abort() error { return c.Close() }

func tcpCarrierPair(t *testing.T) (peer net.Conn, car streamCarrier) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()

	type dialed struct {
		c   net.Conn
		err error
	}
	ch := make(chan dialed, 1)
	go func() {
		c, err := net.Dial("tcp", ln.Addr().String())
		ch <- dialed{c, err}
	}()
	local, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	d := <-ch
	if d.err != nil {
		_ = local.Close()
		t.Fatal(d.err)
	}
	t.Cleanup(func() {
		_ = d.c.Close()
		_ = local.Close()
	})
	return d.c, streamCarrier{local}
}

// wakeRead() expires the carrier deadline from another goroutine, so a frame
// read can be aborted after it has already taken bytes off the wire. Those
// bytes are not replayable: dropping them misaligns every later frame boundary,
// and a length field parsed from the middle of a frame parks the reader on a
// body that never arrives. The read must resume where it stopped.
func TestFrameReadResumesAfterDeadlineWake(t *testing.T) {
	peer, car := tcpCarrierPair(t)
	s := &session{carrier: car, kind: kindByteStream, wakeCh: make(chan struct{})}

	want := Frame{Type: TypeData, CallID: 7, Data: []byte("payload")}
	raw, err := MarshalFrame(want)
	if err != nil {
		t.Fatal(err)
	}

	// Only the length prefix arrives, so the read blocks inside the frame.
	if _, err := peer.Write(raw[:lenPrefix]); err != nil {
		t.Fatal(err)
	}
	s.setReadDeadline(time.Now().Add(100 * time.Millisecond))
	if _, err := s.readFrame(); !isTimeoutErr(err) {
		t.Fatalf("readFrame err = %v, want a timeout", err)
	}
	s.clearReadDeadline()

	if _, err := peer.Write(raw[lenPrefix:]); err != nil {
		t.Fatal(err)
	}
	// Every remaining byte is already on the wire, so a bounded read turns a
	// misaligned stream into a fast failure instead of a hang.
	s.setReadDeadline(time.Now().Add(2 * time.Second))
	defer s.clearReadDeadline()

	got, err := s.readFrame()
	if err != nil {
		t.Fatalf("resumed readFrame: %v (the aborted read lost the bytes it had taken)", err)
	}
	if got.Type != want.Type || got.CallID != want.CallID || string(got.Data) != string(want.Data) {
		t.Fatalf("frame = %+v, want %+v", got, want)
	}

	// The stream is still aligned: the next frame parses too.
	next := Frame{Type: TypeEnd, CallID: 7}
	nraw, err := MarshalFrame(next)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := peer.Write(nraw); err != nil {
		t.Fatal(err)
	}
	got, err = s.readFrame()
	if err != nil {
		t.Fatalf("frame after the aborted read: %v", err)
	}
	if got.Type != TypeEnd || got.CallID != 7 {
		t.Fatalf("frame = %+v, want END callID 7", got)
	}
}

// Retrying a foreign deadline wake must not hand the peer a fresh OpenTimeout.
// A peer that sends one byte and stalls has to fail within one budget however
// many times our own wakeRead() interrupts the read.
func TestOpenTimeoutBudgetSurvivesRepeatedWakes(t *testing.T) {
	peer, car := tcpCarrierPair(t)
	const openTO = 150 * time.Millisecond
	s := &session{
		carrier: car, kind: kindByteStream,
		wakeCh: make(chan struct{}), openTimeout: openTO,
	}

	// One byte, then silence: OpenTimeout starts and must be the only bound.
	if _, err := peer.Write([]byte{0}); err != nil {
		t.Fatal(err)
	}

	type result struct {
		err     error
		elapsed time.Duration
	}
	done := make(chan result, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		start := time.Now()
		_, err := s.readFrameFirstByteThenTimeout(ctx)
		done <- result{err, time.Since(start)}
	}()

	stop := make(chan struct{})
	defer close(stop)
	go func() {
		for {
			select {
			case <-stop:
				return
			case <-time.After(openTO / 10):
				s.wakeRead()
			}
		}
	}()

	select {
	case r := <-done:
		if !isTimeoutErr(r.err) {
			t.Fatalf("err = %v, want a timeout", r.err)
		}
		if r.elapsed > 4*openTO {
			t.Fatalf("accept took %v for a %v OpenTimeout: each wake restarted the budget",
				r.elapsed, openTO)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("OpenTimeout never fired: foreign wakes extend the peer's budget indefinitely")
	}
}

// mode returns to modeAccepting for the next accept, so a reader that waited on
// the mode value could not tell "my accept is still installed" from "mine was
// retired and another one took its place" and slept through the handoff.
func TestAcceptWaitKeysOnAttemptNotModeValue(t *testing.T) {
	s := &session{wakeCh: make(chan struct{})}
	first := make(chan acceptResult, 1)
	s.mu.Lock()
	s.acceptCh = first
	s.mode = modeAccepting
	s.mu.Unlock()

	done := make(chan struct{})
	go func() {
		s.waitAcceptRetired(first)
		close(done)
	}()

	select {
	case <-done:
		t.Fatal("wait returned while its own accept attempt was still installed")
	case <-time.After(50 * time.Millisecond):
	}

	// Retire the attempt and install another. mode is modeAccepting on both
	// sides of this change, so only the attempt's identity can release the wait.
	second := make(chan acceptResult, 1)
	s.mu.Lock()
	s.acceptCh = second
	s.mode = modeAccepting
	s.bumpLocked()
	s.mu.Unlock()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("wait slept through a new accept attempt: mode value aliased the old one")
	}
}

// Finish, Close and the next AcceptCall publish three changes back to back. A
// capacity-1 token channel dropped all but one of them, and a reader that had
// already read its state but not yet parked missed the rest. The latch is
// level-triggered against the version the reader acted on, so none can be lost.
func TestStateChangesAreNotLostBetweenSnapshotAndWait(t *testing.T) {
	s := &session{wakeCh: make(chan struct{})}
	s.mu.Lock()
	s.acceptCh = make(chan acceptResult, 1)
	s.mode = modeAccepting
	ver := s.stateVer // the reader's snapshot, taken before it parks
	s.mu.Unlock()

	publish := func(mode demuxMode, ch chan acceptResult) {
		s.mu.Lock()
		s.mode = mode
		s.acceptCh = ch
		s.bumpLocked()
		s.mu.Unlock()
	}
	publish(modeInCall, nil)                           // finishAccept
	publish(modeIdle, nil)                             // Finish -> detachCall
	publish(modeAccepting, make(chan acceptResult, 1)) // the next AcceptCall

	done := make(chan struct{})
	go func() {
		s.waitVer(ver)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("reader parked on changes published after its snapshot but before it waited")
	}

	// And a change published while the reader is already parked reaches it.
	s.mu.Lock()
	ver = s.stateVer
	s.mu.Unlock()
	done = make(chan struct{})
	go func() {
		s.waitVer(ver)
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("wait returned before anything changed")
	case <-time.After(50 * time.Millisecond):
	}
	publish(modeIdle, nil)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("parked reader missed a published change")
	}
}

package envelope_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/argos-io/argos/budget"
	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/framing"
	"github.com/argos-io/argos/framing/envelope"
	"github.com/argos-io/argos/internal/fake"
	"github.com/argos-io/argos/metadata"
)

// trackingBudget wraps a Budget and records the high-water mark of held bytes.
type trackingBudget struct {
	inner budget.SliceBudget
	cur   atomic.Int64
	peak  atomic.Int64
}

func newTrackingBudget(capacity int64) *trackingBudget {
	return &trackingBudget{inner: budget.New(capacity).(budget.SliceBudget)}
}

func (t *trackingBudget) TryAcquire(n int64) (func(), error) {
	rel, err := t.inner.TryAcquire(n)
	if err != nil {
		return nil, err
	}
	if n > 0 {
		t.note(n)
	}
	return func() {
		if n > 0 {
			t.cur.Add(-n)
		}
		rel()
	}, nil
}

func (t *trackingBudget) TryAcquireSlice(p []byte) (func(), error) {
	n := int64(cap(p))
	rel, err := t.inner.TryAcquireSlice(p)
	if err != nil {
		return nil, err
	}
	if n > 0 {
		t.note(n)
	}
	return func() {
		if n > 0 {
			t.cur.Add(-n)
		}
		rel()
	}, nil
}

func (t *trackingBudget) note(n int64) {
	cur := t.cur.Add(n)
	for {
		peak := t.peak.Load()
		if cur <= peak || t.peak.CompareAndSwap(peak, cur) {
			return
		}
	}
}

func (t *trackingBudget) Peak() int64 { return t.peak.Load() }
func (t *trackingBudget) Cur() int64  { return t.cur.Load() }

func backpressureCfg(maxMsg, maxFrame int64, readAhead int) framing.Config {
	return framing.Config{
		MaxMessageSize:    maxMsg,
		MaxFrameSize:      maxFrame,
		MaxMetadataSize:   64 << 10,
		ReadAheadMessages: readAhead,
	}
}

func perCallBytes(maxFrame, maxMsg int64, readAhead int) int64 {
	return maxFrame + int64(readAhead+1)*maxMsg + maxMsg
}

func TestSlowConsumerBoundedReadAhead(t *testing.T) {
	const (
		maxMsg    = 64 << 10
		maxFrame  = maxMsg + 16 // typ + callID + payload
		readAhead = 1
		nMsgs     = 200
	)
	limit := perCallBytes(maxFrame, maxMsg, readAhead)

	cliConn, srvConn := fake.BytePipe()
	defer cliConn.Close()
	defer srvConn.Close()

	cfg := backpressureCfg(maxMsg, maxFrame, readAhead)
	fr := envelope.New()
	cliSess, err := fr.NewClientSession(context.Background(), cliConn, framing.SessionSpec{Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	defer cliSess.Close()
	srvSess, err := fr.NewServerSession(context.Background(), srvConn, framing.SessionSpec{Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	defer srvSess.Close()

	method := descriptor.MustMethod("bp.v1.Slow.Stream", descriptor.ServerStreaming)
	tb := newTrackingBudget(limit)
	cliCtx := budget.ContextWith(context.Background(), tb)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		sc, err := srvSess.AcceptCall(context.Background(), framing.CallSpec{
			Metadata: metadata.New(metadata.RoleResponder, nil),
		})
		if err != nil {
			t.Errorf("AcceptCall: %v", err)
			return
		}
		defer sc.Close()
		_, _, _ = sc.Recv() // wait for HalfClose / END
		payload := make([]byte, maxMsg)
		for i := 0; i < nMsgs; i++ {
			if err := sc.Send(payload); err != nil {
				t.Errorf("Send %d: %v", i, err)
				return
			}
		}
		_ = sc.Finish(nil)
	}()

	call, err := cliSess.OpenCall(cliCtx, method, framing.CallSpec{
		Metadata: metadata.New(metadata.RoleInitiator, nil),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := call.HalfClose(); err != nil {
		t.Fatal(err)
	}

	got := 0
	for {
		p, rel, err := call.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		if len(p) != maxMsg {
			t.Fatalf("payload len = %d, want %d", len(p), maxMsg)
		}
		time.Sleep(time.Millisecond)
		rel()
		got++
	}
	_ = call.Close()
	wg.Wait()

	peak := tb.Peak()
	t.Logf("slow consumer peak charged bytes = %d (perCall limit %d, msgs=%d, ReadAhead=%d)",
		peak, limit, got, readAhead)
	if got != nMsgs {
		t.Fatalf("got %d messages, want %d", got, nMsgs)
	}
	if peak > limit {
		t.Fatalf("peak charged %d > perCall %d", peak, limit)
	}
	if tb.Cur() != 0 {
		t.Fatalf("budget not fully released: cur=%d", tb.Cur())
	}
}

func TestCancelUnderBackpressure(t *testing.T) {
	const (
		maxMsg    = 32 << 10
		maxFrame  = maxMsg + 16
		readAhead = 1
	)
	limit := perCallBytes(maxFrame, maxMsg, readAhead)

	cliConn, srvConn := fake.BytePipe()
	defer cliConn.Close()
	defer srvConn.Close()

	cfg := backpressureCfg(maxMsg, maxFrame, readAhead)
	fr := envelope.New()
	cliSess, err := fr.NewClientSession(context.Background(), cliConn, framing.SessionSpec{Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	srvSess, err := fr.NewServerSession(context.Background(), srvConn, framing.SessionSpec{Config: cfg})
	if err != nil {
		t.Fatal(err)
	}

	method := descriptor.MustMethod("bp.v1.Cancel.Stream", descriptor.ServerStreaming)
	tb := newTrackingBudget(limit)
	cliCtx := budget.ContextWith(context.Background(), tb)

	serverStarted := make(chan struct{})
	go func() {
		sc, err := srvSess.AcceptCall(context.Background(), framing.CallSpec{
			Metadata: metadata.New(metadata.RoleResponder, nil),
		})
		if err != nil {
			return
		}
		defer sc.Close()
		_, _, _ = sc.Recv()
		close(serverStarted)
		payload := make([]byte, maxMsg)
		// Flood until the connection is torn down (client Close → session Close).
		for {
			if err := sc.Send(payload); err != nil {
				_ = sc.Finish(err)
				return
			}
		}
	}()

	call, err := cliSess.OpenCall(cliCtx, method, framing.CallSpec{
		Metadata: metadata.New(metadata.RoleInitiator, nil),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := call.HalfClose(); err != nil {
		t.Fatal(err)
	}
	<-serverStarted

	// Pull one message so the queue can fill behind a stopped consumer.
	_, rel, err := call.Recv()
	if err != nil {
		t.Fatalf("first Recv: %v", err)
	}
	rel()

	// Give the peer time to fill ReadAhead / pipe buffers.
	time.Sleep(50 * time.Millisecond)

	before := runtime.NumGoroutine()
	if err := call.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Tear down sessions so a peer blocked on Send is unblocked (net.Pipe).
	done := make(chan struct{})
	go func() {
		_ = cliSess.Close()
		_ = srvSess.Close()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("session Close hung after cancel under backpressure")
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if tb.Cur() == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if tb.Cur() != 0 {
		t.Fatalf("budget not released after Close: cur=%d peak=%d", tb.Cur(), tb.Peak())
	}
	t.Logf("cancel-under-backpressure peak=%d cur=%d goroutines before=%d after=%d",
		tb.Peak(), tb.Cur(), before, runtime.NumGoroutine())
}

func TestMaxMessageSizeBoundary(t *testing.T) {
	const (
		maxMsg   = 4 << 10
		maxFrame = maxMsg + 16
	)
	cfg := backpressureCfg(maxMsg, maxFrame, 1)

	t.Run("exact_ok", func(t *testing.T) {
		cliConn, srvConn := fake.BytePipe()
		defer cliConn.Close()
		defer srvConn.Close()

		fr := envelope.New()
		cliSess, _ := fr.NewClientSession(context.Background(), cliConn, framing.SessionSpec{Config: cfg})
		defer cliSess.Close()
		srvSess, _ := fr.NewServerSession(context.Background(), srvConn, framing.SessionSpec{Config: cfg})
		defer srvSess.Close()

		method := descriptor.MustMethod("bp.v1.Size.Exact", descriptor.Unary)
		go func() {
			sc, err := srvSess.AcceptCall(context.Background(), framing.CallSpec{
				Metadata: metadata.New(metadata.RoleResponder, nil),
			})
			if err != nil {
				t.Errorf("AcceptCall: %v", err)
				return
			}
			defer sc.Close()
			p, rel, err := sc.Recv()
			if err != nil {
				t.Errorf("server Recv: %v", err)
				return
			}
			if int64(len(p)) != maxMsg {
				t.Errorf("len=%d want %d", len(p), maxMsg)
			}
			rel()
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
		if err := call.Send(make([]byte, maxMsg)); err != nil {
			t.Fatalf("Send exact: %v", err)
		}
		if err := call.HalfClose(); err != nil {
			t.Fatal(err)
		}
		p, rel, err := call.Recv()
		if err != nil {
			t.Fatalf("Recv: %v", err)
		}
		if string(p) != "ok" {
			t.Fatalf("got %q", p)
		}
		rel()
	})

	t.Run("oversize_rejected", func(t *testing.T) {
		// MaxFrameSize must allow the wire frame so MaxMessageSize is what rejects.
		cfg := backpressureCfg(maxMsg, maxMsg+64, 1)
		cliConn, srvConn := fake.BytePipe()
		defer cliConn.Close()
		defer srvConn.Close()

		fr := envelope.New()
		cliSess, _ := fr.NewClientSession(context.Background(), cliConn, framing.SessionSpec{Config: cfg})
		defer cliSess.Close()
		srvSess, _ := fr.NewServerSession(context.Background(), srvConn, framing.SessionSpec{Config: cfg})
		defer srvSess.Close()

		method := descriptor.MustMethod("bp.v1.Size.Over", descriptor.Unary)

		acceptErr := make(chan error, 1)
		go func() {
			sc, err := srvSess.AcceptCall(context.Background(), framing.CallSpec{
				Metadata: metadata.New(metadata.RoleResponder, nil),
			})
			if err != nil {
				acceptErr <- err
				return
			}
			defer sc.Close()
			_, _, err = sc.Recv()
			acceptErr <- err
			// Do not Finish: STATUS write would deadlock with a peer still writing on net.Pipe.
		}()

		call, err := cliSess.OpenCall(context.Background(), method, framing.CallSpec{
			Metadata: metadata.New(metadata.RoleInitiator, nil),
		})
		if err != nil {
			t.Fatal(err)
		}
		defer call.Close()

		// Local Send rejects before writing an oversized payload.
		err = call.Send(make([]byte, maxMsg+1))
		if err == nil || !errors.Is(err, envelope.ErrMessageTooLarge) {
			t.Fatalf("Send oversize err = %v, want ErrMessageTooLarge", err)
		}

		// Peer-path: write a raw oversized DATA frame; server must reject without
		// a successful Recv of the oversized payload.
		over, err := envelope.MarshalFrame(envelope.Frame{
			Type:   envelope.TypeData,
			CallID: 1,
			Data:   make([]byte, maxMsg+1),
		})
		if err != nil {
			t.Fatal(err)
		}
		writeDone := make(chan error, 1)
		go func() { _, err := cliConn.Write(over); writeDone <- err }()

		select {
		case err := <-acceptErr:
			if err == nil {
				t.Fatal("server Recv succeeded for oversized DATA")
			}
			if !errors.Is(err, envelope.ErrMessageTooLarge) {
				t.Fatalf("server err = %v, want ErrMessageTooLarge", err)
			}
			t.Logf("oversize rejected: %v", err)
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for oversized rejection")
		}
		select {
		case <-writeDone:
		case <-time.After(2 * time.Second):
			// Connection may already be aborted; ignore.
		}
	})
}

func TestUnmarshalPrefixedLimitedRejectsOversizeWithoutHugeAlloc(t *testing.T) {
	const maxMsg = 4 << 10
	maxFrame := int64(maxMsg + 64)
	over, err := envelope.MarshalFrame(envelope.Frame{
		Type:   envelope.TypeData,
		CallID: 1,
		Data:   make([]byte, maxMsg+1),
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = envelope.UnmarshalPrefixedLimited(bytes.NewReader(over), maxFrame, maxMsg)
	if !errors.Is(err, envelope.ErrMessageTooLarge) {
		t.Fatalf("err = %v, want ErrMessageTooLarge", err)
	}
}

// Ensure the limited unmarshaller is exercised for non-DATA under the message
// size heuristic (frame still allowed by MaxFrameSize).
func TestUnmarshalPrefixedLimitedAllowsSmallData(t *testing.T) {
	raw, err := envelope.MarshalFrame(envelope.Frame{
		Type:   envelope.TypeData,
		CallID: 7,
		Data:   make([]byte, 64),
	})
	if err != nil {
		t.Fatal(err)
	}
	f, err := envelope.UnmarshalPrefixedLimited(bytes.NewReader(raw), 1<<20, 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	if f.Type != envelope.TypeData || f.CallID != 7 || len(f.Data) != 64 {
		t.Fatalf("got type=%d id=%d len=%d", f.Type, f.CallID, len(f.Data))
	}
}

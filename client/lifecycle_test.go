package client_test

import (
	"context"
	"errors"
	"io"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/argos-io/argos"
	"github.com/argos-io/argos/client"
	"github.com/argos-io/argos/codec"
	"github.com/argos-io/argos/framing"
	"github.com/argos-io/argos/internal/fake"
	"github.com/argos-io/argos/transport"
)

// freshLoopback returns a Protocol whose Assemble builds a new Transport×Framing
// pair (factory-once / isolation tests).
func freshLoopback(t *testing.T, factoryCalls *atomic.Int64, dialed *[]*fake.ByteConn, dialMu *sync.Mutex) argos.Protocol {
	t.Helper()
	var current atomic.Pointer[loopbackPair]
	build := func() *loopbackPair {
		if factoryCalls != nil {
			factoryCalls.Add(1)
		}
		f := fake.NewFraming(framing.Sequential)
		tr := &loopTransport{
			dial: func(ctx context.Context, endpoint string) (transport.Conn, error) {
				cli, srv := fake.BytePipe()
				if dialMu != nil && dialed != nil {
					dialMu.Lock()
					*dialed = append(*dialed, cli)
					dialMu.Unlock()
				}
				go runEchoServer(t, f, srv)
				return cli, nil
			},
		}
		p := &loopbackPair{tr: tr, f: f}
		current.Store(p)
		return p
	}
	return argos.Protocol{
		Transport: func() (transport.Transport, error) {
			if p := current.Load(); p != nil {
				return p.tr, nil
			}
			return build().tr, nil
		},
		Framing: func() (framing.Framing, error) {
			if p := current.Load(); p != nil {
				return p.f, nil
			}
			return build().f, nil
		},
		Codec: func() (codec.Codec, error) { return bytesCodec{}, nil },
	}
}

func TestProtocolAssembleOncePerClient(t *testing.T) {
	t.Parallel()
	var calls atomic.Int64
	protocol := freshLoopback(t, &calls, nil, nil)
	cli, err := client.New(
		argos.WithServiceName(testService),
		argos.WithMaxConcurrentCalls(4),
		argos.WithMaxBufferedBytes(4*16*1024*1024),
		argos.WithProtocol(protocol),
		argos.WithTarget(testTarget),
	)
	if err != nil {
		t.Fatalf("client.New: %v", err)
	}
	defer cli.Close()

	if got := calls.Load(); got != 1 {
		t.Fatalf("protocol assemble calls = %d, want 1", got)
	}
	// Open must not re-invoke the factory.
	cs, err := cli.Open(context.Background(), testMethod(t))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	_ = cs.Close()
	if got := calls.Load(); got != 1 {
		t.Fatalf("protocol assemble calls after Open = %d, want 1", got)
	}
}

func TestClientsFromSameFactoryIsolated(t *testing.T) {
	t.Parallel()
	var calls atomic.Int64
	var transports []*loopTransport
	var framings []*fake.Framing
	var mu sync.Mutex

	protocol := argos.Protocol{
		Transport: func() (transport.Transport, error) {
			calls.Add(1)
			f := fake.NewFraming(framing.Sequential)
			tr := &loopTransport{
				dial: func(ctx context.Context, endpoint string) (transport.Conn, error) {
					cli, srv := fake.BytePipe()
					go runEchoServer(t, f, srv)
					return cli, nil
				},
			}
			mu.Lock()
			transports = append(transports, tr)
			framings = append(framings, f)
			mu.Unlock()
			return tr, nil
		},
		Framing: func() (framing.Framing, error) {
			mu.Lock()
			fr := framings[len(framings)-1]
			mu.Unlock()
			return fr, nil
		},
		Codec: func() (codec.Codec, error) { return bytesCodec{}, nil },
	}

	cfg := &argos.Config{
		MaxConcurrentCalls: 4,
		MaxBufferedBytes:   4 * 16 * 1024 * 1024,
	}

	cli1, err := client.New(argos.WithConfig(cfg),
		argos.WithServiceName(testService), argos.WithTarget(testTarget), argos.WithProtocol(protocol))
	if err != nil {
		t.Fatalf("client.New #1: %v", err)
	}
	cli2, err := client.New(argos.WithConfig(cfg),
		argos.WithServiceName(testService), argos.WithTarget(testTarget), argos.WithProtocol(protocol))
	if err != nil {
		t.Fatalf("client.New #2: %v", err)
	}
	defer cli1.Close()
	defer cli2.Close()

	if got := calls.Load(); got != 2 {
		t.Fatalf("protocol Transport factory calls = %d, want 2", got)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(transports) != 2 || len(framings) != 2 {
		t.Fatalf("created %d transports / %d framings, want 2/2", len(transports), len(framings))
	}
	if transports[0] == transports[1] {
		t.Fatal("clients share Transport instance")
	}
	if framings[0] == framings[1] {
		t.Fatal("clients share Framing instance")
	}

	if err := cli1.Close(); err != nil {
		t.Fatalf("cli1.Close: %v", err)
	}
	if transports[0].closeN.Load() != 1 {
		t.Fatalf("cli1 Transport.Close count = %d, want 1", transports[0].closeN.Load())
	}
	if transports[1].closeN.Load() != 0 {
		t.Fatalf("cli2 Transport closed early: %d", transports[1].closeN.Load())
	}
	if err := cli2.Close(); err != nil {
		t.Fatalf("cli2.Close: %v", err)
	}
	if transports[1].closeN.Load() != 1 {
		t.Fatalf("cli2 Transport.Close count = %d, want 1", transports[1].closeN.Load())
	}
}

func TestCloseDrainsIdleSessions(t *testing.T) {
	t.Parallel()
	var dialed []*fake.ByteConn
	var dialMu sync.Mutex
	var calls atomic.Int64
	fn := freshLoopback(t, &calls, &dialed, &dialMu)

	cli, err := client.New(
		argos.WithServiceName(testService),
		argos.WithMaxConcurrentCalls(4),
		argos.WithMaxBufferedBytes(4*16*1024*1024),
		argos.WithMaxIdleSessions(4),
		argos.WithProtocol(fn),
		argos.WithTarget(testTarget),
	)
	if err != nil {
		t.Fatalf("client.New: %v", err)
	}

	cs, err := cli.Open(context.Background(), testMethod(t))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := cs.Send([]byte("x")); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if err := cs.HalfClose(); err != nil {
		t.Fatalf("HalfClose: %v", err)
	}
	var got []byte
	if err := cs.Recv(&got); err != nil {
		t.Fatalf("Recv: %v", err)
	}
	if err := cs.Recv(&got); !errors.Is(err, io.EOF) {
		t.Fatalf("Recv EOF: %v", err)
	}
	if err := cs.Close(); err != nil {
		t.Fatalf("CallStream.Close: %v", err)
	}

	dialMu.Lock()
	if len(dialed) != 1 {
		dialMu.Unlock()
		t.Fatalf("dialed = %d, want 1", len(dialed))
	}
	conn := dialed[0]
	dialMu.Unlock()

	if conn.CloseCount() != 0 {
		t.Fatalf("conn CloseCount before Client.Close = %d, want 0 (idle in pool)", conn.CloseCount())
	}
	if err := cli.Close(); err != nil {
		t.Fatalf("Client.Close: %v", err)
	}
	if conn.CloseCount() == 0 {
		t.Fatal("Client.Close did not drain idle session / close conn")
	}
}

func TestCallStreamLeakReportsPhaseLeak(t *testing.T) {
	if raceEnabled {
		t.Skip("CallStream leak GC observation is flaky under the race detector")
	}

	var leaked atomic.Bool
	var phase atomic.Uint32
	cli, err := client.New(
		argos.WithServiceName(testService),
		argos.WithMaxConcurrentCalls(4),
		argos.WithMaxBufferedBytes(4*16*1024*1024),
		argos.WithCallErrorObserver(func(info argos.CallInfo, _ error) {
			if info.Phase == argos.PhaseLeak {
				phase.Store(uint32(info.Phase))
				leaked.Store(true)
			}
		}),
		argos.WithProtocol(freshLoopback(t, nil, nil, nil)),
		argos.WithTarget(testTarget),
	)
	if err != nil {
		t.Fatalf("client.New: %v", err)
	}
	defer cli.Close()

	func() {
		cs, err := cli.Open(context.Background(), testMethod(t))
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		// Intentionally leak: drop without Close so AddCleanup can fire.
		_ = cs
	}()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		runtime.GC()
		runtime.GC()
		if leaked.Load() {
			if argos.Phase(phase.Load()) != argos.PhaseLeak {
				t.Fatalf("phase = %v, want PhaseLeak", phase.Load())
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("PhaseLeak not reported after forcing GC (AddCleanup)")
}

func TestCloseNoGoroutineLeak(t *testing.T) {
	t.Parallel()
	// Optional slack check: happy-path Open/Close should not permanently
	// grow the goroutine count beyond scheduler noise.
	runtime.GC()
	time.Sleep(20 * time.Millisecond)
	before := runtime.NumGoroutine()

	cli := newTestClient(t)
	m := testMethod(t)
	for i := 0; i < 5; i++ {
		cs, err := cli.Open(context.Background(), m)
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		if err := cs.Send([]byte("g")); err != nil {
			t.Fatalf("Send: %v", err)
		}
		if err := cs.HalfClose(); err != nil {
			t.Fatalf("HalfClose: %v", err)
		}
		var got []byte
		if err := cs.Recv(&got); err != nil {
			t.Fatalf("Recv: %v", err)
		}
		_ = cs.Recv(&got)
		if err := cs.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
	}
	if err := cli.Close(); err != nil {
		t.Fatalf("Client.Close: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	var after int
	for time.Now().Before(deadline) {
		runtime.GC()
		time.Sleep(30 * time.Millisecond)
		after = runtime.NumGoroutine()
		if after <= before+8 {
			return
		}
	}
	t.Fatalf("goroutines before=%d after=%d (slack 8)", before, after)
}

// A Client that is dropped instead of closed used to hold its session pool's
// reclaim goroutine and every socket it had open for the life of the process.
// Close is still the contract; this is the safety net for forgetting it.
func TestDroppedClientReleasesItsResources(t *testing.T) {
	t.Parallel()
	var reported atomic.Bool
	var info atomic.Value
	tr := &loopTransport{
		dial: func(ctx context.Context, endpoint string) (transport.Conn, error) {
			return nil, errors.New("not dialed")
		},
	}

	func() {
		cli, err := client.New(
			argos.WithServiceName(testService),
			argos.WithConnErrorObserver(func(ci argos.ConnInfo, _ error) {
				info.Store(ci)
				reported.Store(true)
			}),
			argos.WithProtocol(fixedLoopback(tr, fake.NewFraming(framing.Sequential))),
			argos.WithTarget(testTarget),
		)
		if err != nil {
			t.Fatalf("client.New: %v", err)
		}
		// Intentionally leak: drop without Close so AddCleanup can fire. This
		// only works because the pool's DialFunc does not point back at the
		// Client — otherwise the Client stays reachable from its own hook.
		_ = cli
	}()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		runtime.GC()
		runtime.GC()
		if reported.Load() {
			ci := info.Load().(argos.ConnInfo)
			if ci.Side != argos.SideClient || ci.Phase != argos.ConnPhaseClose {
				t.Errorf("ConnInfo = %+v, want SideClient and ConnPhaseClose", ci)
			}
			if ci.Endpoint != testTarget {
				t.Errorf("Endpoint = %q, want %q", ci.Endpoint, testTarget)
			}
			if n := tr.closeN.Load(); n != 1 {
				t.Errorf("Transport closed %d times, want 1", n)
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("dropped Client was never released after forcing GC (AddCleanup)")
}

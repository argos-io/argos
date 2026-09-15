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
	"github.com/argos-io/argos/framing"
	"github.com/argos-io/argos/internal/fake"
	"github.com/argos-io/argos/transport"
)

// freshLoopback returns a BindingFunc that creates a new Transport×Framing
// pair on every call (factory-once / isolation tests).
func freshLoopback(t *testing.T, factoryCalls *atomic.Int64, dialed *[]*fake.ByteConn, dialMu *sync.Mutex) argos.BindingFunc {
	t.Helper()
	return func() (argos.Binding, error) {
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
		return argos.Binding{
			Transport: tr,
			Framing:   f,
			Codec:     bytesCodec{},
		}, nil
	}
}

func TestBindingFuncOncePerClient(t *testing.T) {
	t.Parallel()
	var calls atomic.Int64
	fn := freshLoopback(t, &calls, nil, nil)
	cfg, err := argos.New(
		argos.WithMaxConcurrentCalls(4),
		argos.WithMaxBufferedBytes(4*16*1024*1024),
		argos.WithService(testService,
			argos.ServiceBinding(fn),
			argos.ServiceTarget(testTarget)),
	)
	if err != nil {
		t.Fatalf("argos.New: %v", err)
	}
	cli, err := client.New(cfg, testService)
	if err != nil {
		t.Fatalf("client.New: %v", err)
	}
	defer cli.Close()

	if got := calls.Load(); got != 1 {
		t.Fatalf("BindingFunc calls = %d, want 1", got)
	}
	// Open must not re-invoke the factory.
	cs, err := cli.Open(context.Background(), testMethod(t))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	_ = cs.Close()
	if got := calls.Load(); got != 1 {
		t.Fatalf("BindingFunc calls after Open = %d, want 1", got)
	}
}

func TestClientsFromSameFactoryIsolated(t *testing.T) {
	t.Parallel()
	var calls atomic.Int64
	var transports []*loopTransport
	var framings []*fake.Framing
	var mu sync.Mutex

	fn := func() (argos.Binding, error) {
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
		return argos.Binding{Transport: tr, Framing: f, Codec: bytesCodec{}}, nil
	}

	cfg, err := argos.New(
		argos.WithMaxConcurrentCalls(4),
		argos.WithMaxBufferedBytes(4*16*1024*1024),
		argos.WithService(testService,
			argos.ServiceBinding(fn),
			argos.ServiceTarget(testTarget)),
	)
	if err != nil {
		t.Fatalf("argos.New: %v", err)
	}

	cli1, err := client.New(cfg, testService)
	if err != nil {
		t.Fatalf("client.New #1: %v", err)
	}
	cli2, err := client.New(cfg, testService)
	if err != nil {
		t.Fatalf("client.New #2: %v", err)
	}
	defer cli1.Close()
	defer cli2.Close()

	if got := calls.Load(); got != 2 {
		t.Fatalf("BindingFunc calls = %d, want 2", got)
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

	cfg, err := argos.New(
		argos.WithMaxConcurrentCalls(4),
		argos.WithMaxBufferedBytes(4*16*1024*1024),
		argos.WithMaxIdleSessions(4),
		argos.WithService(testService,
			argos.ServiceBinding(fn),
			argos.ServiceTarget(testTarget)),
	)
	if err != nil {
		t.Fatalf("argos.New: %v", err)
	}
	cli, err := client.New(cfg, testService)
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
	cfg, err := argos.New(
		argos.WithMaxConcurrentCalls(4),
		argos.WithMaxBufferedBytes(4*16*1024*1024),
		argos.WithCallErrorObserver(func(info argos.CallInfo, _ error) {
			if info.Phase == argos.PhaseLeak {
				phase.Store(uint32(info.Phase))
				leaked.Store(true)
			}
		}),
		argos.WithService(testService,
			argos.ServiceBinding(freshLoopback(t, nil, nil, nil)),
			argos.ServiceTarget(testTarget)),
	)
	if err != nil {
		t.Fatalf("argos.New: %v", err)
	}
	cli, err := client.New(cfg, testService)
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

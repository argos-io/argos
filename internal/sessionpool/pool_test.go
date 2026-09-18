package sessionpool

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/framing"
	"github.com/argos-io/argos/internal/fake"
	"github.com/argos-io/argos/status"
	"github.com/argos-io/argos/transport"
)

func testMethod(t *testing.T) descriptor.Method {
	t.Helper()
	m, err := descriptor.NewMethod("echo.v1.Echo.Echo", descriptor.Unary)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func TestSequentialReuseKeepsConnUntilPoolClose(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	var conns []*fake.ByteConn
	var servers []*fake.ByteConn

	dial := func(ctx context.Context, endpoint string) (transport.Conn, error) {
		cli, srv := fake.BytePipe()
		mu.Lock()
		conns = append(conns, cli)
		servers = append(servers, srv)
		mu.Unlock()
		return cli, nil
	}

	f := fake.NewFraming(framing.Sequential)
	p := New(f, dial, Config{
		MaxSessionsPerEndpoint: 8,
		MaxIdleSessions:        8,
	})
	defer p.Close()

	ctx := context.Background()
	const n = 100
	for i := 0; i < n; i++ {
		sess, err := p.Acquire(ctx, "ep1")
		if err != nil {
			t.Fatalf("Acquire #%d: %v", i, err)
		}
		p.Release(sess)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(conns) != 1 {
		t.Fatalf("dial count = %d, want 1", len(conns))
	}
	if got := conns[0].CloseCount(); got != 0 {
		t.Fatalf("CloseCount before Pool.Close = %d, want 0", got)
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if got := conns[0].CloseCount(); got != 1 {
		t.Fatalf("CloseCount after Pool.Close = %d, want 1", got)
	}
	for _, s := range servers {
		_ = s.Close()
	}
}

func TestConcurrentSingleflightOneDial(t *testing.T) {
	t.Parallel()
	var dials atomic.Int64
	hold := make(chan struct{})

	f := fake.NewFraming(framing.Concurrent)
	f.Handshake = func(ctx context.Context, c transport.Conn) error {
		select {
		case <-hold:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	dial := func(ctx context.Context, endpoint string) (transport.Conn, error) {
		dials.Add(1)
		cli, lis := fake.HTTPLoopback()
		go func() {
			for {
				srv, err := lis.Accept(context.Background())
				if err != nil {
					return
				}
				_ = srv.Close()
			}
		}()
		return cli, nil
	}

	p := New(f, dial, Config{
		MaxSessionsPerEndpoint: 64,
		MaxIdleSessions:        8,
		HandshakeTimeout:       5 * time.Second,
	})
	defer p.Close()

	const n = 8
	var wg sync.WaitGroup
	wg.Add(n)
	sessions := make([]framing.ClientSession, n)
	errs := make([]error, n)
	started := make(chan struct{})
	var startOnce sync.Once

	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			startOnce.Do(func() { close(started) })
			sessions[i], errs[i] = p.Acquire(context.Background(), "ep")
		}()
	}
	<-started
	time.Sleep(50 * time.Millisecond) // let waiters pile on singleflight
	close(hold)
	wg.Wait()

	if got := dials.Load(); got != 1 {
		t.Fatalf("dial count = %d, want 1", got)
	}
	var first framing.ClientSession
	for i := 0; i < n; i++ {
		if errs[i] != nil {
			t.Fatalf("Acquire #%d: %v", i, errs[i])
		}
		if sessions[i] == nil {
			t.Fatalf("Acquire #%d: nil session", i)
		}
		if i == 0 {
			first = sessions[i]
		} else if sessions[i] != first {
			t.Fatalf("Acquire #%d got distinct session; Concurrent singleflight should share one", i)
		}
	}
	for i := 0; i < n; i++ {
		p.Release(sessions[i])
	}
}

func TestConcurrentCloseDuringDial(t *testing.T) {
	t.Parallel()
	hold := make(chan struct{})

	f := fake.NewFraming(framing.Concurrent)
	f.Handshake = func(ctx context.Context, c transport.Conn) error {
		select {
		case <-hold:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	var closed atomic.Bool
	dial := func(ctx context.Context, endpoint string) (transport.Conn, error) {
		cli, lis := fake.HTTPLoopback()
		go func() {
			for {
				srv, err := lis.Accept(context.Background())
				if err != nil {
					return
				}
				_ = srv.Close()
			}
		}()
		return cli, nil
	}

	p := New(f, dial, Config{
		MaxSessionsPerEndpoint: 64,
		MaxIdleSessions:        8,
		HandshakeTimeout:       5 * time.Second,
	})

	errCh := make(chan error, 1)
	go func() {
		_, err := p.Acquire(context.Background(), "ep")
		errCh <- err
	}()
	time.Sleep(50 * time.Millisecond) // dial in flight, blocked in Handshake
	p.Close()
	closed.Store(true)
	close(hold)

	err := <-errCh
	if err == nil {
		t.Fatal("Acquire after Pool.Close during dial returned nil error")
	}
	if !closed.Load() {
		t.Fatal("test sequencing: Close should have run before Acquire returns")
	}
	if !strings.Contains(err.Error(), "pool closed") {
		t.Fatalf("Acquire err = %v, want pool closed", err)
	}
}

func TestSequentialConcurrentAcquireDistinct(t *testing.T) {
	t.Parallel()
	var dials atomic.Int64
	hold := make(chan struct{})

	f := fake.NewFraming(framing.Sequential)
	f.Handshake = func(ctx context.Context, c transport.Conn) error {
		select {
		case <-hold:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	var servers []*fake.ByteConn
	var serversMu sync.Mutex
	t.Cleanup(func() {
		serversMu.Lock()
		defer serversMu.Unlock()
		for _, s := range servers {
			_ = s.Close()
		}
	})
	dial := func(ctx context.Context, endpoint string) (transport.Conn, error) {
		dials.Add(1)
		cli, srv := fake.BytePipe()
		serversMu.Lock()
		servers = append(servers, srv)
		serversMu.Unlock()
		return cli, nil
	}

	p := New(f, dial, Config{
		MaxSessionsPerEndpoint: 16,
		MaxIdleSessions:        16,
		HandshakeTimeout:       5 * time.Second,
	})
	defer p.Close()

	const n = 8
	var wg sync.WaitGroup
	wg.Add(n)
	sessions := make([]framing.ClientSession, n)
	started := make(chan struct{})
	var startOnce sync.Once

	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			startOnce.Do(func() { close(started) })
			var err error
			sessions[i], err = p.Acquire(context.Background(), "ep")
			if err != nil {
				t.Errorf("Acquire #%d: %v", i, err)
			}
		}()
	}
	<-started
	time.Sleep(50 * time.Millisecond)
	close(hold)
	wg.Wait()

	if got := dials.Load(); got != int64(n) {
		t.Fatalf("dial count = %d, want %d", got, n)
	}
	seen := make(map[framing.ClientSession]struct{})
	for i, s := range sessions {
		if s == nil {
			t.Fatalf("nil session #%d", i)
		}
		if _, ok := seen[s]; ok {
			t.Fatalf("duplicate session at #%d", i)
		}
		seen[s] = struct{}{}
		p.Release(s)
	}
}

func TestCapExhaustedNonBlocking(t *testing.T) {
	t.Parallel()
	var servers []*fake.ByteConn
	t.Cleanup(func() {
		for _, s := range servers {
			_ = s.Close()
		}
	})
	dial := func(ctx context.Context, endpoint string) (transport.Conn, error) {
		cli, srv := fake.BytePipe()
		servers = append(servers, srv)
		return cli, nil
	}
	f := fake.NewFraming(framing.Sequential)
	p := New(f, dial, Config{
		MaxSessionsPerEndpoint: 1,
		MaxIdleSessions:        1,
	})
	defer p.Close()

	ctx := context.Background()
	s1, err := p.Acquire(ctx, "ep")
	if err != nil {
		t.Fatal(err)
	}
	defer p.Release(s1)

	done := make(chan error, 1)
	go func() {
		_, err := p.Acquire(ctx, "ep")
		done <- err
	}()

	select {
	case err := <-done:
		if !errors.Is(err, status.ErrSessionsExhausted) {
			t.Fatalf("got %v, want ErrSessionsExhausted", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("Acquire blocked; want immediate ErrSessionsExhausted")
	}
}

func TestNotReusableOnReleaseClosedNotRelent(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	var conns []*fake.ByteConn

	dial := func(ctx context.Context, endpoint string) (transport.Conn, error) {
		cli, srv := fake.BytePipe()
		mu.Lock()
		conns = append(conns, cli)
		mu.Unlock()
		_ = srv
		return cli, nil
	}

	f := fake.NewFraming(framing.Sequential)
	p := New(f, dial, Config{
		MaxSessionsPerEndpoint: 8,
		MaxIdleSessions:        8,
	})
	defer p.Close()

	ctx := context.Background()
	s1, err := p.Acquire(ctx, "ep")
	if err != nil {
		t.Fatal(err)
	}
	fs, ok := s1.(*fake.ClientSession)
	if !ok {
		t.Fatalf("session type %T", s1)
	}
	fs.MarkBad()
	p.Release(s1)

	mu.Lock()
	if len(conns) != 1 {
		mu.Unlock()
		t.Fatalf("dial count = %d, want 1", len(conns))
	}
	if got := conns[0].CloseCount(); got != 1 {
		mu.Unlock()
		t.Fatalf("bad session CloseCount = %d, want 1", got)
	}
	mu.Unlock()

	s2, err := p.Acquire(ctx, "ep")
	if err != nil {
		t.Fatal(err)
	}
	if s2 == s1 {
		t.Fatal("re-lent non-reusable session")
	}
	p.Release(s2)

	mu.Lock()
	defer mu.Unlock()
	if len(conns) != 2 {
		t.Fatalf("dial count after re-acquire = %d, want 2", len(conns))
	}
}

func TestReusableRaceWithMarkBad(t *testing.T) {
	t.Parallel()
	dial := func(ctx context.Context, endpoint string) (transport.Conn, error) {
		cli, srv := fake.BytePipe()
		_ = srv
		return cli, nil
	}
	f := fake.NewFraming(framing.Sequential)
	p := New(f, dial, Config{
		MaxSessionsPerEndpoint: 64,
		MaxIdleSessions:        64,
	})
	defer p.Close()

	ctx := context.Background()
	s, err := p.Acquire(ctx, "ep")
	if err != nil {
		t.Fatal(err)
	}
	fs := s.(*fake.ClientSession)
	p.Release(s)

	var wg sync.WaitGroup
	const goroutines = 32
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				sess, err := p.Acquire(ctx, "ep")
				if err != nil {
					if errors.Is(err, status.ErrSessionsExhausted) {
						continue
					}
					return
				}
				if cs, ok := sess.(*fake.ClientSession); ok && j%7 == 0 {
					cs.MarkBad()
				}
				_ = sess.Reusable()
				p.Release(sess)
			}
		}()
	}
	// Concurrent MarkBad on the original session pointer if still live.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			fs.MarkBad()
			_ = fs.Reusable()
		}
	}()
	wg.Wait()
}

func TestIdleFullClosesOnReturn(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	var conns []*fake.ByteConn
	dial := func(ctx context.Context, endpoint string) (transport.Conn, error) {
		cli, srv := fake.BytePipe()
		mu.Lock()
		conns = append(conns, cli)
		mu.Unlock()
		_ = srv
		return cli, nil
	}
	f := fake.NewFraming(framing.Sequential)
	p := New(f, dial, Config{
		MaxSessionsPerEndpoint: 4,
		MaxIdleSessions:        0, // no idle keep
	})
	defer p.Close()

	ctx := context.Background()
	s, err := p.Acquire(ctx, "ep")
	if err != nil {
		t.Fatal(err)
	}
	p.Release(s)

	mu.Lock()
	defer mu.Unlock()
	if len(conns) != 1 {
		t.Fatalf("dials = %d", len(conns))
	}
	if got := conns[0].CloseCount(); got != 1 {
		t.Fatalf("CloseCount = %d, want 1 (idle full / MaxIdleSessions=0)", got)
	}
}

func TestMaxSessionLifetimeClosesWhenIdle(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	var conns []*fake.ByteConn
	dial := func(ctx context.Context, endpoint string) (transport.Conn, error) {
		cli, srv := fake.BytePipe()
		mu.Lock()
		conns = append(conns, cli)
		mu.Unlock()
		_ = srv
		return cli, nil
	}
	f := fake.NewFraming(framing.Sequential)
	p := New(f, dial, Config{
		MaxSessionsPerEndpoint: 4,
		MaxIdleSessions:        4,
		MaxSessionLifetime:     30 * time.Millisecond,
	})
	defer p.Close()

	ctx := context.Background()
	s, err := p.Acquire(ctx, "ep")
	if err != nil {
		t.Fatal(err)
	}
	p.Release(s)

	time.Sleep(60 * time.Millisecond)
	s2, err := p.Acquire(ctx, "ep")
	if err != nil {
		t.Fatal(err)
	}
	p.Release(s2)

	mu.Lock()
	defer mu.Unlock()
	if len(conns) < 2 {
		t.Fatalf("expected new dial after lifetime expiry, dials=%d", len(conns))
	}
	if got := conns[0].CloseCount(); got != 1 {
		t.Fatalf("expired session CloseCount = %d, want 1", got)
	}
}

func TestConcurrentKeepAliveUntilLastRelease(t *testing.T) {
	t.Parallel()
	var closes atomic.Int64
	dial := func(ctx context.Context, endpoint string) (transport.Conn, error) {
		cli, lis := fake.HTTPLoopback()
		go func() {
			for {
				srv, err := lis.Accept(context.Background())
				if err != nil {
					return
				}
				_ = srv.Close()
			}
		}()
		// Wrap to count Close via session — use HTTPClientConn.CloseCount after Session.Close.
		return &countingConn{Conn: cli, n: &closes}, nil
	}

	f := fake.NewFraming(framing.Concurrent)
	p := New(f, dial, Config{
		MaxSessionsPerEndpoint: 4,
		MaxIdleSessions:        4,
	})
	defer p.Close()

	ctx := context.Background()
	s1, err := p.Acquire(ctx, "ep")
	if err != nil {
		t.Fatal(err)
	}
	s2, err := p.Acquire(ctx, "ep")
	if err != nil {
		t.Fatal(err)
	}
	if s1 != s2 {
		t.Fatal("Concurrent should share session")
	}
	s1.(*fake.ClientSession).MarkBad()
	p.Release(s1)
	if closes.Load() != 0 {
		t.Fatalf("closed early with refcount>0: %d", closes.Load())
	}
	p.Release(s2)
	if closes.Load() != 1 {
		t.Fatalf("CloseCount after last Release = %d, want 1", closes.Load())
	}
}

func TestOpenCallBusyThenSucceeds(t *testing.T) {
	t.Parallel()
	var attempts atomic.Int64
	const busyFirst = 2

	var mu sync.Mutex
	var servers []*fake.ByteConn
	t.Cleanup(func() {
		mu.Lock()
		defer mu.Unlock()
		for _, s := range servers {
			_ = s.Close()
		}
	})
	dial := func(ctx context.Context, endpoint string) (transport.Conn, error) {
		cli, srv := fake.BytePipe()
		mu.Lock()
		servers = append(servers, srv)
		mu.Unlock()
		go drainByteConn(srv)
		return cli, nil
	}

	f := fake.NewFraming(framing.Sequential)
	f.OpenCallHook = func(callSeq int) error {
		if attempts.Add(1) <= busyFirst {
			return framing.ErrSessionBusy
		}
		return nil
	}

	p := New(f, dial, Config{
		MaxSessionsPerEndpoint: 8,
		MaxIdleSessions:        8,
	})
	defer p.Close()

	call, sess, err := p.OpenCall(context.Background(), "ep", testMethod(t), framing.CallSpec{})
	if err != nil {
		t.Fatalf("OpenCall: %v", err)
	}
	if errors.Is(err, framing.ErrSessionBusy) {
		t.Fatal("ErrSessionBusy leaked")
	}
	if call == nil || sess == nil {
		t.Fatal("nil call or session")
	}
	if got := attempts.Load(); got <= busyFirst {
		t.Fatalf("attempts=%d, want > %d", got, busyFirst)
	}
	_ = call.Close()
	p.Release(sess)
}

func TestOpenCallAlwaysBusyExhausted(t *testing.T) {
	t.Parallel()
	var servers []*fake.ByteConn
	t.Cleanup(func() {
		for _, s := range servers {
			_ = s.Close()
		}
	})
	dial := func(ctx context.Context, endpoint string) (transport.Conn, error) {
		cli, srv := fake.BytePipe()
		servers = append(servers, srv)
		return cli, nil
	}

	const max = 3
	f := fake.NewFraming(framing.Sequential)
	f.OpenCallHook = func(callSeq int) error {
		return framing.ErrSessionBusy
	}

	p := New(f, dial, Config{
		MaxSessionsPerEndpoint: max,
		MaxIdleSessions:        max,
	})
	defer p.Close()

	call, sess, err := p.OpenCall(context.Background(), "ep", testMethod(t), framing.CallSpec{})
	if call != nil || sess != nil {
		t.Fatalf("want nil call/sess on exhaust, got call=%v sess=%v", call, sess)
	}
	if errors.Is(err, framing.ErrSessionBusy) {
		t.Fatalf("ErrSessionBusy leaked: %v", err)
	}
	if !errors.Is(err, status.ErrSessionsExhausted) {
		t.Fatalf("got %v, want ErrSessionsExhausted", err)
	}
	if status.CodeOf(err) != status.ResourceExhausted {
		t.Fatalf("CodeOf = %v, want ResourceExhausted", status.CodeOf(err))
	}
}

func TestOpenCallConcurrentEmptyPoolSingleflight(t *testing.T) {
	t.Parallel()
	var dials atomic.Int64
	hold := make(chan struct{})
	stopErr := errors.New("opencall stop after dial")

	f := fake.NewFraming(framing.Concurrent)
	// Fail in hook before OpenStream/write so this asserts dial singleflight only.
	f.OpenCallHook = func(callSeq int) error { return stopErr }
	f.Handshake = func(ctx context.Context, c transport.Conn) error {
		select {
		case <-hold:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	dial := func(ctx context.Context, endpoint string) (transport.Conn, error) {
		dials.Add(1)
		cli, lis := fake.HTTPLoopback()
		go func() {
			for {
				srv, err := lis.Accept(context.Background())
				if err != nil {
					return
				}
				_ = srv.Close()
			}
		}()
		return cli, nil
	}

	p := New(f, dial, Config{
		MaxSessionsPerEndpoint: 64,
		MaxIdleSessions:        8,
		HandshakeTimeout:       5 * time.Second,
	})
	defer p.Close()

	const n = 8
	var wg sync.WaitGroup
	wg.Add(n)
	errs := make([]error, n)
	started := make(chan struct{})
	var startOnce sync.Once
	m := testMethod(t)

	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			startOnce.Do(func() { close(started) })
			_, _, errs[i] = p.OpenCall(context.Background(), "ep", m, framing.CallSpec{})
		}()
	}
	<-started
	time.Sleep(50 * time.Millisecond)
	close(hold)
	wg.Wait()

	if got := dials.Load(); got != 1 {
		t.Fatalf("dial count = %d, want 1", got)
	}
	for i := 0; i < n; i++ {
		if !errors.Is(errs[i], stopErr) {
			t.Fatalf("OpenCall #%d: %v, want stopErr", i, errs[i])
		}
		if errors.Is(errs[i], framing.ErrSessionBusy) {
			t.Fatal("ErrSessionBusy leaked")
		}
	}
}

func TestOpenCallSequentialEmptyPoolOwnDial(t *testing.T) {
	t.Parallel()
	var dials atomic.Int64
	hold := make(chan struct{})
	stopErr := errors.New("opencall stop after dial")

	f := fake.NewFraming(framing.Sequential)
	f.OpenCallHook = func(callSeq int) error { return stopErr }
	f.Handshake = func(ctx context.Context, c transport.Conn) error {
		select {
		case <-hold:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	var servers []*fake.ByteConn
	var serversMu sync.Mutex
	t.Cleanup(func() {
		serversMu.Lock()
		defer serversMu.Unlock()
		for _, s := range servers {
			_ = s.Close()
		}
	})
	dial := func(ctx context.Context, endpoint string) (transport.Conn, error) {
		dials.Add(1)
		cli, srv := fake.BytePipe()
		serversMu.Lock()
		servers = append(servers, srv)
		serversMu.Unlock()
		return cli, nil
	}

	p := New(f, dial, Config{
		MaxSessionsPerEndpoint: 16,
		MaxIdleSessions:        16,
		HandshakeTimeout:       5 * time.Second,
	})
	defer p.Close()

	const n = 8
	var wg sync.WaitGroup
	wg.Add(n)
	errs := make([]error, n)
	started := make(chan struct{})
	var startOnce sync.Once
	m := testMethod(t)

	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			startOnce.Do(func() { close(started) })
			_, _, errs[i] = p.OpenCall(context.Background(), "ep", m, framing.CallSpec{})
		}()
	}
	<-started
	time.Sleep(50 * time.Millisecond)
	close(hold)
	wg.Wait()

	if got := dials.Load(); got != int64(n) {
		t.Fatalf("dial count = %d, want %d", got, n)
	}
	for i := 0; i < n; i++ {
		if !errors.Is(errs[i], stopErr) {
			t.Fatalf("OpenCall #%d: %v, want stopErr", i, errs[i])
		}
		if errors.Is(errs[i], framing.ErrSessionBusy) {
			t.Fatal("ErrSessionBusy leaked")
		}
	}
}

func drainByteConn(c *fake.ByteConn) {
	buf := make([]byte, 4096)
	for {
		_, err := c.Read(buf)
		if err != nil {
			return
		}
	}
}

// countingConn counts Close calls on an underlying Conn.
type countingConn struct {
	transport.Conn
	n *atomic.Int64
}

func (c *countingConn) Close() error {
	c.n.Add(1)
	return c.Conn.Close()
}

func (c *countingConn) OpenStream(ctx context.Context, p transport.RequestPreface) (transport.Carrier, error) {
	sc, ok := c.Conn.(transport.StreamConn)
	if !ok {
		return nil, errors.New("not StreamConn")
	}
	return sc.OpenStream(ctx, p)
}

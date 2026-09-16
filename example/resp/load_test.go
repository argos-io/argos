package resp_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/argos-io/argos"
	"github.com/argos-io/argos/client"
	"github.com/argos-io/argos/example/resp"
	"github.com/argos-io/argos/server"
	"github.com/argos-io/argos/status"
	"github.com/argos-io/argos/transport/tcp"

	_ "github.com/argos-io/argos/resolver/ip"
)

type loadEnv struct {
	h            *harness
	srv          *server.Server
	addr         string
	admitRejects *atomic.Int64
	serveDone    chan error
}

func startLoadRESP(t *testing.T, extra ...argos.Option) *loadEnv {
	t.Helper()

	store := resp.NewStore()
	var clientFr *resp.Framing
	var dials atomic.Int64
	var admitRejects atomic.Int64
	var addrTr hasAddr
	bound := make(chan struct{})

	serverFn := func() (argos.Binding, error) {
		fr := resp.New()
		tr := tcp.New()
		addrTr = tr.(hasAddr)
		select {
		case <-bound:
		default:
			close(bound)
		}
		return argos.Binding{Transport: tr, Framing: fr, Codec: resp.NewBytesCodec()}, nil
	}
	clientFn := func() (argos.Binding, error) {
		clientFr = resp.New()
		tr := tcp.New()
		return argos.Binding{
			Transport: &dialCounter{Transport: tr, dials: &dials},
			Framing:   clientFr,
			Codec:     resp.NewBytesCodec(),
		}, nil
	}

	opts := append([]argos.Option{
		argos.WithMaxConcurrentCalls(64),
		argos.WithMaxBufferedBytes(64 * 16 * 1024 * 1024),
		argos.WithHandshakeTimeout(5 * time.Second),
		argos.WithListenAddress(testListenAddr),
		argos.WithConnErrorObserver(func(info argos.ConnInfo, err error) {
			if info.Phase == argos.ConnPhaseAdmit && status.CodeOf(err) == status.ResourceExhausted {
				admitRejects.Add(1)
			}
		}),
	}, extra...)

	cfg, err := argos.New(opts...)
	if err != nil {
		t.Fatal(err)
	}
	srv := server.New(cfg)
	if err := srv.AddBinding(serverFn); err != nil {
		t.Fatal(err)
	}
	if err := resp.Register(srv, store); err != nil {
		t.Fatal(err)
	}
	serveDone := make(chan error, 1)
	go func() { serveDone <- srv.Run(context.Background()) }()
	select {
	case <-bound:
	case <-time.After(3 * time.Second):
		t.Fatal("server BindingFunc not invoked")
	}
	addr := waitAddr(t, addrTr)
	t.Cleanup(func() { _ = srv.Close() })

	cliCfg, err := argos.New(append(opts,
		argos.WithService(svcName,
			argos.ServiceBinding(clientFn),
			argos.ServiceTarget("ip://"+addr),
		),
	)...)
	if err != nil {
		t.Fatal(err)
	}
	cli, err := client.New(cliCfg, svcName)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cli.Close() })

	return &loadEnv{
		h:            &harness{cli: cli, fr: clientFr, store: store, dials: &dials},
		srv:          srv,
		addr:         addr,
		admitRejects: &admitRejects,
		serveDone:    serveDone,
	}
}

func onePING(t *testing.T, h *harness) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	st, err := h.cli.Open(ctx, resp.MethodPING)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.Send(resp.EncodeArgs()); err != nil {
		t.Fatal(err)
	}
	if err := st.HalfClose(); err != nil {
		t.Fatalf("HalfClose: %v", err)
	}
	var out []byte
	if err := st.Recv(&out); err != nil {
		t.Fatal(err)
	}
}

// assertServeRunning fails when the server's Run already returned: a
// peer-visible read failure proves nothing about the connection lifecycle
// under test if the server died for an unrelated reason.
func assertServeRunning(t *testing.T, env *loadEnv) {
	t.Helper()
	select {
	case err := <-env.serveDone:
		t.Fatalf("Serve returned early: %v", err)
	default:
	}
}

// holdWave opens n concurrent PINGs and holds them mid-flight (after
// HalfClose, before Recv) so Sequential cannot reuse within the wave.
// It then finishes all calls. Returns elapsed wall time for the hold phase.
func holdWave(t *testing.T, h *harness, n int) time.Duration {
	t.Helper()
	type held struct {
		st     *client.CallStream
		cancel context.CancelFunc
	}
	streams := make([]*held, n)
	var fails atomic.Int64
	var wg sync.WaitGroup
	start := time.Now()
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			st, err := h.cli.Open(ctx, resp.MethodPING)
			if err != nil {
				cancel()
				fails.Add(1)
				return
			}
			if err := st.Send(resp.EncodeArgs()); err != nil {
				_ = st.Close()
				cancel()
				fails.Add(1)
				return
			}
			if err := st.HalfClose(); err != nil {
				_ = st.Close()
				cancel()
				fails.Add(1)
				return
			}
			streams[i] = &held{st: st, cancel: cancel}
		}()
	}
	wg.Wait()
	if f := fails.Load(); f != 0 {
		t.Fatalf("holdWave open failures: %d / %d", f, n)
	}
	for i, s := range streams {
		if s == nil {
			t.Fatalf("holdWave slot %d nil", i)
		}
	}
	var wg2 sync.WaitGroup
	wg2.Add(n)
	for _, s := range streams {
		s := s
		go func() {
			defer wg2.Done()
			defer s.cancel()
			defer s.st.Close()
			var out []byte
			if err := s.st.Recv(&out); err != nil {
				fails.Add(1)
			}
		}()
	}
	wg2.Wait()
	elapsed := time.Since(start)
	if f := fails.Load(); f != 0 {
		t.Fatalf("holdWave finish failures: %d / %d", f, n)
	}
	return elapsed
}

// TestBurstIdleBurstMaxIdleSessions measures wave-2 HELLO cost under
// MaxIdleSessions ∈ {0,8,64} with MaxSessionsPerEndpoint=64 (default scale).
// Waves hold all calls mid-flight so Sequential cannot collapse concurrency
// via intra-wave reuse (otherwise idle=64 never retains a full set).
func TestBurstIdleBurstMaxIdleSessions(t *testing.T) {
	const (
		maxSess = 64
		burst   = 64
		idleFor = 300 * time.Millisecond
	)

	type row struct {
		maxIdle   int
		wave1Dial int64
		wave1Helo int64
		deltaDial int64
		deltaHelo int64
		wave1     time.Duration
		wave2     time.Duration
	}
	var rows []row

	for _, maxIdle := range []int{0, 8, 64} {
		maxIdle := maxIdle
		t.Run(fmt.Sprintf("idle=%d", maxIdle), func(t *testing.T) {
			env := startLoadRESP(t,
				argos.WithMaxIdleSessions(maxIdle),
				argos.WithMaxSessionsPerEndpoint(maxSess),
				argos.WithMaxConcurrentCalls(maxSess),
				argos.WithSessionIdleTimeout(0),
				argos.WithMaxSessionLifetime(0),
				argos.WithMaxInboundConns(256),
				argos.WithMaxInboundConnIdle(2*time.Minute),
			)
			h := env.h

			wave1 := holdWave(t, h, burst)
			d1, he1 := h.dials.Load(), h.fr.ClientHellos()
			if d1 < int64(burst) || he1 < int64(burst) {
				t.Fatalf("wave1 under-opened: dials=%d HELLO=%d want >=%d", d1, he1, burst)
			}

			time.Sleep(idleFor)

			wave2 := holdWave(t, h, burst)
			d2, he2 := h.dials.Load(), h.fr.ClientHellos()
			dd, dh := d2-d1, he2-he1

			rows = append(rows, row{
				maxIdle:   maxIdle,
				wave1Dial: d1, wave1Helo: he1,
				deltaDial: dd, deltaHelo: dh,
				wave1: wave1, wave2: wave2,
			})

			switch maxIdle {
			case 0:
				if dh < int64(burst*3/4) {
					t.Fatalf("idle=0: want ΔHELLO roughly ~burst, got %d", dh)
				}
			case 8:
				// Retain 8, close ~56 → wave2 pays most HELLO cost again.
				if dh < 40 {
					t.Fatalf("idle=8: want large wave2 HELLO cost (close ~56), ΔHELLO=%d", dh)
				}
				if dh > int64(burst-4) {
					t.Fatalf("idle=8: expected reuse of ~8 idle, ΔHELLO=%d", dh)
				}
			case 64:
				if dh > 5 {
					t.Fatalf("idle=64: want near-full reuse, ΔHELLO=%d", dh)
				}
			}
			t.Logf("idle=%d wave1=%s dials=%d HELLO=%d | wave2=%s Δdials=%d ΔHELLO=%d",
				maxIdle, wave1.Round(time.Millisecond), d1, he1,
				wave2.Round(time.Millisecond), dd, dh)
		})
	}

	t.Run("idle=8_small_followup", func(t *testing.T) {
		env := startLoadRESP(t,
			argos.WithMaxIdleSessions(8),
			argos.WithMaxSessionsPerEndpoint(maxSess),
			argos.WithMaxConcurrentCalls(maxSess),
			argos.WithSessionIdleTimeout(0),
			argos.WithMaxSessionLifetime(0),
			argos.WithMaxInboundConns(256),
			argos.WithMaxInboundConnIdle(2*time.Minute),
		)
		h := env.h
		_ = holdWave(t, h, burst)
		d1, he1 := h.dials.Load(), h.fr.ClientHellos()
		time.Sleep(idleFor)
		_ = holdWave(t, h, 8)
		dd, dh := h.dials.Load()-d1, h.fr.ClientHellos()-he1
		if dh > 1 {
			t.Fatalf("idle=8 after large burst: small follow-up of 8 should reuse, ΔHELLO=%d", dh)
		}
		t.Logf("idle=8 small follow-up Δdials=%d ΔHELLO=%d", dd, dh)
	})

	var b strings.Builder
	b.WriteString("MaxIdleSessions burst→idle→burst (maxSess=64, burst=64, held mid-flight):\n")
	for _, r := range rows {
		fmt.Fprintf(&b, "  idle=%-2d  wave1=%s dials=%d HELLO=%d  wave2 Δdials=%d ΔHELLO=%d (%s)\n",
			r.maxIdle, r.wave1.Round(time.Millisecond), r.wave1Dial, r.wave1Helo,
			r.deltaDial, r.deltaHelo, r.wave2.Round(time.Millisecond))
	}
	t.Log("\n" + b.String())
}

// TestSessionIdleTimeoutReclaim verifies idle sessions are closed after
// SessionIdleTimeout and the next call pays a new HELLO.
func TestSessionIdleTimeoutReclaim(t *testing.T) {
	const idleTO = 200 * time.Millisecond
	env := startLoadRESP(t,
		argos.WithMaxIdleSessions(4),
		argos.WithMaxSessionsPerEndpoint(8),
		argos.WithSessionIdleTimeout(idleTO),
		argos.WithMaxSessionLifetime(0),
	)
	h := env.h

	onePING(t, h)
	d1, he1 := h.dials.Load(), h.fr.ClientHellos()
	if he1 < 1 {
		t.Fatal("expected at least one HELLO")
	}

	// Reclaim loop ticks every 1s; wait past idleTO + two ticks.
	time.Sleep(idleTO + 2500*time.Millisecond)

	onePING(t, h)
	dd, dh := h.dials.Load()-d1, h.fr.ClientHellos()-he1
	if dh < 1 || dd < 1 {
		t.Fatalf("after SessionIdleTimeout reclaim want new dial+HELLO, Δdials=%d ΔHELLO=%d", dd, dh)
	}
	t.Logf("SessionIdleTimeout=%s reclaim: Δdials=%d ΔHELLO=%d", idleTO, dd, dh)
}

// TestMaxSessionLifetimeNonReusable marks expired sessions non-reusable without
// killing an in-flight call; the next Open dials again.
func TestMaxSessionLifetimeNonReusable(t *testing.T) {
	const life = 150 * time.Millisecond
	env := startLoadRESP(t,
		argos.WithMaxIdleSessions(4),
		argos.WithMaxSessionsPerEndpoint(8),
		argos.WithSessionIdleTimeout(0),
		argos.WithMaxSessionLifetime(life),
	)
	h := env.h

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	st, err := h.cli.Open(ctx, resp.MethodPING)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.Send(resp.EncodeArgs()); err != nil {
		t.Fatal(err)
	}
	if err := st.HalfClose(); err != nil {
		t.Fatalf("HalfClose: %v", err)
	}

	// Lifetime elapses while the call is still in flight.
	time.Sleep(life + 50*time.Millisecond)

	var out []byte
	if err := st.Recv(&out); err != nil {
		t.Fatalf("in-flight call must survive MaxSessionLifetime: %v", err)
	}
	_ = st.Close()
	d1, he1 := h.dials.Load(), h.fr.ClientHellos()

	onePING(t, h)
	dd, dh := h.dials.Load()-d1, h.fr.ClientHellos()-he1
	if dh < 1 || dd < 1 {
		t.Fatalf("after lifetime expiry want new dial+HELLO, Δdials=%d ΔHELLO=%d", dd, dh)
	}
	t.Logf("MaxSessionLifetime=%s: in-flight OK; next Open Δdials=%d ΔHELLO=%d", life, dd, dh)
}

// TestMaxInboundConnsRejectsBeyondLimit opens idle tcp sessions up to the
// limit, asserts further accepts are rejected via ConnPhaseAdmit, and that
// Serve continues (a free slot after close is usable again).
func TestMaxInboundConnsRejectsBeyondLimit(t *testing.T) {
	const limit = 2
	env := startLoadRESP(t,
		argos.WithMaxInboundConns(limit),
		argos.WithMaxInboundConnIdle(30*time.Second),
		argos.WithMaxInboundConnAge(30*time.Minute),
		argos.WithMaxIdleSessions(0),
		argos.WithMaxSessionsPerEndpoint(8),
	)

	// Hold `limit` inbound connections open (handshake done, idle on AcceptCall).
	holders := make([]net.Conn, 0, limit)
	t.Cleanup(func() {
		for _, c := range holders {
			_ = c.Close()
		}
	})

	// Close the harness client so its pooled sessions do not consume slots.
	_ = env.h.cli.Close()
	time.Sleep(50 * time.Millisecond)

	listenAddr := env.addr
	for i := 0; i < limit; i++ {
		c, err := net.DialTimeout("tcp", listenAddr, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		holders = append(holders, c)
		if err := respHello(c); err != nil {
			t.Fatalf("holder %d HELLO: %v", i, err)
		}
	}

	// Extra dial must be admitted then closed (ResourceExhausted notify).
	before := env.admitRejects.Load()
	extra, err := net.DialTimeout("tcp", listenAddr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_ = extra.SetDeadline(time.Now().Add(2 * time.Second))
	_, _ = extra.Write([]byte("*2\r\n$5\r\nHELLO\r\n$1\r\n2\r\n"))
	buf := make([]byte, 64)
	n, rerr := extra.Read(buf)
	_ = extra.Close()
	deadline := time.Now().Add(2 * time.Second)
	for env.admitRejects.Load() == before && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if env.admitRejects.Load() <= before {
		t.Fatalf("want ConnPhaseAdmit ResourceExhausted after limit=%d; rejects=%d read_n=%d read_err=%v",
			limit, env.admitRejects.Load(), n, rerr)
	}

	// Serve must still be running.
	select {
	case err := <-env.serveDone:
		t.Fatalf("Serve returned early: %v", err)
	default:
	}

	// Free one holder; a new HELLO must succeed (Serve continues).
	_ = holders[0].Close()
	holders = holders[1:]
	time.Sleep(50 * time.Millisecond)
	c, err := net.DialTimeout("tcp", listenAddr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	holders = append(holders, c)
	if err := respHello(c); err != nil {
		t.Fatalf("post-reject HELLO should succeed: %v", err)
	}
	t.Logf("MaxInboundConns=%d: reject counted=%d; Serve continued; slot reuse OK",
		limit, env.admitRejects.Load()-before)
}

// TestMaxInboundConnIdleCloses closes an idle inbound tcp session after
// MaxInboundConnIdle.
func TestMaxInboundConnIdleCloses(t *testing.T) {
	const idle = 120 * time.Millisecond
	env := startLoadRESP(t,
		argos.WithMaxInboundConns(8),
		argos.WithMaxInboundConnIdle(idle),
		argos.WithMaxInboundConnAge(30*time.Minute),
		argos.WithMaxIdleSessions(0),
		argos.WithMaxSessionsPerEndpoint(4),
	)
	_ = env.h.cli.Close()
	listenAddr := env.addr

	c, err := net.DialTimeout("tcp", listenAddr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := respHello(c); err != nil {
		t.Fatal(err)
	}

	_ = c.SetReadDeadline(time.Now().Add(idle + 2*time.Second))
	buf := make([]byte, 8)
	_, err = c.Read(buf)
	if err == nil {
		t.Fatal("expected idle inbound close to end the connection")
	}
	// Any error at all would satisfy "the read failed" — including our own dead
	// deadline, a reset, or a server that died. Only a clean EOF (FIN) proves
	// the server retired the idle connection.
	if !errors.Is(err, io.EOF) {
		t.Fatalf("idle inbound read = %v (%T), want io.EOF from a clean server close", err, err)
	}
	assertServeRunning(t, env)
	t.Logf("MaxInboundConnIdle=%s closed idle inbound (read err=%v)", idle, err)
}

// TestMaxInboundConnAgeDrains stops accepting new calls after age and drains.
func TestMaxInboundConnAgeDrains(t *testing.T) {
	const age = 200 * time.Millisecond
	env := startLoadRESP(t,
		argos.WithMaxInboundConns(8),
		argos.WithMaxInboundConnIdle(30*time.Second),
		argos.WithMaxInboundConnAge(age),
		argos.WithMaxIdleSessions(0),
		argos.WithMaxSessionsPerEndpoint(4),
	)
	_ = env.h.cli.Close()
	listenAddr := env.addr

	c, err := net.DialTimeout("tcp", listenAddr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := respHello(c); err != nil {
		t.Fatal(err)
	}

	// Wait past age; AcceptCall loop should exit (drain). Peer eventually sees close.
	time.Sleep(age + 300*time.Millisecond)
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 8)
	_, err = c.Read(buf)
	if err == nil {
		t.Fatal("expected MaxInboundConnAge to end accepting and close/drain the conn")
	}
	// Draining must present as a clean close (EOF/FIN), not as a read that
	// merely failed — a dead deadline, a reset, or a dead server would all
	// produce a non-nil error too.
	if !errors.Is(err, io.EOF) {
		t.Fatalf("aged inbound read = %v (%T), want io.EOF from a clean drain", err, err)
	}
	assertServeRunning(t, env)
	t.Logf("MaxInboundConnAge=%s drained inbound (read err=%v)", age, err)

	// New connection after age of a prior conn must still be accepted (Serve continues).
	c2, err := net.DialTimeout("tcp", listenAddr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	if err := respHello(c2); err != nil {
		t.Fatalf("Serve should accept new conn after aged peer drained: %v", err)
	}
}

func respHello(c net.Conn) error {
	_ = c.SetDeadline(time.Now().Add(3 * time.Second))
	defer func() { _ = c.SetDeadline(time.Time{}) }()
	if _, err := c.Write([]byte("*2\r\n$5\r\nHELLO\r\n$1\r\n2\r\n")); err != nil {
		return err
	}
	// Expect a RESP map/array reply starting with '%' or '*'.
	buf := make([]byte, 256)
	n, err := c.Read(buf)
	if err != nil {
		return err
	}
	if n == 0 {
		return io.EOF
	}
	switch buf[0] {
	case '%', '*', '+', '$':
		return nil
	case '-':
		return fmt.Errorf("HELLO error: %q", buf[:n])
	default:
		return fmt.Errorf("unexpected HELLO reply: %q", buf[:n])
	}
}

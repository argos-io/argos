package resp_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/argos-io/argos"
	"github.com/argos-io/argos/client"
	"github.com/argos-io/argos/example/resp"
	"github.com/argos-io/argos/server"
	"github.com/argos-io/argos/transport/tcp"

	_ "github.com/argos-io/argos/resolver/ip"
)

// loadWave runs n concurrent PING calls and returns elapsed duration.
func loadWave(t *testing.T, h *harness, n int) time.Duration {
	t.Helper()
	var wg sync.WaitGroup
	var fails atomic.Int64
	start := time.Now()
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			st, err := h.cli.Open(ctx, resp.MethodPING)
			if err != nil {
				fails.Add(1)
				return
			}
			defer st.Close()
			if err := st.Send(resp.EncodeArgs()); err != nil {
				fails.Add(1)
				return
			}
			_ = st.HalfClose()
			var out []byte
			if err := st.Recv(&out); err != nil {
				fails.Add(1)
				return
			}
		}()
	}
	wg.Wait()
	elapsed := time.Since(start)
	if f := fails.Load(); f != 0 {
		t.Fatalf("wave failures: %d / %d", f, n)
	}
	return elapsed
}

// TestBurstIdleBurstLoadHints exercises Sequential pool churn for task 7.5:
// burst → idle → burst under MaxIdleSessions / MaxSessionsPerEndpoint.
// It logs measured dials/HELLOs; it does not rewrite §6.1 defaults.
func TestBurstIdleBurstLoadHints(t *testing.T) {
	const (
		maxIdle = 4
		maxSess = 16
		burst   = 16
		idleFor = 200 * time.Millisecond
	)

	store := resp.NewStore()
	var clientFr *resp.Framing
	var dials atomic.Int64
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

	opts := []argos.Option{
		argos.WithMaxConcurrentCalls(maxSess),
		argos.WithMaxBufferedBytes(maxSess * 16 * 1024 * 1024),
		argos.WithMaxIdleSessions(maxIdle),
		argos.WithMaxSessionsPerEndpoint(maxSess),
		argos.WithHandshakeTimeout(5 * time.Second),
		argos.WithListenAddress(testListenAddr),
	}
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
	go func() { _ = srv.Run(context.Background()) }()
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

	h := &harness{cli: cli, fr: clientFr, store: store, dials: &dials}

	wave1 := loadWave(t, h, burst)
	dialsAfter1 := dials.Load()
	hellosAfter1 := clientFr.ClientHellos()

	time.Sleep(idleFor)

	wave2 := loadWave(t, h, burst)
	dialsAfter2 := dials.Load()
	hellosAfter2 := clientFr.ClientHellos()

	// After wave1 with burst==maxSess and maxIdle < burst, most sessions are
	// closed on return; wave2 must dial again (HELLO count rises).
	newDials := dialsAfter2 - dialsAfter1
	newHellos := hellosAfter2 - hellosAfter1
	if newDials < 1 || newHellos < 1 {
		t.Fatalf("wave2 reused too aggressively: Δdials=%d ΔHELLO=%d (want both >= 1); "+
			"maxIdle=%d maxSess=%d", newDials, newHellos, maxIdle, maxSess)
	}
	if dialsAfter1 < int64(maxIdle) {
		// First burst should open up to maxSess sessions.
		t.Logf("note: wave1 dials=%d < maxIdle=%d (scheduler may serialize)", dialsAfter1, maxIdle)
	}

	summary := fmt.Sprintf(`# Connection pool load hints (task 7.5)

Measured on example/resp Sequential × tcp with PING bursts.
§6.1 ⚠️ defaults were **not** rewritten — peaks below are observational only.

## Config under test

| Option | Value |
|--------|------:|
| MaxSessionsPerEndpoint | %d |
| MaxIdleSessions | %d |
| Burst concurrency | %d |
| Idle gap | %s |

## Measured

| Phase | Elapsed | Cumulative dials | Cumulative HELLO |
|-------|--------:|-----------------:|-----------------:|
| Wave 1 (burst) | %s | %d | %d |
| Idle | %s | (unchanged) | (unchanged) |
| Wave 2 (burst) | %s | %d | %d |

| Delta wave2 − wave1 | Value |
|---------------------|------:|
| New dials | %d |
| New HELLOs | %d |

## Reading

With MaxIdleSessions=%d and a burst of %d, the pool keeps at most %d idle
sessions after wave 1; the rest are closed. Wave 2 therefore pays handshake
cost again (ΔHELLO=%d). This matches the §6.1 warning that MaxIdleSessions(8)
paired with MaxSessionsPerEndpoint(64) closes most sockets between bursts.

No recommendation strong enough to clear the ⚠️ markers on
MaxIdleSessions / SessionIdleTimeout / MaxSessionLifetime /
MaxInboundConns / MaxInboundConnIdle / MaxInboundConnAge was produced by
this pragmatic run — leave §6.1 provisional.
`,
		maxSess, maxIdle, burst, idleFor,
		wave1.Round(time.Millisecond), dialsAfter1, hellosAfter1,
		idleFor,
		wave2.Round(time.Millisecond), dialsAfter2, hellosAfter2,
		newDials, newHellos,
		maxIdle, burst, maxIdle, newHellos,
	)

	t.Log("\n" + summary)
}

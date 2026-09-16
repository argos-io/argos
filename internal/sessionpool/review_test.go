package sessionpool_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/argos-io/argos/framing"
	"github.com/argos-io/argos/internal/fake"
	"github.com/argos-io/argos/internal/sessionpool"
	"github.com/argos-io/argos/transport"
)

// concurrentDial returns a dial function producing StreamConns, which is what
// fake's Concurrent framing requires.
func concurrentDial(dials *atomic.Int64) func(context.Context, string) (transport.Conn, error) {
	return func(ctx context.Context, endpoint string) (transport.Conn, error) {
		if dials != nil {
			dials.Add(1)
		}
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
}

// Concurrent cold start used to release the dial reservation and deregister the
// singleflight before registering the session, leaving a window where the
// endpoint looked untouched. A second Acquire dialled in that window, so the
// bucket ended up over MaxSessionsPerEndpoint.
func TestConcurrentColdStartHonoursSessionCap(t *testing.T) {
	t.Parallel()
	var dials atomic.Int64
	dial := concurrentDial(&dials)
	f := fake.NewFraming(framing.Concurrent)

	// Many rounds: the window is narrow, so one attempt proves little.
	for round := 0; round < 200; round++ {
		p := sessionpool.New(f, dial, sessionpool.Config{
			MaxSessionsPerEndpoint: 1,
			MaxIdleSessions:        1,
			SessionIdleTimeout:     time.Minute,
			MaxSessionLifetime:     time.Minute,
			HandshakeTimeout:       5 * time.Second,
		})

		const racers = 8
		var wg sync.WaitGroup
		got := make([]framing.ClientSession, racers)
		for i := 0; i < racers; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				sess, err := p.Acquire(ctx, "endpoint")
				if err != nil {
					return // ErrSessionsExhausted is a legal outcome at cap 1
				}
				got[i] = sess
			}(i)
		}
		wg.Wait()

		distinct := make(map[framing.ClientSession]struct{})
		for _, s := range got {
			if s != nil {
				distinct[s] = struct{}{}
				p.Release(s)
			}
		}
		if len(distinct) > 1 {
			_ = p.Close()
			t.Fatalf("round %d: %d distinct sessions for MaxSessionsPerEndpoint=1", round, len(distinct))
		}
		_ = p.Close()
	}
}

// A waiter that joined a failed cold-start flight used to loop with no bound
// and no backoff, turning one Acquire into a dial storm for as long as the
// caller's ctx allowed. It now reports the dial failure like the flight leader
// and the Sequential path do.
func TestConcurrentDialFailureDoesNotRetryUnbounded(t *testing.T) {
	t.Parallel()
	dialErr := errors.New("connection refused")
	var dials atomic.Int64
	release := make(chan struct{})
	dial := func(ctx context.Context, endpoint string) (transport.Conn, error) {
		if dials.Add(1) == 1 {
			// Hold the first dial so the others pile onto the flight.
			<-release
		}
		return nil, dialErr
	}

	p := sessionpool.New(fake.NewFraming(framing.Concurrent), dial, sessionpool.Config{
		MaxSessionsPerEndpoint: 4,
		MaxIdleSessions:        4,
		SessionIdleTimeout:     time.Minute,
		MaxSessionLifetime:     time.Minute,
		HandshakeTimeout:       5 * time.Second,
	})
	defer func() { _ = p.Close() }()

	const waiters = 4
	errs := make(chan error, waiters)
	for i := 0; i < waiters; i++ {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_, err := p.Acquire(ctx, "endpoint")
			errs <- err
		}()
	}
	// Let the waiters register on the flight before it fails.
	time.Sleep(100 * time.Millisecond)
	close(release)

	for i := 0; i < waiters; i++ {
		select {
		case err := <-errs:
			if err == nil {
				t.Fatal("Acquire succeeded against a refused endpoint")
			}
			if errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("waiter %d burned its whole ctx retrying: %v", i, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("waiter %d never returned", i)
		}
	}
	// All four registered before the flight failed: one leader dialled, the
	// other three joined it. Reporting the shared failure costs exactly that
	// one dial; retrying made each waiter become a leader and dial again.
	if n := dials.Load(); n != 1 {
		t.Fatalf("dials = %d, want 1: waiters redialled instead of reporting the flight's failure", n)
	}
}

// Release must pair with a successful Acquire. A second Release on an
// already-idle entry used to append it to the idle queue again, so reclaim and
// lend saw one session under two identities.
func TestDoubleReleaseDoesNotDuplicateIdleEntry(t *testing.T) {
	t.Parallel()
	p := sessionpool.New(fake.NewFraming(framing.Concurrent), concurrentDial(nil), sessionpool.Config{
		MaxSessionsPerEndpoint: 1,
		MaxIdleSessions:        1,
		SessionIdleTimeout:     time.Minute,
		MaxSessionLifetime:     time.Minute,
		HandshakeTimeout:       5 * time.Second,
	})
	defer func() { _ = p.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	sess, err := p.Acquire(ctx, "endpoint")
	if err != nil {
		t.Fatal(err)
	}
	p.Release(sess)
	p.Release(sess) // misuse, must be inert
	p.Release(sess)

	// The endpoint still holds exactly one lendable session.
	first, err := p.Acquire(ctx, "endpoint")
	if err != nil {
		t.Fatalf("Acquire after double Release: %v", err)
	}
	if first != sess {
		t.Fatal("pool handed out a different session")
	}
	// Concurrent reuse lends the same session again; what must not happen is a
	// second identity for it appearing at the cap.
	second, err := p.Acquire(ctx, "endpoint")
	if err == nil && second != sess {
		t.Fatal("double Release created a second session past MaxSessionsPerEndpoint=1")
	}
}

package grpc

import (
	"testing"
	"time"

	"github.com/argos-io/argos/internal/session"
	"github.com/argos-io/argos/internal/sessionpool"
	"github.com/argos-io/argos/internal/transportbind"
	"github.com/argos-io/argos/transport"
)

// TestTransportReportsConfiguredLimits is the contract that replaced the
// bind-time push: WithLimits fixes the session limits at construction and
// Limits() reports them. Every field carries a distinct value so a report that
// dropped, defaulted or transposed one cannot pass.
func TestTransportReportsConfiguredLimits(t *testing.T) {
	t.Parallel()
	want := transport.Limits{
		MaxMessageSize:         1 << 20,
		MaxFrameSize:           1 << 18,
		MaxMetadataSize:        1 << 15,
		MaxInboundMetadataSize: 1 << 16,
		ReadAheadMessages:      7,
		OpenTimeout:            3 * time.Second,
		MaxDrainBytes:          1 << 17,
	}
	a, err := NewTransport(WithLimits(want))
	if err != nil {
		t.Fatalf("NewTransport: %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })

	if got := a.Limits(); got != want {
		t.Fatalf("Limits() = %+v, want %+v", got, want)
	}
}

// TestTransportReportsConfiguredPoolLimits does the same for the pool side.
// Zero is a meaningful value here — the axis does not bound that dimension —
// so the test pins that a zero field stays zero instead of being quietly
// replaced by the pool's own default.
func TestTransportReportsConfiguredPoolLimits(t *testing.T) {
	t.Parallel()
	a, err := NewTransport(
		WithPool(4, 0, 90*time.Second, 0),
		WithHandshakeTimeout(2*time.Second),
	)
	if err != nil {
		t.Fatalf("NewTransport: %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })

	want := transport.PoolLimits{
		MaxSessionsPerEndpoint: 4,
		MaxIdleSessions:        0,
		SessionIdleTimeout:     90 * time.Second,
		MaxSessionLifetime:     0,
		HandshakeTimeout:       2 * time.Second,
	}
	if got := a.PoolLimits(); got != want {
		t.Fatalf("PoolLimits() = %+v, want %+v", got, want)
	}
}

// TestBareTransportReportsDefaultBaseline pins the other half of the contract:
// a bare axis is not unbounded, it starts at the shared DefaultOptions
// baseline, which is what the composition layer checks against argos.Options.
func TestBareTransportReportsDefaultBaseline(t *testing.T) {
	t.Parallel()
	a, err := NewTransport()
	if err != nil {
		t.Fatalf("NewTransport: %v", err)
	}
	t.Cleanup(func() { _ = a.Close() })

	if got, want := a.Limits(), transportbind.ToLimits(session.DefaultOptions()); got != want {
		t.Fatalf("bare axis Limits() = %+v, want the session.DefaultOptions baseline %+v", got, want)
	}
	if got, want := a.PoolLimits(), transportbind.ToPoolLimits(sessionpool.DefaultOptions()); got != want {
		t.Fatalf("bare axis PoolLimits() = %+v, want the sessionpool.DefaultOptions baseline %+v", got, want)
	}
}

// TestLimitsArePerAxisNotGlobal: the limits belong to the instance, so building
// a second axis with different limits leaves the first exactly as it was. A
// package-level or last-writer-wins home for these values would pass the tests
// above and still hand one axis another axis' limits.
func TestLimitsArePerAxisNotGlobal(t *testing.T) {
	t.Parallel()
	first, err := NewTransport(WithLimits(transport.Limits{
		MaxMessageSize:    1 << 20,
		MaxFrameSize:      1 << 18,
		ReadAheadMessages: 1,
		OpenTimeout:       time.Second,
	}))
	if err != nil {
		t.Fatalf("NewTransport: %v", err)
	}
	t.Cleanup(func() { _ = first.Close() })

	second, err := NewTransport(WithLimits(transport.Limits{
		MaxMessageSize:    2 << 20,
		MaxFrameSize:      2 << 18,
		ReadAheadMessages: 3,
		OpenTimeout:       2 * time.Second,
	}))
	if err != nil {
		t.Fatalf("NewTransport: %v", err)
	}
	t.Cleanup(func() { _ = second.Close() })

	got := first.Limits()
	if got.MaxMessageSize != 1<<20 || got.MaxFrameSize != 1<<18 ||
		got.ReadAheadMessages != 1 || got.OpenTimeout != time.Second {
		t.Fatalf("first axis Limits() = %+v after a second axis was built; want its own values", got)
	}
}

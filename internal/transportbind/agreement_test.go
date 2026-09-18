package transportbind_test

import (
	"testing"

	"github.com/argos-io/argos/internal/session"
	"github.com/argos-io/argos/internal/sessionpool"
	"github.com/argos-io/argos/internal/transportbind"
	"github.com/argos-io/argos/transport"
	"github.com/argos-io/argos/transport/grpc"
	"github.com/argos-io/argos/transport/httpunary"
)

// TestBareAxesAreBounded guards the rule that a transport axis built with no
// options already enforces the baseline, rather than running unbounded.
//
// This is not hypothetical: every axis used to start at the zero Options, and
// zero means "no limit" to every check that reads it, so `grpc.NewTransport()`
// silently accepted messages of any size until something pushed limits in from
// outside. The defaults now come from one place, at construction, and this is
// the test that says so.
func TestBareAxesAreBounded(t *testing.T) {
	t.Parallel()

	wantSession := transportbind.ToLimits(session.DefaultOptions())
	wantPool := transportbind.ToPoolLimits(sessionpool.DefaultOptions())

	grpcAx, err := grpc.NewTransport()
	if err != nil {
		t.Fatalf("grpc.NewTransport: %v", err)
	}
	t.Cleanup(func() { _ = grpcAx.Close() })

	httpAx, err := httpunary.NewTransport()
	if err != nil {
		t.Fatalf("httpunary.NewTransport: %v", err)
	}
	t.Cleanup(func() { _ = httpAx.Close() })

	for _, tc := range []struct {
		name string
		tr   interface {
			Limits() transport.Limits
			PoolLimits() transport.PoolLimits
		}
	}{
		{"grpc", grpcAx},
		{"httpunary", httpAx},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := tc.tr.Limits()
			checkInt64(t, "Limits", map[string][2]int64{
				"MaxFrameSize":           {wantSession.MaxFrameSize, got.MaxFrameSize},
				"MaxMessageSize":         {wantSession.MaxMessageSize, got.MaxMessageSize},
				"MaxMetadataSize":        {wantSession.MaxMetadataSize, got.MaxMetadataSize},
				"MaxInboundMetadataSize": {wantSession.MaxInboundMetadataSize, got.MaxInboundMetadataSize},
				"MaxDrainBytes":          {wantSession.MaxDrainBytes, got.MaxDrainBytes},
				"ReadAheadMessages":      {int64(wantSession.ReadAheadMessages), int64(got.ReadAheadMessages)},
				"OpenTimeout":            {int64(wantSession.OpenTimeout), int64(got.OpenTimeout)},
			})

			gotP := tc.tr.PoolLimits()
			checkInt64(t, "PoolLimits", map[string][2]int64{
				"MaxSessionsPerEndpoint": {int64(wantPool.MaxSessionsPerEndpoint), int64(gotP.MaxSessionsPerEndpoint)},
				"SessionIdleTimeout":     {int64(wantPool.SessionIdleTimeout), int64(gotP.SessionIdleTimeout)},
				"MaxSessionLifetime":     {int64(wantPool.MaxSessionLifetime), int64(gotP.MaxSessionLifetime)},
				"HandshakeTimeout":       {int64(wantPool.HandshakeTimeout), int64(gotP.HandshakeTimeout)},
			})
		})
	}
}

// checkInt64 asserts each field matches the baseline. A built-in axis is
// expected to bound every dimension, so a zero is a failure rather than "not
// applicable" — these axes have no dimension they decline to limit.
func checkInt64(t *testing.T, kind string, fields map[string][2]int64) {
	t.Helper()
	for name, v := range fields {
		want, got := v[0], v[1]
		if got == 0 {
			t.Errorf("%s: bare axis leaves %s unbounded (0); want the %v baseline", kind, name, want)
			continue
		}
		if want != got {
			t.Errorf("%s: %s = %v in the baseline, %v on a bare axis", kind, name, want, got)
		}
	}
}

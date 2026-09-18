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

// TestBareAxesUseDefaultLimits guards the rule that a transport axis built with
// no options reports session.DefaultOptions / sessionpool.DefaultOptions (all
// zero: no cap until WithLimits / WithPool sets a field > 0).
func TestBareAxesUseDefaultLimits(t *testing.T) {
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

func checkInt64(t *testing.T, kind string, fields map[string][2]int64) {
	t.Helper()
	for name, v := range fields {
		want, got := v[0], v[1]
		if want != got {
			t.Errorf("%s: %s = %v on axis, want baseline %v", kind, name, got, want)
		}
	}
}

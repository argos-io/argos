package envelope_test

import (
	"testing"

	"github.com/argos-io/argos/framing"
	envframing "github.com/argos-io/argos/framing/envelope"
	"github.com/argos-io/argos/transport/ws"
)

func TestFrameSizeAboveWSReadLimitRejectedAtStart(t *testing.T) {
	fr := envframing.New(envframing.WithMaxInboundWireBytes(ws.DefaultMaxReadBytes))
	checker, ok := fr.(interface {
		CheckConfig(framing.Config) error
	})
	if !ok {
		t.Fatal("envelope Framing no longer exposes CheckConfig")
	}

	over := ws.DefaultMaxReadBytes
	if err := checker.CheckConfig(framing.Config{MaxFrameSize: over}); err == nil {
		t.Fatalf("CheckConfig accepted MaxFrameSize %d against a %d read limit",
			over, ws.DefaultMaxReadBytes)
	}

	under := ws.DefaultMaxReadBytes - 64
	if err := checker.CheckConfig(framing.Config{MaxFrameSize: under}); err != nil {
		t.Fatalf("CheckConfig rejected a fitting MaxFrameSize %d: %v", under, err)
	}
}

func TestTCPFramingHasNoWireLimitCheck(t *testing.T) {
	fr := envframing.New()
	checker := fr.(interface {
		CheckConfig(framing.Config) error
	})
	if err := checker.CheckConfig(framing.Config{MaxFrameSize: 1 << 30}); err != nil {
		t.Fatalf("tcp-style envelope rejected a large MaxFrameSize: %v", err)
	}
}

func TestMaxInboundWireBytesIgnoresNonPositive(t *testing.T) {
	fr := envframing.New(envframing.WithMaxInboundWireBytes(0))
	checker := fr.(interface {
		CheckConfig(framing.Config) error
	})
	if err := checker.CheckConfig(framing.Config{MaxFrameSize: 1 << 30}); err != nil {
		t.Fatalf("non-positive wire limit should opt out: %v", err)
	}
}

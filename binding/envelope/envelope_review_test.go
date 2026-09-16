package envelope_test

import (
	"strings"
	"testing"

	bindenvelope "github.com/argos-io/argos/binding/envelope"
	"github.com/argos-io/argos/framing"
	envframing "github.com/argos-io/argos/framing/envelope"
	"github.com/argos-io/argos/transport/ws"
)

// ws reads a negative limit as "no limit", which would hand a peer control over
// how much one inbound message may allocate. The binding must refuse it rather
// than forward it.
func TestNegativeMaxReadBytesRejected(t *testing.T) {
	_, err := bindenvelope.NewWS(bindenvelope.WithMaxReadBytes(-1))()
	if err == nil {
		t.Fatal("NewWS accepted a negative read limit")
	}
	if !strings.Contains(err.Error(), "negative") {
		t.Fatalf("err = %v, want it to name the negative limit", err)
	}

	// Zero still means "keep the transport default".
	if _, err := bindenvelope.NewWS(bindenvelope.WithMaxReadBytes(0))(); err != nil {
		t.Fatalf("NewWS with zero: %v", err)
	}
}

// A MaxFrameSize the ws transport would never deliver used to be accepted at
// start and fail at the first large message, closing the connection with 1009
// and taking unrelated calls with it.
func TestFrameSizeAboveWSReadLimitRejectedAtStart(t *testing.T) {
	b, err := bindenvelope.NewWS()()
	if err != nil {
		t.Fatal(err)
	}
	checker, ok := b.Framing.(interface {
		CheckConfig(framing.Config) error
	})
	if !ok {
		t.Fatal("envelope Framing no longer exposes CheckConfig")
	}

	// Default ws read limit is DefaultMaxReadBytes; one frame needs the body
	// plus the length prefix, so exactly at the limit is already too large.
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

// The declared wire limit is opt-in: tcp has no per-message ceiling, so the
// check must stay silent there.
func TestNonWSBindingsHaveNoWireLimitCheck(t *testing.T) {
	b, err := bindenvelope.NewTCP()()
	if err != nil {
		t.Fatal(err)
	}
	checker := b.Framing.(interface {
		CheckConfig(framing.Config) error
	})
	if err := checker.CheckConfig(framing.Config{MaxFrameSize: 1 << 30}); err != nil {
		t.Fatalf("tcp binding rejected a large MaxFrameSize: %v", err)
	}
}

// WithMaxInboundWireBytes is what carries the transport's ceiling to the
// framing; a non-positive value means "no ceiling declared".
func TestMaxInboundWireBytesIgnoresNonPositive(t *testing.T) {
	fr := envframing.New(envframing.WithMaxInboundWireBytes(0))
	checker := fr.(interface {
		CheckConfig(framing.Config) error
	})
	if err := checker.CheckConfig(framing.Config{MaxFrameSize: 1 << 30}); err != nil {
		t.Fatalf("zero wire limit should be inert: %v", err)
	}
}

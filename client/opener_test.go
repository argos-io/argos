package client

import (
	"context"
	"strings"
	"testing"

	"github.com/argos-io/argos"
	"github.com/argos-io/argos/internal/fake"
	"github.com/argos-io/argos/internal/session"
	"github.com/argos-io/argos/internal/teststack"
)

// This file covers what New checks while it assembles the opener. Only the
// codec-name comparison is left: an axis fixes its limits when it is
// constructed and nothing else holds a second copy of them, so there is
// nothing to reconcile against argos.Options any more.

// namedCodec is bytesCodec with a wire identity, so the axis/codec name check
// has something to compare.
type namedCodec struct {
	bytesCodec
	name string
}

func (c namedCodec) CodecName() string { return c.name }

// axisSpec is the spec a fake axis is built with: the session baseline, with
// whatever the test retunes.
func axisSpec(codecName string, tune func(*session.Options)) session.SessionSpec {
	spec := session.SessionSpec{CodecName: codecName, Options: session.DefaultOptions()}
	if tune != nil {
		tune(&spec.Options)
	}
	return spec
}

// An axis' codec name is fixed when it is constructed, so pairing it with a
// codec it does not advertise is an assembly error rather than a body the peer
// decodes with the wrong codec.
func TestNewRejectsCodecNameMismatch(t *testing.T) {
	t.Parallel()
	ax := buildLoopbackTransport(t, nil, nil, nil, fake.WithSessionSpec(axisSpec("proto", nil)))

	_, err := newClientLoopback(t, ax,
		argos.WithServiceName(testService),
		argos.WithTarget(testTarget),
		argos.WithCodec(teststack.CodecName(t, namedCodec{name: "json"})),
	)
	if err == nil {
		t.Fatal("New paired an axis that advertises proto with a codec that reports json")
	}
	for _, want := range []string{`"proto"`, `"json"`} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q does not name %q", err, want)
		}
	}
}

// The check is a comparison, not a requirement: a codec that carries no
// identity has nothing to compare against, so it pairs with any axis.
func TestNewAcceptsCodecWithoutName(t *testing.T) {
	t.Parallel()
	ax := buildLoopbackTransport(t, nil, nil, nil, fake.WithSessionSpec(axisSpec("proto", nil)))

	// bytesCodec does not implement codec.Named.
	cli, err := newClientLoopback(t, ax,
		argos.WithServiceName(testService),
		argos.WithTarget(testTarget),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	echoRoundTrip(t, cli, context.Background())
}

// The Client reads an axis' limits but never writes to them: one axis may be
// shared, and a bind-time write would silently overwrite the limits every other
// user already depends on.
func TestNewLeavesAxisLimitsAlone(t *testing.T) {
	t.Parallel()
	const limit = int64(1 << 20)
	ax := buildLoopbackTransport(t, nil, nil, nil, fake.WithSessionSpec(axisSpec("json", func(c *session.Options) {
		c.MaxMessageSize = limit
		c.MaxFrameSize = limit
		c.ReadAheadMessages = 4
	})))

	cli, err := newClientLoopback(t, ax,
		argos.WithServiceName(testService),
		argos.WithTarget(testTarget),
		argos.WithMaxConcurrentCalls(2),
		argos.WithCodec(teststack.CodecName(t, namedCodec{name: "json"})),
	)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	got := ax.Limits()
	if got.MaxMessageSize != limit || got.MaxFrameSize != limit || got.ReadAheadMessages != 4 {
		t.Fatalf("axis limits = %+v, want the ones it was built with (MaxMessageSize=%d, MaxFrameSize=%d, ReadAheadMessages=4)",
			got, limit, limit)
	}

	// The Client works over the axis it read.
	echoRoundTrip(t, cli, context.Background())
}

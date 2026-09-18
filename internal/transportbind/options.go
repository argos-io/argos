// Package transportbind maps the public transport limit types onto the internal
// session settings the axes use, and carries the one assembly check the
// composition layer still performs (internal only).
//
// It deliberately no longer installs limits on an axis, and no longer compares
// session or pool limits against argos.Options. An axis owns its limits from
// construction and nothing else holds a second copy of them, so there is
// nothing left to reconcile: passing a mismatched value is not caught here, it
// is simply not expressible.
package transportbind

import (
	"fmt"

	"github.com/argos-io/argos/codec"
	"github.com/argos-io/argos/internal/session"
	"github.com/argos-io/argos/internal/sessionpool"
	"github.com/argos-io/argos/transport"
)

// FromLimits maps the public transport.Limits an axis is built with onto the
// internal session.Options it applies.
func FromLimits(l transport.Limits) session.Options {
	return session.Options{
		MaxMessageSize:         l.MaxMessageSize,
		MaxFrameSize:           l.MaxFrameSize,
		MaxMetadataSize:        l.MaxMetadataSize,
		MaxInboundMetadataSize: l.MaxInboundMetadataSize,
		ReadAheadMessages:      l.ReadAheadMessages,
		OpenTimeout:            l.OpenTimeout,
		MaxDrainBytes:          l.MaxDrainBytes,
	}
}

// ToLimits is the inverse: an axis reports its effective session settings.
func ToLimits(c session.Options) transport.Limits {
	return transport.Limits{
		MaxMessageSize:         c.MaxMessageSize,
		MaxFrameSize:           c.MaxFrameSize,
		MaxMetadataSize:        c.MaxMetadataSize,
		MaxInboundMetadataSize: c.MaxInboundMetadataSize,
		ReadAheadMessages:      c.ReadAheadMessages,
		OpenTimeout:            c.OpenTimeout,
		MaxDrainBytes:          c.MaxDrainBytes,
	}
}

// ToPoolLimits reports an axis' effective pool settings as the public type.
func ToPoolLimits(c sessionpool.Options) transport.PoolLimits {
	return transport.PoolLimits{
		MaxSessionsPerEndpoint: c.MaxSessionsPerEndpoint,
		MaxIdleSessions:        c.MaxIdleSessions,
		SessionIdleTimeout:     c.SessionIdleTimeout,
		MaxSessionLifetime:     c.MaxSessionLifetime,
		HandshakeTimeout:       c.HandshakeTimeout,
	}
}

// CheckCodecName compares the codec identity an axis carries against the name
// its Codec reports. Pairing an axis with a codec it does not carry used to be
// silent: the axis held a constant name, so a gRPC axis with a JSON codec
// advertised protobuf and a peer decoded the body with the wrong codec.
//
// This is the one place two independent values still have to agree. Everything
// else the composition layer used to reconcile — session limits, pool limits —
// now has a single source (the axis) and needs no comparison.
//
// An axis that names no codec on the wire, or a codec that carries no identity,
// has nothing to compare.
func CheckCodecName(what, axisName string, cd codec.Codec) error {
	if axisName == "" {
		return nil
	}
	n, ok := cd.(codec.Named)
	if !ok {
		return nil
	}
	if got := n.CodecName(); got != axisName {
		return fmt.Errorf(
			"%s: transport carries codec %q but the codec reports %q; the name "+
				"is fixed when the axis is constructed, so set WithCodecName or pair "+
				"the axis with the codec that matches it",
			what, axisName, got)
	}
	return nil
}

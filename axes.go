package argos

import (
	"github.com/argos-io/argos/codec"
	"github.com/argos-io/argos/framing"
	"github.com/argos-io/argos/transport"
)

// A protocol is one Transport × Framing × Codec. Each axis is a factory rather
// than an instance: every Client and every server endpoint gets its own, and
// the composition layer closes what it built. Two Clients sharing one Config
// would otherwise share one Transport, and the first Close would kill the
// second.
//
// Each factory is invoked exactly once per Client or endpoint, and must not
// Dial or Serve.
type (
	// TransportFunc builds the transport axis.
	TransportFunc func() (transport.Transport, error)
	// FramingFunc builds the framing axis.
	FramingFunc func() (framing.Framing, error)
	// CodecFunc builds the codec axis.
	CodecFunc func() (codec.Codec, error)
)

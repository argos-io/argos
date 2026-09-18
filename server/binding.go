package server

import (
	"github.com/argos-io/argos"
	"github.com/argos-io/argos/codec"
	"github.com/argos-io/argos/internal/transportbind"
	"github.com/argos-io/argos/transport"
)

// newBinding assembles one listen surface: it takes the codec factory's Codec
// and checks that it matches the codec identity the caller-supplied axis
// carries.
//
// It installs nothing on the axis. Limits are fixed when the axis is
// constructed, and one instance may serve several surfaces — writing limits
// here is what let whichever surface started last decide what every other user
// got.
func newBinding(name string, sc argos.ServiceOptions) (transport.Transport, codec.Codec, error) {
	tr, err := sc.AssembleTransport()
	if err != nil {
		return nil, nil, err
	}
	cd, err := sc.AssembleCodec()
	if err != nil {
		return nil, nil, err
	}
	if err := transportbind.CheckCodecName(name, tr.CodecName(), cd); err != nil {
		return nil, nil, err
	}
	return tr, cd, nil
}

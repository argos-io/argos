package echov1

import (
	"github.com/argos-io/argos"
	"github.com/argos-io/argos/transport"
)

const testListenAddr = "127.0.0.1:0"

func withLoopbackTransport(tr transport.Transport) []argos.Option {
	return []argos.Option{
		argos.WithTransport(tr),
		argos.WithListenAddress(testListenAddr),
	}
}

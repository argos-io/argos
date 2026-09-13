package echov1

import (
	"github.com/argos-io/argos/option"
	"github.com/argos-io/argos/transport"
)

const testListenAddr = "127.0.0.1:0"

func withLoopbackTransport(tr transport.Transport) []option.Option {
	return []option.Option{
		option.WithTransport(tr),
		option.WithListenAddress(testListenAddr),
	}
}

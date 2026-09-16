package argos

import (
	"fmt"

	"github.com/argos-io/argos/codec"
	"github.com/argos-io/argos/framing"
	"github.com/argos-io/argos/transport"
)

// Protocol is Transport × Framing × Codec. Each axis is a factory: every Client
// and every server endpoint invokes them once and owns the result.
type Protocol struct {
	Transport TransportFunc
	Framing   FramingFunc
	Codec     CodecFunc
}

// Assemble builds fresh instances. Factories must not Dial or Serve.
func (p Protocol) Assemble() (transport.Transport, framing.Framing, codec.Codec, error) {
	if p.Transport == nil || p.Framing == nil || p.Codec == nil {
		return nil, nil, nil, fmt.Errorf("argos: incomplete protocol (Transport, Framing, Codec required)")
	}
	tr, err := p.Transport()
	if err != nil {
		return nil, nil, nil, err
	}
	fr, err := p.Framing()
	if err != nil {
		if tr != nil {
			_ = tr.Close()
		}
		return nil, nil, nil, err
	}
	cd, err := p.Codec()
	if err != nil {
		if tr != nil {
			_ = tr.Close()
		}
		if c, ok := fr.(interface{ Close() error }); ok {
			_ = c.Close()
		}
		return nil, nil, nil, err
	}
	if tr == nil || fr == nil || cd == nil {
		return nil, nil, nil, fmt.Errorf("argos: protocol factory returned nil component")
	}
	return tr, fr, cd, nil
}

// ServiceConfig holds per-service client settings (§6): how to reach the
// service and which protocol to speak. Populate via WithService; a Client
// selects one entry with WithServiceName.
type ServiceConfig struct {
	Protocol
	Target string
}

// EndpointConfig describes one server listen surface: which protocol it speaks
// and where it binds. An empty ServiceNames list exposes every registered
// service on that endpoint; otherwise only the named IDL services are routed
// there. Declare endpoints on DefaultConfig().Endpoints or pass them to
// server.AddEndpoint.
type EndpointConfig struct {
	Protocol
	ListenAddress string
	ServiceNames  []string
}

// ServiceOption configures one ServiceConfig entry during WithService.
type ServiceOption interface {
	applyService(*ServiceConfig)
}

type serviceOptionFunc func(*ServiceConfig)

func (f serviceOptionFunc) applyService(sc *ServiceConfig) { f(sc) }

// ServiceTransport sets the transport factory for a service entry.
func ServiceTransport(fn TransportFunc) ServiceOption {
	return serviceOptionFunc(func(sc *ServiceConfig) { sc.Transport = fn })
}

// ServiceFraming sets the framing factory for a service entry.
func ServiceFraming(fn FramingFunc) ServiceOption {
	return serviceOptionFunc(func(sc *ServiceConfig) { sc.Framing = fn })
}

// ServiceCodec sets the codec factory for a service entry.
func ServiceCodec(fn CodecFunc) ServiceOption {
	return serviceOptionFunc(func(sc *ServiceConfig) { sc.Codec = fn })
}

// ServiceProtocol sets all three axes at once (typical preset).
func ServiceProtocol(p Protocol) ServiceOption {
	return serviceOptionFunc(func(sc *ServiceConfig) { sc.Protocol = p })
}

// ServiceTarget sets the client dial target (e.g. ip://host:port).
func ServiceTarget(target string) ServiceOption {
	return serviceOptionFunc(func(sc *ServiceConfig) { sc.Target = target })
}

package argos

import (
	"fmt"

	"github.com/argos-io/argos/codec"
	"github.com/argos-io/argos/framing"
	"github.com/argos-io/argos/transport"
)

// Protocol is Transport × Framing × Codec. Each axis is a factory: every Client
// and every server listen surface invokes them once and owns the result.
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

// ServiceConfig holds per-service settings: protocol (Transport × Framing × Codec),
// client target, and server listen address(es). Populate via WithService; Clients
// select an entry with WithServiceName, and server.Run materialises listeners for
// every registered service that has a complete entry here.
type ServiceConfig struct {
	Protocol
	Target        string // client dial target (e.g. ip://host:port)
	ListenAddress string // server bind when Listeners is empty

	listeners []serviceListen
}

type serviceListen struct {
	address  string
	protocol Protocol
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

// ServiceTarget sets the client dial target (e.g. ip://127.0.0.1:7001).
func ServiceTarget(target string) ServiceOption {
	return serviceOptionFunc(func(sc *ServiceConfig) { sc.Target = target })
}

// ServiceListenAddress sets the server bind address when the service exposes a
// single listen surface (Listeners empty). Empty falls back to Config.ListenAddress.
func ServiceListenAddress(addr string) ServiceOption {
	return serviceOptionFunc(func(sc *ServiceConfig) { sc.ListenAddress = addr })
}

// ServiceListener adds a server listen surface for this service (protocol + address).
// Use multiple listeners to expose the same registered impl on several transports.
func ServiceListener(address string, p Protocol) ServiceOption {
	return serviceOptionFunc(func(sc *ServiceConfig) {
		sc.listeners = append(sc.listeners, serviceListen{address: address, protocol: p})
	})
}

// ServiceListenPlan is one server listen surface derived from ServiceConfig.
type ServiceListenPlan struct {
	Address  string
	Protocol Protocol
}

// ServerListenPlans returns listen surfaces for this service (server-side).
func (sc ServiceConfig) ServerListenPlans(fallbackListen string) ([]ServiceListenPlan, error) {
	if len(sc.listeners) > 0 {
		out := make([]ServiceListenPlan, 0, len(sc.listeners))
		for _, l := range sc.listeners {
			if l.protocol.Transport == nil || l.protocol.Framing == nil || l.protocol.Codec == nil {
				return nil, fmt.Errorf("incomplete protocol on listener %q", l.address)
			}
			addr := l.address
			if addr == "" {
				addr = fallbackListen
			}
			if addr == "" {
				return nil, fmt.Errorf("listener missing address (set ServiceListenAddress or Config.ListenAddress)")
			}
			out = append(out, ServiceListenPlan{Address: addr, Protocol: l.protocol})
		}
		return out, nil
	}
	if sc.Transport == nil || sc.Framing == nil || sc.Codec == nil {
		return nil, fmt.Errorf("incomplete protocol (Transport, Framing, Codec required)")
	}
	addr := sc.ListenAddress
	if addr == "" {
		addr = fallbackListen
	}
	if addr == "" {
		return nil, fmt.Errorf("missing listen address (ServiceListenAddress or Config.ListenAddress)")
	}
	return []ServiceListenPlan{{Address: addr, Protocol: sc.Protocol}}, nil
}

func cloneServiceConfig(sc ServiceConfig) ServiceConfig {
	out := sc
	if len(sc.listeners) > 0 {
		out.listeners = append([]serviceListen(nil), sc.listeners...)
	}
	return out
}

// ProtocolListenKey identifies a deduplicated listen surface (address + factories).
func ProtocolListenKey(addr string, p Protocol) string {
	return fmt.Sprintf("%s|%p|%p|%p", addr, p.Transport, p.Framing, p.Codec)
}

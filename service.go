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
	// TransportName, FramingName and CodecName select registered factories on
	// Config when the matching factory field is nil (text configuration).
	TransportName string
	FramingName   string
	CodecName     string
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

// ServiceTransportName selects a registered transport factory by name.
func ServiceTransportName(name string) ServiceOption {
	return serviceOptionFunc(func(sc *ServiceConfig) { sc.TransportName = name })
}

// ServiceFramingName selects a registered framing factory by name.
func ServiceFramingName(name string) ServiceOption {
	return serviceOptionFunc(func(sc *ServiceConfig) { sc.FramingName = name })
}

// ServiceCodecName selects a registered codec factory by name.
func ServiceCodecName(name string) ServiceOption {
	return serviceOptionFunc(func(sc *ServiceConfig) { sc.CodecName = name })
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
// cfg resolves named axes before checking completeness.
func (sc ServiceConfig) ServerListenPlans(cfg *Config, fallbackListen string) ([]ServiceListenPlan, error) {
	if cfg == nil {
		return nil, fmt.Errorf("argos: nil Config")
	}
	if len(sc.listeners) > 0 {
		out := make([]ServiceListenPlan, 0, len(sc.listeners))
		for _, l := range sc.listeners {
			p, err := cfg.ResolveProtocol(l.protocol)
			if err != nil {
				return nil, fmt.Errorf("listener %q: %w", l.address, err)
			}
			addr := l.address
			if addr == "" {
				addr = fallbackListen
			}
			if addr == "" {
				return nil, fmt.Errorf("listener missing address (set ServiceListenAddress or Config.ListenAddress)")
			}
			out = append(out, ServiceListenPlan{Address: addr, Protocol: p})
		}
		return out, nil
	}
	p, err := cfg.ResolveProtocol(sc.Protocol)
	if err != nil {
		return nil, err
	}
	addr := sc.ListenAddress
	if addr == "" {
		addr = fallbackListen
	}
	if addr == "" {
		return nil, fmt.Errorf("missing listen address (ServiceListenAddress or Config.ListenAddress)")
	}
	return []ServiceListenPlan{{Address: addr, Protocol: p}}, nil
}

func cloneServiceConfig(sc ServiceConfig) ServiceConfig {
	out := sc
	if len(sc.listeners) > 0 {
		out.listeners = append([]serviceListen(nil), sc.listeners...)
	}
	return out
}

// ProtocolListenKey identifies a deduplicated listen surface (address + axes).
func ProtocolListenKey(addr string, p Protocol) string {
	if p.Transport != nil || p.Framing != nil || p.Codec != nil {
		return fmt.Sprintf("%s|%p|%p|%p", addr, p.Transport, p.Framing, p.Codec)
	}
	return fmt.Sprintf("%s|%s|%s|%s", addr, p.TransportName, p.FramingName, p.CodecName)
}

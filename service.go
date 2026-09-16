package argos

import (
	"fmt"

	"github.com/argos-io/argos/codec"
	"github.com/argos-io/argos/framing"
	"github.com/argos-io/argos/transport"
)

// ServiceConfig holds per-service settings: Transport, Framing and Codec
// factories, client target, and server listen address(es). Populate via
// WithService; Clients select an entry with WithServiceName, and server.Run
// materialises listeners for every registered service that has a complete entry
// here.
type ServiceConfig struct {
	Transport     TransportFunc
	Framing       FramingFunc
	Codec         CodecFunc
	Target        string // client dial target (e.g. ip://host:port)
	ListenAddress string // server bind when Listeners is empty

	listeners []serviceListen
}

type serviceListen struct {
	address string
	axes    ServiceConfig // transport / framing / codec for this surface
}

// ServiceOption configures one ServiceConfig entry during WithService.
type ServiceOption interface {
	applyService(*ServiceConfig)
}

type serviceOptionFunc func(*ServiceConfig)

func (f serviceOptionFunc) applyService(sc *ServiceConfig) { f(sc) }

// JoinService applies several service options in order (typical preset).
func JoinService(opts ...ServiceOption) ServiceOption {
	return serviceOptionFunc(func(sc *ServiceConfig) {
		for _, o := range opts {
			if o != nil {
				o.applyService(sc)
			}
		}
	})
}

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

// ServiceTarget sets the client dial target (e.g. ip://127.0.0.1:7001).
func ServiceTarget(target string) ServiceOption {
	return serviceOptionFunc(func(sc *ServiceConfig) { sc.Target = target })
}

// ServiceListenAddress sets the server bind address when the service exposes a
// single listen surface (Listeners empty). Empty falls back to Config.ListenAddress.
func ServiceListenAddress(addr string) ServiceOption {
	return serviceOptionFunc(func(sc *ServiceConfig) { sc.ListenAddress = addr })
}

// ServiceListener adds a server listen surface for this service (address +
// per-listener transport / framing / codec options). Use multiple listeners to
// expose the same registered impl on several transports.
func ServiceListener(address string, opts ...ServiceOption) ServiceOption {
	return serviceOptionFunc(func(sc *ServiceConfig) {
		var l serviceListen
		l.address = address
		for _, o := range opts {
			if o != nil {
				o.applyService(&l.axes)
			}
		}
		sc.listeners = append(sc.listeners, l)
	})
}

// ServiceListenPlan is one server listen surface derived from ServiceConfig.
type ServiceListenPlan struct {
	Address string
	Axes    ServiceConfig
}

// ServerListenPlans returns listen surfaces for this service (server-side).
func (sc ServiceConfig) ServerListenPlans(fallbackListen string) ([]ServiceListenPlan, error) {
	if len(sc.listeners) > 0 {
		out := make([]ServiceListenPlan, 0, len(sc.listeners))
		for _, l := range sc.listeners {
			if err := l.axes.checkComplete(); err != nil {
				return nil, fmt.Errorf("listener %q: %w", l.address, err)
			}
			addr := l.address
			if addr == "" {
				addr = fallbackListen
			}
			if addr == "" {
				return nil, fmt.Errorf("listener missing address (set ServiceListenAddress or Config.ListenAddress)")
			}
			out = append(out, ServiceListenPlan{Address: addr, Axes: l.axes})
		}
		return out, nil
	}
	if err := sc.checkComplete(); err != nil {
		return nil, err
	}
	addr := sc.ListenAddress
	if addr == "" {
		addr = fallbackListen
	}
	if addr == "" {
		return nil, fmt.Errorf("missing listen address (ServiceListenAddress or Config.ListenAddress)")
	}
	return []ServiceListenPlan{{Address: addr, Axes: sc}}, nil
}

func (sc ServiceConfig) checkComplete() error {
	if sc.Transport == nil || sc.Framing == nil || sc.Codec == nil {
		return fmt.Errorf("argos: incomplete service axes (Transport, Framing, Codec required)")
	}
	return nil
}

// Assemble builds fresh Transport, Framing and Codec instances. Factories must
// not Dial or Serve.
func (sc ServiceConfig) Assemble() (transport.Transport, framing.Framing, codec.Codec, error) {
	if err := sc.checkComplete(); err != nil {
		return nil, nil, nil, err
	}
	tr, err := sc.Transport()
	if err != nil {
		closeTransport(tr)
		return nil, nil, nil, err
	}
	fr, err := sc.Framing()
	if err != nil {
		closeTransport(tr)
		return nil, nil, nil, err
	}
	cd, err := sc.Codec()
	if err != nil {
		closeTransport(tr)
		return nil, nil, nil, err
	}
	if tr == nil || fr == nil || cd == nil {
		closeTransport(tr)
		return nil, nil, nil, fmt.Errorf("argos: axis factory returned nil component")
	}
	return tr, fr, cd, nil
}

// closeTransport releases a Transport a failed Assemble built. Assemble returns
// no component alongside an error, so one it built and does not return would be
// unreachable; Framing and Codec hold nothing to release (README §4.1).
func closeTransport(tr transport.Transport) {
	if tr != nil {
		_ = tr.Close()
	}
}

func cloneServiceConfig(sc ServiceConfig) ServiceConfig {
	out := sc
	if len(sc.listeners) > 0 {
		out.listeners = append([]serviceListen(nil), sc.listeners...)
	}
	return out
}

// ServiceListenKey identifies a deduplicated listen surface (address + axes).
func ServiceListenKey(addr string, sc ServiceConfig) string {
	return fmt.Sprintf("%s|%p|%p|%p", addr, sc.Transport, sc.Framing, sc.Codec)
}

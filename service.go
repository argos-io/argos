package argos

import (
	"fmt"

	"github.com/argos-io/argos/codec"
	"github.com/argos-io/argos/transport"
)

// ServiceOptions holds per-service settings: registered Transport and Codec
// names, client target, and server listen address(es). Populate via
// WithClientService / WithServerService; Clients select an entry with
// WithServiceName, and server.Run materialises listeners for every registered
// service that has a complete entry here.
type ServiceOptions struct {
	Transport     string // registered transport name
	Codec         string // registered codec name
	Target        string // client dial target (e.g. ip://host:port)
	ListenAddress string // server bind when Listeners is empty

	listeners []serviceListen
}

type serviceListen struct {
	address   string
	transport string
	codec     string
}

// ServiceOption configures one ServiceOptions entry during WithClientService / WithServerService.
type ServiceOption interface {
	applyService(*ServiceOptions)
}

type serviceOptionFunc func(*ServiceOptions)

func (f serviceOptionFunc) applyService(sc *ServiceOptions) { f(sc) }

// ServiceTransport sets the registered transport name for a service entry.
func ServiceTransport(name string) ServiceOption {
	return serviceOptionFunc(func(sc *ServiceOptions) { sc.Transport = name })
}

// ServiceCodec sets the registered codec name for a service entry.
func ServiceCodec(name string) ServiceOption {
	return serviceOptionFunc(func(sc *ServiceOptions) { sc.Codec = name })
}

// ServiceTarget sets the client dial target (e.g. ip://127.0.0.1:7001).
func ServiceTarget(target string) ServiceOption {
	return serviceOptionFunc(func(sc *ServiceOptions) { sc.Target = target })
}

// ServiceListenAddress sets the server bind address when the service exposes a
// single listen surface (Listeners empty). Empty falls back to Options.ListenAddress.
func ServiceListenAddress(addr string) ServiceOption {
	return serviceOptionFunc(func(sc *ServiceOptions) { sc.ListenAddress = addr })
}

// ServiceBindListen adds one listen surface: address and registered transport
// and codec names.
//
// Each resolved transport serves one surface. Use several ServiceBindListen
// options under WithServerService to expose the same registered impl on
// several transports or ports.
func ServiceBindListen(address, transportName, codecName string) ServiceOption {
	return serviceOptionFunc(func(sc *ServiceOptions) {
		sc.listeners = append(sc.listeners, serviceListen{
			address:   address,
			transport: transportName,
			codec:     codecName,
		})
	})
}

// ServiceListenPlan is one server listen surface derived from ServiceOptions.
type ServiceListenPlan struct {
	Address string
	Stack   ServiceOptions
}

// ServerListenPlans returns listen surfaces for this service (server-side).
func (sc ServiceOptions) ServerListenPlans(fallbackListen string) ([]ServiceListenPlan, error) {
	if len(sc.listeners) > 0 {
		out := make([]ServiceListenPlan, 0, len(sc.listeners))
		for _, l := range sc.listeners {
			stack := ServiceOptions{Transport: l.transport, Codec: l.codec}
			if err := stack.checkComplete(); err != nil {
				return nil, fmt.Errorf("listener %q: %w", l.address, err)
			}
			addr := l.address
			if addr == "" {
				addr = fallbackListen
			}
			if addr == "" {
				return nil, fmt.Errorf("listener missing address (set ServiceListenAddress or Options.ListenAddress)")
			}
			out = append(out, ServiceListenPlan{Address: addr, Stack: stack})
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
		return nil, fmt.Errorf("missing listen address (ServiceListenAddress or Options.ListenAddress)")
	}
	return []ServiceListenPlan{{Address: addr, Stack: sc}}, nil
}

func (sc ServiceOptions) checkComplete() error {
	if sc.Transport == "" || sc.Codec == "" {
		return fmt.Errorf("argos: incomplete service stack (Transport + Codec)")
	}
	return nil
}

// AssembleTransport builds this entry's Transport from its registered name.
func (sc ServiceOptions) AssembleTransport() (transport.Transport, error) {
	if err := sc.checkComplete(); err != nil {
		return nil, err
	}
	return transport.New(sc.Transport)
}

// AssembleCodec builds this entry's Codec from its registered name.
func (sc ServiceOptions) AssembleCodec() (codec.Codec, error) {
	if err := sc.checkComplete(); err != nil {
		return nil, err
	}
	return codec.New(sc.Codec)
}

func cloneServiceOptions(sc ServiceOptions) ServiceOptions {
	out := sc
	if len(sc.listeners) > 0 {
		out.listeners = append([]serviceListen(nil), sc.listeners...)
	}
	return out
}

// ServiceListenKey identifies a listen surface for deduplication.
func ServiceListenKey(addr string, sc ServiceOptions) string {
	return addr + "|" + sc.Transport + "|" + sc.Codec
}

package argos

import (
	"fmt"

	"github.com/argos-io/argos/codec"
	"github.com/argos-io/argos/transport"
)

// ServiceOptions holds per-service settings: registered Transport and Codec
// names, client target, and server listen address(es). Write fields on
// Options.Services[fullName]; Clients select an entry with WithServiceName,
// and server.Run materialises listeners for every registered service that has
// a complete entry here.
type ServiceOptions struct {
	Transport     string // registered transport name
	Codec         string // registered codec name
	Target        string // client dial target (e.g. ip://host:port)
	ListenAddress string // server bind when Listeners is empty

	// Listeners holds one or more listen surfaces (address + transport + codec).
	// When non-empty, ServerListenPlans ignores Transport, Codec, and
	// ListenAddress on this struct.
	Listeners []ServiceListen
}

// ServiceListen is one server listen surface.
type ServiceListen struct {
	Address   string
	Transport string
	Codec     string
}

// ServiceListenPlan is one server listen surface derived from ServiceOptions.
type ServiceListenPlan struct {
	Address string
	Stack   ServiceOptions
}

// ServerListenPlans returns listen surfaces for this service (server-side).
func (sc ServiceOptions) ServerListenPlans(fallbackListen string) ([]ServiceListenPlan, error) {
	if len(sc.Listeners) > 0 {
		out := make([]ServiceListenPlan, 0, len(sc.Listeners))
		for _, l := range sc.Listeners {
			stack := ServiceOptions{Transport: l.Transport, Codec: l.Codec}
			if err := stack.checkComplete(); err != nil {
				return nil, fmt.Errorf("listener %q: %w", l.Address, err)
			}
			addr := l.Address
			if addr == "" {
				addr = fallbackListen
			}
			if addr == "" {
				return nil, fmt.Errorf("listener missing address (set ListenAddress on ServiceOptions or Options.ListenAddress)")
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
		return nil, fmt.Errorf("missing listen address (ServiceOptions.ListenAddress or Options.ListenAddress)")
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
	if len(sc.Listeners) > 0 {
		out.Listeners = append([]ServiceListen(nil), sc.Listeners...)
	}
	return out
}

// ServiceListenKey identifies a listen surface for deduplication.
func ServiceListenKey(addr string, sc ServiceOptions) string {
	return addr + "|" + sc.Transport + "|" + sc.Codec
}

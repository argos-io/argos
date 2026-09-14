package transport

import (
	"context"
	"errors"
	"fmt"
	"net"
	"reflect"
	"sync"
)

// ListenState tracks bind address for loopback dial after port 0 listen.
type ListenState struct {
	configured string
	ready      chan struct{}
	once       sync.Once

	mu   sync.RWMutex
	addr net.Addr
	err  error
}

// NewListenState creates listen tracking state for a Transport channel.
func NewListenState() *ListenState {
	return &ListenState{ready: make(chan struct{})}
}

// MarkListen records the configured listen address and bind result.
func (s *ListenState) MarkListen(configured string, bound net.Addr, err error) {
	s.mu.Lock()
	s.configured = configured
	s.addr = bound
	s.err = err
	s.mu.Unlock()
	s.once.Do(func() { close(s.ready) })
}

// BoundAddr returns the bound listener address after ListenAndServe starts.
func (s *ListenState) BoundAddr() net.Addr {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.addr
}

// DialAddress resolves the dial target from client options and listen state.
func (s *ListenState) DialAddress(ctx context.Context, opts ClientOptions) (string, error) {
	if ctx == nil {
		return "", errors.New("transport: nil context")
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if opts.DialAddress != "" {
		return opts.DialAddress, nil
	}
	s.mu.RLock()
	configured := s.configured
	bound := s.addr
	s.mu.RUnlock()
	if configured == "" {
		if bound != nil {
			return dialableAddress(bound), nil
		}
		return "", errors.New("transport: dial address required")
	}
	_, port, err := net.SplitHostPort(configured)
	if err == nil && port == "0" {
		select {
		case <-s.ready:
		case <-ctx.Done():
			return "", ctx.Err()
		}
		s.mu.RLock()
		defer s.mu.RUnlock()
		if s.err != nil {
			return "", s.err
		}
		if s.addr == nil {
			return "", errors.New("transport: listener did not report a bound address")
		}
		return dialableAddress(s.addr), nil
	}
	if host, port, err := net.SplitHostPort(configured); err == nil {
		if host == "" {
			// A shared loopback Transport is often configured with the server
			// shorthand ":port". That is a valid listen address but not a
			// useful client dial target on its own.
			if bound != nil {
				return dialableAddress(bound), nil
			}
			return net.JoinHostPort("127.0.0.1", port), nil
		}
		if ip := net.ParseIP(host); ip != nil && ip.IsUnspecified() {
			if bound != nil {
				return dialableAddress(bound), nil
			}
			return net.JoinHostPort(loopbackHost(ip), port), nil
		}
	}
	return configured, nil
}

// DialableAddress formats a bound listener address for local dial (127.0.0.1:port).
func DialableAddress(addr net.Addr) string {
	return dialableAddress(addr)
}

func dialableAddress(addr net.Addr) string {
	if addr == nil {
		return ""
	}
	switch a := addr.(type) {
	case *net.TCPAddr:
		if !a.IP.IsUnspecified() && len(a.IP) != 0 {
			return addr.String()
		}
		return net.JoinHostPort(loopbackHost(a.IP), fmt.Sprint(a.Port))
	case *net.UDPAddr:
		if !a.IP.IsUnspecified() && len(a.IP) != 0 {
			return addr.String()
		}
		return net.JoinHostPort(loopbackHost(a.IP), fmt.Sprint(a.Port))
	default:
		return addr.String()
	}
}

func loopbackHost(ip net.IP) string {
	if len(ip) == 0 {
		return "127.0.0.1"
	}
	if ip.To4() != nil {
		return "127.0.0.1"
	}
	return "::1"
}

// ListenTCP opens a TCP listener from server options.
func ListenTCP(opts ServerOptions) (net.Listener, error) {
	if opts.Listener != nil {
		if isNilListener(opts.Listener) {
			return nil, errors.New("transport: listener is nil")
		}
		return opts.Listener, nil
	}
	if opts.ListenAddress == "" {
		return nil, errors.New("transport: listen address required")
	}
	network := opts.Network
	if network == "" {
		network = "tcp"
	}
	return net.Listen(network, opts.ListenAddress)
}

func isNilListener(listener net.Listener) bool {
	value := reflect.ValueOf(listener)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

// ListenUDP opens a UDP packet conn from server options.
func ListenUDP(opts ServerOptions) (net.PacketConn, error) {
	if opts.ListenAddress == "" {
		return nil, errors.New("transport: listen address required")
	}
	network := opts.Network
	if network == "" || network == "udp" {
		network = udpNetwork(opts.ListenAddress)
	}
	return net.ListenPacket(network, opts.ListenAddress)
}

func udpNetwork(address string) string {
	host, _, err := net.SplitHostPort(address)
	if err == nil {
		if ip := net.ParseIP(host); ip != nil {
			if ip.To4() != nil {
				return "udp4"
			}
			return "udp6"
		}
	}
	// IPv4 is the most portable default for an unspecified listen address and
	// matches DialableAddress's loopback fallback.
	return "udp4"
}

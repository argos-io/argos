package transport

import (
	"context"
	"errors"
	"fmt"
	"net"
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
	s.configured = configured
	s.mu.Lock()
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
	if opts.DialAddress != "" {
		return opts.DialAddress, nil
	}
	if s.configured == "" {
		return "", errors.New("transport: dial address required")
	}
	_, port, err := net.SplitHostPort(s.configured)
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
		return dialableAddress(s.addr), nil
	}
	return s.configured, nil
}

// DialableAddress formats a bound listener address for local dial (127.0.0.1:port).
func DialableAddress(addr net.Addr) string {
	return dialableAddress(addr)
}

func dialableAddress(addr net.Addr) string {
	switch a := addr.(type) {
	case *net.TCPAddr:
		if !a.IP.IsUnspecified() && len(a.IP) != 0 {
			return addr.String()
		}
		return net.JoinHostPort("127.0.0.1", fmt.Sprint(a.Port))
	case *net.UDPAddr:
		if !a.IP.IsUnspecified() && len(a.IP) != 0 {
			return addr.String()
		}
		return net.JoinHostPort("127.0.0.1", fmt.Sprint(a.Port))
	default:
		return addr.String()
	}
}

// ListenTCP opens a TCP listener from server options.
func ListenTCP(opts ServerOptions) (net.Listener, error) {
	if opts.Listener != nil {
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

// ListenUDP opens a UDP packet conn from server options.
func ListenUDP(opts ServerOptions) (net.PacketConn, error) {
	if opts.ListenAddress == "" {
		return nil, errors.New("transport: listen address required")
	}
	network := opts.Network
	if network == "" {
		network = "udp"
	}
	return net.ListenPacket(network, opts.ListenAddress)
}

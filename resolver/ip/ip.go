// Package ip registers the ip resolver for direct host:port targets.
package ip

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"strings"

	"github.com/argos-io/argos/resolver"
)

func init() {
	resolver.Register("ip", Resolver{})
}

// Resolver parses ip://host:port (or a comma-separated list; one entry is chosen).
type Resolver struct{}

func (Resolver) Select(_ context.Context, service string) (string, error) {
	service = strings.TrimSpace(strings.TrimPrefix(service, "//"))
	if service == "" {
		return "", errors.New("resolver/ip: empty address")
	}
	candidates := splitCandidates(service)
	addr, err := pick(candidates)
	if err != nil {
		return "", err
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "", fmt.Errorf("resolver/ip: %w", err)
	}
	if host == "" || port == "" {
		return "", errors.New("resolver/ip: invalid address")
	}
	return net.JoinHostPort(host, port), nil
}

func splitCandidates(service string) []string {
	parts := strings.Split(service, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func pick(candidates []string) (string, error) {
	if len(candidates) == 0 {
		return "", errors.New("resolver/ip: empty address")
	}
	if len(candidates) == 1 {
		return candidates[0], nil
	}
	return candidates[rand.IntN(len(candidates))], nil
}

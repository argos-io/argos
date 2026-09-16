//go:build ignore

package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/argos-io/argos"
	envelopebinding "github.com/argos-io/argos/binding/envelope"
	grpcbinding "github.com/argos-io/argos/binding/grpc"
	wholebodybinding "github.com/argos-io/argos/binding/wholebody"
	echov1 "github.com/argos-io/argos/example/echo"
	"github.com/argos-io/argos/server"
)

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))

	argos.DefaultConfig().MaxMessageSize = 1 << 20

	srv := server.New()
	impl := echov1.NewEchoImpl()

	endpoints := []struct {
		name string
		ep   argos.EndpointConfig
		opts []argos.ServerOption
	}{
		{
			name: "grpc/http2",
			ep:   argos.EndpointConfig{Protocol: grpcbinding.New()},
			opts: []argos.ServerOption{argos.WithListenAddress(":9090")},
		},
		{
			name: "envelope/tcp",
			ep:   argos.EndpointConfig{Protocol: envelopebinding.NewTCP()},
			opts: []argos.ServerOption{argos.WithListenAddress(":7000")},
		},
		{
			name: "envelope/ws",
			ep:   argos.EndpointConfig{Protocol: envelopebinding.NewWS()},
			opts: []argos.ServerOption{argos.WithListenAddress(":8081")},
		},
		{
			name: "envelope/udp",
			ep:   argos.EndpointConfig{Protocol: envelopebinding.NewUDP()},
			opts: []argos.ServerOption{
				argos.WithListenAddress(":7001"),
				argos.WithMaxFrameSize(65507),
				argos.WithMaxMessageSize(32 << 10),
			},
		},
		{
			name: "wholebody/http1",
			ep:   argos.EndpointConfig{Protocol: wholebodybinding.New()},
			opts: []argos.ServerOption{argos.WithListenAddress(":8080")},
		},
	}

	for _, b := range endpoints {
		if err := srv.AddEndpoint(b.ep, b.opts...); err != nil {
			slog.Error("AddEndpoint", "endpoint", b.name, "err", err)
			os.Exit(1)
		}
	}
	if err := echov1.RegisterEchoService(srv, impl); err != nil {
		slog.Error("RegisterEchoService", "err", err)
		os.Exit(1)
	}

	if err := srv.Run(context.Background()); err != nil {
		slog.Error("echo server", "err", err)
		os.Exit(1)
	}
}

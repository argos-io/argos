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

	cfg, err := argos.New()
	if err != nil {
		slog.Error("argos.New", "err", err)
		os.Exit(1)
	}
	srv := server.New(cfg)
	impl := echov1.NewEchoImpl()

	bindings := []struct {
		name string
		fn   argos.BindingFunc
		addr string
	}{
		{name: "grpc/http2", fn: grpcbinding.New(), addr: ":9090"},
		{name: "envelope/tcp", fn: envelopebinding.NewTCP(), addr: ":7000"},
		{name: "envelope/ws", fn: envelopebinding.NewWS(), addr: ":8081"},
		{name: "envelope/udp", fn: envelopebinding.NewUDP(), addr: ":7001"},
		{name: "wholebody/http1", fn: wholebodybinding.New(), addr: ":8080"},
	}

	for _, b := range bindings {
		opts := []argos.Option{argos.WithListenAddress(b.addr)}
		if b.name == "envelope/udp" {
			// Datagram budget: keep frames/messages under udp.MaxDatagramSize.
			opts = append(opts,
				argos.WithMaxFrameSize(65507),
				argos.WithMaxMessageSize(32<<10),
			)
		}
		if err := srv.AddBinding(b.fn, opts...); err != nil {
			slog.Error("AddBinding", "binding", b.name, "err", err)
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

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

	// Tuning that belongs to the program rather than to one binding goes on the
	// process default, once, before anything is built: this server only echoes
	// short strings, so the 4 MiB message default is more than it will ever
	// need. Every server.New / client.New that does not name a Config of its
	// own starts from here.
	argos.DefaultConfig().MaxMessageSize = 1 << 20

	// New takes options only and reports no error, so a rejected option set
	// surfaces on AddBinding and Run — both checked below.
	srv := server.New()
	impl := echov1.NewEchoImpl()

	// A binding is the unit of server configuration: its listen address, and
	// anything only its transport needs, travel with it to AddBinding.
	bindings := []struct {
		name string
		fn   argos.BindingFunc
		opts []argos.ServerOption
	}{
		{
			name: "grpc/http2",
			fn:   grpcbinding.New(),
			opts: []argos.ServerOption{argos.WithListenAddress(":9090")},
		},
		{
			name: "envelope/tcp",
			fn:   envelopebinding.NewTCP(),
			opts: []argos.ServerOption{argos.WithListenAddress(":7000")},
		},
		{
			name: "envelope/ws",
			fn:   envelopebinding.NewWS(),
			opts: []argos.ServerOption{argos.WithListenAddress(":8081")},
		},
		{
			name: "envelope/udp",
			fn:   envelopebinding.NewUDP(),
			opts: []argos.ServerOption{
				argos.WithListenAddress(":7001"),
				// Datagram budget: keep frames/messages under
				// udp.MaxDatagramSize. Only this transport has to fit a call
				// into one datagram, so the limits stay on its binding instead
				// of shrinking the whole process.
				argos.WithMaxFrameSize(65507),
				argos.WithMaxMessageSize(32 << 10),
			},
		},
		{
			name: "wholebody/http1",
			fn:   wholebodybinding.New(),
			opts: []argos.ServerOption{argos.WithListenAddress(":8080")},
		},
	}

	for _, b := range bindings {
		if err := srv.AddBinding(b.fn, b.opts...); err != nil {
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

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

const echoService = "echo.v1.EchoService"

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))

	// Datagram limits must fit the smallest carrier in the demo (envelope×udp).
	demoCfg := argos.Defaults()
	demoCfg.MaxMessageSize = 32 << 10
	demoCfg.MaxFrameSize = 65507

	srv := server.New(
		argos.WithConfig(&demoCfg),
		argos.WithService(echoService,
			argos.ServiceListener(":9090", grpcbinding.New()),
			argos.ServiceListener(":7000", envelopebinding.NewTCP()),
			argos.ServiceListener(":8081", envelopebinding.NewWS()),
			argos.ServiceListener(":7001", envelopebinding.NewUDP()),
			argos.ServiceListener(":8080", wholebodybinding.New()),
		),
	)
	if err := echov1.RegisterEchoService(srv, echov1.NewEchoImpl()); err != nil {
		slog.Error("RegisterEchoService", "err", err)
		os.Exit(1)
	}
	if err := srv.Run(context.Background()); err != nil {
		slog.Error("echo server", "err", err)
		os.Exit(1)
	}
}

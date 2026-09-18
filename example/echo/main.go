//go:build ignore

package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/argos-io/argos"
	echov1 "github.com/argos-io/argos/example/echo"
	"github.com/argos-io/argos/server"
)

const echoService = "echo.v1.EchoService"

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))

	trG, frG, cdG := echov1.GRPCAxes()
	trH, frH, cdH := echov1.HTTPUnaryRPCAxes()

	srv := server.New(
		argos.WithService(echoService,
			argos.ServiceListener(":9090", echov1.ServiceAxes(trG, frG, cdG)...),
			argos.ServiceListener(":8080", echov1.ServiceAxes(trH, frH, cdH)...),
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

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

	// Datagram limits must fit the smallest carrier in the demo (envelope×udp).
	demoCfg := argos.Defaults()
	demoCfg.MaxMessageSize = 32 << 10
	demoCfg.MaxFrameSize = 65507

	trG, frG, cdG := echov1.GRPCAxes()
	trT, frT, cdT := echov1.EnvelopeTCPAxes()
	trW, frW, cdW := echov1.EnvelopeWSAxes()
	trU, frU, cdU := echov1.EnvelopeUDPAxes()
	trH, frH, cdH := echov1.WholebodyHTTP1Axes()

	srv := server.New(
		argos.WithConfig(&demoCfg),
		argos.WithService(echoService,
			argos.ServiceListener(":9090", echov1.ServiceAxes(trG, frG, cdG)...),
			argos.ServiceListener(":7000", echov1.ServiceAxes(trT, frT, cdT)...),
			argos.ServiceListener(":8081", echov1.ServiceAxes(trW, frW, cdW)...),
			argos.ServiceListener(":7001", echov1.ServiceAxes(trU, frU, cdU)...),
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

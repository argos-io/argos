//go:build ignore

package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/argos-io/argos"
	echov1 "github.com/argos-io/argos/example/echo"
	"github.com/argos-io/argos/server"

	_ "github.com/argos-io/argos/codec/json"
	_ "github.com/argos-io/argos/codec/protobuf"
	_ "github.com/argos-io/argos/transport/grpc"
	_ "github.com/argos-io/argos/transport/httpunary"
)

const echoService = "echo.v1.EchoService"

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))

	srv := server.New(argos.WithServerOptions(&argos.Options{
		Services: map[string]argos.ServiceOptions{
			echoService: {
				Listeners: []argos.ServiceListen{
					{Address: ":9090", Transport: "grpc", Codec: "protobuf"},
					{Address: ":8080", Transport: "httpunary", Codec: "json"},
				},
			},
		},
	}))
	if err := srv.Register(echov1.EchoServiceDesc, echov1.EchoServiceHandlers(echov1.NewEchoImpl())); err != nil {
		slog.Error("Register", "err", err)
		os.Exit(1)
	}
	if err := srv.Run(context.Background()); err != nil {
		slog.Error("echo server", "err", err)
		os.Exit(1)
	}
}

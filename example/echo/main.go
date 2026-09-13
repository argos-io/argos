//go:build ignore

package main

import (
	"context"
	"log/slog"
	"os"

	jsoncodec "github.com/argos-io/argos/codec/json"
	protobufcodec "github.com/argos-io/argos/codec/protobuf"
	echov1 "github.com/argos-io/argos/example/echo"
	"github.com/argos-io/argos"
	"github.com/argos-io/argos/server"
	"github.com/argos-io/argos/transport/http1"
	"github.com/argos-io/argos/transport/http2"
	"github.com/argos-io/argos/transport/tcp"
	"github.com/argos-io/argos/transport/telnet"
	"github.com/argos-io/argos/transport/udp"
	"github.com/argos-io/argos/transport/ws"
)

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))

	ctx := context.Background()
	srv := server.New()
	impl := echov1.NewEchoImpl()

	for _, opts := range [][]argos.Option{
		{
			argos.WithTransport(http2.New()),
			argos.WithListenAddress(":9090"),
			argos.WithCodec(protobufcodec.New()),
		},
		{
			argos.WithTransport(http1.New()),
			argos.WithListenAddress(":8080"),
			argos.WithCodec(jsoncodec.New()),
		},
		{
			argos.WithTransport(ws.New()),
			argos.WithListenAddress(":8081"),
			argos.WithCodec(protobufcodec.New()),
		},
		{
			argos.WithTransport(tcp.New()),
			argos.WithListenAddress(":7000"),
			argos.WithCodec(protobufcodec.New()),
		},
		{
			argos.WithTransport(udp.New()),
			argos.WithListenAddress(":7001"),
			argos.WithCodec(protobufcodec.New()),
		},
		{
			argos.WithTransport(telnet.New()),
			argos.WithListenAddress(":2323"),
			argos.WithCodec(jsoncodec.New()),
			argos.WithFilter(echov1.ServerAuth),
		},
	} {
		service := srv.NewService(opts...)
		echov1.RegisterEchoService(service, impl)
	}

	if err := srv.Run(ctx); err != nil {
		slog.Error("echo server", "err", err)
		os.Exit(1)
	}
}

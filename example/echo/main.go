//go:build ignore

package main

import (
	"context"
	"log/slog"
	"os"

	jsoncodec "github.com/argos-io/argos/codec/json"
	protobufcodec "github.com/argos-io/argos/codec/protobuf"
	echov1 "github.com/argos-io/argos/example/echo"
	"github.com/argos-io/argos/option"
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

	for _, opts := range [][]option.Option{
		{
			option.WithTransport(http2.New()),
			option.WithListenAddress(":9090"),
			option.WithCodec(protobufcodec.New()),
		},
		{
			option.WithTransport(http1.New()),
			option.WithListenAddress(":8080"),
			option.WithCodec(jsoncodec.New()),
		},
		{
			option.WithTransport(ws.New()),
			option.WithListenAddress(":8081"),
			option.WithCodec(protobufcodec.New()),
		},
		{
			option.WithTransport(tcp.New()),
			option.WithListenAddress(":7000"),
			option.WithCodec(protobufcodec.New()),
		},
		{
			option.WithTransport(udp.New()),
			option.WithListenAddress(":7001"),
			option.WithCodec(protobufcodec.New()),
		},
		{
			option.WithTransport(telnet.New()),
			option.WithListenAddress(":2323"),
			option.WithCodec(jsoncodec.New()),
			option.WithFilter(echov1.ServerAuth),
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

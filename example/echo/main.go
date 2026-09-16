//go:build ignore

package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/argos-io/argos"
	"github.com/argos-io/argos/codec"
	echov1 "github.com/argos-io/argos/example/echo"
	"github.com/argos-io/argos/framing"
	"github.com/argos-io/argos/server"
	"github.com/argos-io/argos/transport"
)

const echoService = "echo.v1.EchoService"

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, nil)))

	trReg, frReg, coReg, err := echov1.DemoRegistries()
	if err != nil {
		slog.Error("DemoRegistries", "err", err)
		os.Exit(1)
	}

	// Datagram limits must fit the smallest carrier in the demo (envelope×udp).
	demoCfg := argos.Defaults()
	demoCfg.MaxMessageSize = 32 << 10
	demoCfg.MaxFrameSize = 65507

	grpcAxes := []argos.ServiceOption{
		argos.ServiceTransportName(transport.NameHTTP2),
		argos.ServiceFramingName(framing.NameGRPC),
		argos.ServiceCodecName(codec.NameProtobuf),
	}
	envelopeTCP := []argos.ServiceOption{
		argos.ServiceTransportName(transport.NameTCP),
		argos.ServiceFramingName(framing.NameEnvelope),
		argos.ServiceCodecName(codec.NameProtobuf),
	}
	envelopeWS := []argos.ServiceOption{
		argos.ServiceTransportName(transport.NameWS),
		argos.ServiceFramingName(framing.NameEnvelope),
		argos.ServiceCodecName(codec.NameProtobuf),
	}
	envelopeUDP := []argos.ServiceOption{
		argos.ServiceTransportName(transport.NameUDP),
		argos.ServiceFramingName(framing.NameEnvelope),
		argos.ServiceCodecName(codec.NameProtobuf),
	}
	wholebodyAxes := []argos.ServiceOption{
		argos.ServiceTransportName(transport.NameHTTP1),
		argos.ServiceFramingName(framing.NameWholebody),
		argos.ServiceCodecName(codec.NameJSON),
	}

	srv := server.New(
		argos.WithConfig(&demoCfg),
		argos.WithTransportRegistry(trReg),
		argos.WithFramingRegistry(frReg),
		argos.WithCodecRegistry(coReg),
		argos.WithService(echoService,
			argos.ServiceListener(":9090", grpcAxes...),
			argos.ServiceListener(":7000", envelopeTCP...),
			argos.ServiceListener(":8081", envelopeWS...),
			argos.ServiceListener(":7001", envelopeUDP...),
			argos.ServiceListener(":8080", wholebodyAxes...),
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

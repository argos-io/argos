package echov1

import (
	"github.com/argos-io/argos/transport/grpc"
	"github.com/argos-io/argos/transport/httpunary"
)

const (
	GRPCTransportName      = "grpc"
	HTTPUnaryTransportName = "httpunary"
	GRPCCodecName          = "protobuf"
	HTTPUnaryCodecName     = "json"
)

// GRPCTransport builds the gRPC axis the echo tests share. Session and pool
// limits are fixed when an axis is constructed and nothing else holds a second
// copy of them, so the built-in axis is what these tests want: there is no
// Options left for it to agree with.
func GRPCTransport() (*grpc.Transport, error) {
	return grpc.NewTransport()
}

func HTTPUnaryRPCTransport() (*httpunary.Transport, error) {
	return httpunary.NewTransport()
}

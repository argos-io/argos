// Package reflection registers gRPC server reflection v1 on an Argos server,
// aligned with google.golang.org/grpc/reflection.RegisterV1.
package reflection

import (
	"context"
	"errors"
	"io"

	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/filter"
	"github.com/argos-io/argos/server"
	"github.com/argos-io/argos/status"
	"github.com/argos-io/argos/stream"
	v1reflectionpb "google.golang.org/grpc/reflection/grpc_reflection_v1"
	"google.golang.org/protobuf/reflect/protoregistry"
)

const (
	// ServiceV1 is the v1 reflection service full name.
	ServiceV1 = "grpc.reflection.v1.ServerReflection"

	methodServerReflectionInfo = "ServerReflectionInfo"
)

var v1MethodInfo = ServiceV1 + "." + methodServerReflectionInfo

// Options configures Register.
type Options struct {
	// Services lists gRPC service full names exposed (ListServices).
	Services []string
	// Files is the descriptor set used to answer file/symbol queries.
	Files *protoregistry.Files
}

// Register registers grpc.reflection.v1.ServerReflection on srv.
func Register(srv *server.Server, opts Options) error {
	if opts.Files == nil {
		return status.Error(status.InvalidArgument, "reflection: nil Files")
	}
	eng := &Engine{
		Services:     append([]string(nil), opts.Services...),
		DescResolver: opts.Files,
		ExtResolver:  protoregistry.GlobalTypes,
	}
	return srv.Register(v1ServiceDesc(), map[string]filter.Handler{
		methodServerReflectionInfo: bidiHandler(eng),
	})
}

func v1ServiceDesc() descriptor.Service {
	return descriptor.MustService(ServiceV1,
		descriptor.MustMethod(v1MethodInfo, descriptor.BidiStreaming),
	)
}

func bidiHandler(eng *Engine) filter.Handler {
	return func(ctx context.Context, _ descriptor.Method, st stream.Stream) error {
		sent := make(map[string]bool)
		for {
			if err := ctx.Err(); err != nil {
				return err
			}
			var in v1reflectionpb.ServerReflectionRequest
			if err := st.Recv(&in); err != nil {
				if errors.Is(err, io.EOF) {
					return nil
				}
				return err
			}
			out, err := eng.ProcessV1(&in, sent)
			if err != nil {
				return status.Error(status.InvalidArgument, err.Error())
			}
			if err := st.Send(out); err != nil {
				return err
			}
		}
	}
}

package httpunary

import (
	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/metadata"
	"github.com/argos-io/argos/transport"
)

// PathVarMetadataPrefix is prepended to path template variable names in
// metadata (e.g. x-argos-path-id for {id}).
const PathVarMetadataPrefix = "x-argos-path-"

// Router maps between HTTP request surfaces and descriptor method full names.
type Router interface {
	BuildPreface(m descriptor.Method, codecName string, out metadata.Metadata) (transport.RequestPreface, error)
	ResolveAccept(httpMethod, requestTarget string, in metadata.Metadata) (fullName string, err error)
}

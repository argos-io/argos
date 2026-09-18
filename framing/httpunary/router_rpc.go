package httpunary

import (
	"fmt"
	"strings"

	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/framing"
	"github.com/argos-io/argos/metadata"
	"github.com/argos-io/argos/status"
	"github.com/argos-io/argos/transport"
)

type rpcRouter struct{}

// MethodPath builds the HTTP request target from a descriptor.Method:
// "/" + Service() + "/" + Name().
func MethodPath(m descriptor.Method) string {
	return "/" + m.Service() + "/" + m.Name()
}

// ParseMethodPath splits a request target of the form "/service/method".
// A query string, if present, is ignored.
func ParseMethodPath(target string) (service, method string, err error) {
	if i := strings.IndexByte(target, '?'); i >= 0 {
		target = target[:i]
	}
	if !strings.HasPrefix(target, "/") {
		return "", "", fmt.Errorf("framing/httpunary: invalid method path %q: must start with /", target)
	}
	rest := target[1:]
	pos := strings.LastIndex(rest, "/")
	if pos < 0 {
		return "", "", fmt.Errorf("framing/httpunary: invalid method path %q: missing /method", target)
	}
	service, method = rest[:pos], rest[pos+1:]
	if service == "" || method == "" {
		return "", "", fmt.Errorf("framing/httpunary: invalid method path %q: empty service or method", target)
	}
	return service, method, nil
}

func (rpcRouter) BuildPreface(m descriptor.Method, codecName string, out metadata.Metadata) (transport.RequestPreface, error) {
	hs := transport.Headers{
		{Name: "content-type", Value: ContentType(codecName)},
	}
	hs = append(hs, EncodeMetadata(out)...)
	return transport.RequestPreface{
		RequestTarget: MethodPath(m),
		Headers:       hs,
	}, nil
}

func (rpcRouter) ResolveAccept(_ string, requestTarget string, _ metadata.Metadata) (string, error) {
	svc, meth, err := ParseMethodPath(requestTarget)
	if err != nil {
		return "", fmt.Errorf("%w: %w", framing.ErrCallRejected,
			status.Error(status.InvalidArgument, err.Error()))
	}
	return svc + "." + meth, nil
}

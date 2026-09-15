package grpc

import (
	"fmt"
	"strings"

	"github.com/argos-io/argos/descriptor"
)

// MethodPath builds the gRPC HTTP/2 :path from a descriptor.Method:
// "/" + Service() + "/" + Name().
func MethodPath(m descriptor.Method) string {
	return "/" + m.Service() + "/" + m.Name()
}

// ParseMethodPath splits a gRPC :path of the form "/service/method".
// The last "/" separates service from method (grpc-go compatible).
func ParseMethodPath(path string) (service, method string, err error) {
	if !strings.HasPrefix(path, "/") {
		return "", "", fmt.Errorf("framing/grpc: invalid method path %q: must start with /", path)
	}
	rest := path[1:]
	pos := strings.LastIndex(rest, "/")
	if pos < 0 {
		return "", "", fmt.Errorf("framing/grpc: invalid method path %q: missing /method", path)
	}
	service, method = rest[:pos], rest[pos+1:]
	if service == "" || method == "" {
		return "", "", fmt.Errorf("framing/grpc: invalid method path %q: empty service or method", path)
	}
	return service, method, nil
}

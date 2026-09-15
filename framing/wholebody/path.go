package wholebody

import (
	"fmt"
	"strings"

	"github.com/argos-io/argos/descriptor"
)

// MethodPath builds the HTTP request target from a descriptor.Method:
// "/" + Service() + "/" + Name().
func MethodPath(m descriptor.Method) string {
	return "/" + m.Service() + "/" + m.Name()
}

// ParseMethodPath splits a request target of the form "/service/method".
// A query string, if present, is ignored. The last "/" separates service
// from method (same convention as framing/grpc).
func ParseMethodPath(target string) (service, method string, err error) {
	if i := strings.IndexByte(target, '?'); i >= 0 {
		target = target[:i]
	}
	if !strings.HasPrefix(target, "/") {
		return "", "", fmt.Errorf("framing/wholebody: invalid method path %q: must start with /", target)
	}
	rest := target[1:]
	pos := strings.LastIndex(rest, "/")
	if pos < 0 {
		return "", "", fmt.Errorf("framing/wholebody: invalid method path %q: missing /method", target)
	}
	service, method = rest[:pos], rest[pos+1:]
	if service == "" || method == "" {
		return "", "", fmt.Errorf("framing/wholebody: invalid method path %q: empty service or method", target)
	}
	return service, method, nil
}

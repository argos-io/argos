package httpunary

import (
	"fmt"
	"strings"

	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/internal/session"
	"github.com/argos-io/argos/metadata"
	"github.com/argos-io/argos/status"
	"github.com/argos-io/argos/transport"
)

type restRouter struct {
	byMethod map[string]Binding
}

func newRESTRouter(bindings []Binding) (restRouter, error) {
	if len(bindings) == 0 {
		return restRouter{}, fmt.Errorf("httpunary: REST requires at least one Binding")
	}
	byMethod := make(map[string]Binding, len(bindings))
	seen := make(map[string]struct{}, len(bindings))
	for _, b := range bindings {
		if b.Method.IsZero() {
			return restRouter{}, fmt.Errorf("httpunary: binding has zero Method")
		}
		verb := strings.ToUpper(strings.TrimSpace(b.Verb))
		if verb == "" {
			return restRouter{}, fmt.Errorf("httpunary: binding %q missing Verb", b.Method.FullName())
		}
		pat := normalizePattern(b.Pattern)
		if pat == "" {
			return restRouter{}, fmt.Errorf("httpunary: binding %q missing Pattern", b.Method.FullName())
		}
		key := verb + " " + pat
		if _, ok := seen[key]; ok {
			return restRouter{}, fmt.Errorf("httpunary: duplicate binding %q", key)
		}
		seen[key] = struct{}{}
		byMethod[b.Method.FullName()] = Binding{Method: b.Method, Verb: verb, Pattern: pat}
	}
	return restRouter{byMethod: byMethod}, nil
}

func normalizePattern(p string) string {
	p = strings.TrimSpace(p)
	if p == "" {
		return ""
	}
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	return p
}

func pathOnly(target string) string {
	if i := strings.IndexByte(target, '?'); i >= 0 {
		return target[:i]
	}
	return target
}

func (r restRouter) BuildPreface(m descriptor.Method, codecName string, out metadata.Metadata) (transport.RequestPreface, error) {
	b, ok := r.byMethod[m.FullName()]
	if !ok {
		return transport.RequestPreface{}, status.Error(status.Internal,
			fmt.Sprintf("httpunary: no REST binding for method %q", m.FullName()))
	}
	target, err := buildPath(b.Pattern, out)
	if err != nil {
		return transport.RequestPreface{}, err
	}
	var hs transport.Headers
	if verbAllowsBody(b.Verb) {
		hs = transport.Headers{{Name: "content-type", Value: ContentType(codecName)}}
	}
	hs = append(hs, EncodeMetadata(out)...)
	return transport.RequestPreface{
		Method:        b.Verb,
		RequestTarget: target,
		Headers:       hs,
	}, nil
}

func verbAllowsBody(verb string) bool {
	switch strings.ToUpper(verb) {
	case "GET", "HEAD", "DELETE":
		return false
	default:
		return true
	}
}

func buildPath(pattern string, out metadata.Metadata) (string, error) {
	segs := strings.Split(normalizePattern(pattern), "/")
	var b strings.Builder
	for i, seg := range segs {
		if i == 0 && seg == "" {
			continue
		}
		if !strings.HasPrefix(seg, "{") || !strings.HasSuffix(seg, "}") || len(seg) < 3 {
			b.WriteByte('/')
			b.WriteString(seg)
			continue
		}
		name := seg[1 : len(seg)-1]
		if name == "" {
			return "", status.Error(status.InvalidArgument, "httpunary: empty path variable name")
		}
		key := PathVarMetadataPrefix + name
		val := ""
		if len(out[key]) > 0 {
			val = out[key][0]
		}
		if val == "" {
			return "", status.Error(status.InvalidArgument,
				fmt.Sprintf("httpunary: missing path variable %q in metadata", name))
		}
		b.WriteByte('/')
		b.WriteString(val)
	}
	outPath := b.String()
	if outPath == "" {
		return "/", nil
	}
	return outPath, nil
}

func (r restRouter) ResolveAccept(httpMethod, requestTarget string, in metadata.Metadata) (string, error) {
	method := strings.ToUpper(strings.TrimSpace(httpMethod))
	if method == "" {
		method = "POST"
	}
	path := pathOnly(requestTarget)

	var pathMatches []Binding
	for _, b := range r.byMethod {
		if ok, vars := matchPattern(b.Pattern, path); ok {
			cp := b
			applyPathVars(in, vars)
			pathMatches = append(pathMatches, cp)
		}
	}
	if len(pathMatches) == 0 {
		return "", fmt.Errorf("%w: %w", session.ErrCallRejected,
			status.Error(status.NotFound, "httpunary: route not found"))
	}
	for _, b := range pathMatches {
		if b.Verb == method {
			return b.Method.FullName(), nil
		}
	}
	return "", fmt.Errorf("%w: %w", session.ErrCallRejected,
		status.Error(status.Unimplemented, "httpunary: http method not allowed"))
}

func applyPathVars(in metadata.Metadata, vars map[string]string) {
	for k, v := range vars {
		in[PathVarMetadataPrefix+k] = []string{v}
	}
}

func matchPattern(pattern, path string) (bool, map[string]string) {
	pSegs := strings.Split(normalizePattern(pattern), "/")
	pathSegs := strings.Split(pathOnly(path), "/")
	if len(pSegs) != len(pathSegs) {
		return false, nil
	}
	vars := make(map[string]string)
	for i := 0; i < len(pSegs); i++ {
		ps, ts := pSegs[i], pathSegs[i]
		if i == 0 && ps == "" && ts == "" {
			continue
		}
		if strings.HasPrefix(ps, "{") && strings.HasSuffix(ps, "}") {
			name := ps[1 : len(ps)-1]
			if name == "" {
				return false, nil
			}
			vars[name] = ts
			continue
		}
		if ps != ts {
			return false, nil
		}
	}
	return true, vars
}

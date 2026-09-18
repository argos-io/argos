// Package descriptor holds generated and runtime-shared call descriptions.
// Method and Service fields are unexported, so package-level vars constructed
// via MustMethod / MustService are immutable by construction.
package descriptor

import (
	"fmt"
	"strings"
)

// Shape is the RPC call pattern for a method.
type Shape uint8

const (
	Unary Shape = iota
	ServerStreaming
	ClientStreaming
	BidiStreaming
)

func (s Shape) valid() bool {
	return s <= BidiStreaming
}

// Method describes a single RPC method.
// FullName uses the protobuf full name (dot-separated), NOT the gRPC path form.
// Wire representation is left to each transport axis (e.g. transport/grpc builds "/svc/method").
type Method struct {
	fullName string
	service  string
	name     string
	shape    Shape
}

// NewMethod parses fullName as "service.name", where Name is the last dot
// segment and Service is everything before the last dot.
func NewMethod(fullName string, shape Shape) (Method, error) {
	if fullName == "" {
		return Method{}, fmt.Errorf("descriptor: empty method full name")
	}
	if !shape.valid() {
		return Method{}, fmt.Errorf("descriptor: invalid shape %d", shape)
	}
	parts := strings.Split(fullName, ".")
	if len(parts) < 2 {
		return Method{}, fmt.Errorf("descriptor: method full name %q missing service separator", fullName)
	}
	for _, seg := range parts {
		if seg == "" {
			return Method{}, fmt.Errorf("descriptor: method full name %q has empty segment", fullName)
		}
	}
	name := parts[len(parts)-1]
	service := strings.Join(parts[:len(parts)-1], ".")
	return Method{
		fullName: fullName,
		service:  service,
		name:     name,
		shape:    shape,
	}, nil
}

// MustMethod is like NewMethod but panics on error. Intended for generated code.
func MustMethod(fullName string, shape Shape) Method {
	m, err := NewMethod(fullName, shape)
	if err != nil {
		panic(err.Error())
	}
	return m
}

// FullName returns the protobuf full method name, e.g. "echo.v1.EchoService.Echo".
func (m Method) FullName() string { return m.fullName }

// Service returns the protobuf full service name, e.g. "echo.v1.EchoService".
func (m Method) Service() string { return m.service }

// Name returns the short method name, e.g. "Echo".
func (m Method) Name() string { return m.name }

// Shape returns the call pattern.
func (m Method) Shape() Shape { return m.shape }

// IsZero reports whether m is the zero Method.
func (m Method) IsZero() bool {
	return m.fullName == "" && m.service == "" && m.name == "" && m.shape == 0
}

// Service describes a named service and its methods.
type Service struct {
	fullName string
	methods  []Method
}

// NewService builds a Service whose methods all belong to fullName.
func NewService(fullName string, methods ...Method) (Service, error) {
	if fullName == "" {
		return Service{}, fmt.Errorf("descriptor: empty service full name")
	}
	seen := make(map[string]struct{}, len(methods))
	out := make([]Method, len(methods))
	for i, m := range methods {
		if m.Service() != fullName {
			return Service{}, fmt.Errorf("descriptor: method %q service %q does not match service %q",
				m.FullName(), m.Service(), fullName)
		}
		if _, ok := seen[m.Name()]; ok {
			return Service{}, fmt.Errorf("descriptor: duplicate method name %q in service %q", m.Name(), fullName)
		}
		seen[m.Name()] = struct{}{}
		out[i] = m
	}
	return Service{fullName: fullName, methods: out}, nil
}

// MustService is like NewService but panics on error. Intended for generated code.
func MustService(fullName string, methods ...Method) Service {
	s, err := NewService(fullName, methods...)
	if err != nil {
		panic(err.Error())
	}
	return s
}

// FullName returns the protobuf full service name.
func (s Service) FullName() string { return s.fullName }

// Methods returns a defensive copy of the service methods.
func (s Service) Methods() []Method {
	if len(s.methods) == 0 {
		return nil
	}
	out := make([]Method, len(s.methods))
	copy(out, s.methods)
	return out
}

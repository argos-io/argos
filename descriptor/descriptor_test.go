package descriptor_test

import (
	"reflect"
	"testing"

	"github.com/argos-io/argos/descriptor"
)

func TestNewMethodSuccess(t *testing.T) {
	m, err := descriptor.NewMethod("echo.v1.EchoService.Echo", descriptor.Unary)
	if err != nil {
		t.Fatalf("NewMethod: %v", err)
	}
	if got, want := m.FullName(), "echo.v1.EchoService.Echo"; got != want {
		t.Errorf("FullName = %q, want %q", got, want)
	}
	if got, want := m.Service(), "echo.v1.EchoService"; got != want {
		t.Errorf("Service = %q, want %q", got, want)
	}
	if got, want := m.Name(), "Echo"; got != want {
		t.Errorf("Name = %q, want %q", got, want)
	}
	if got, want := m.Shape(), descriptor.Unary; got != want {
		t.Errorf("Shape = %v, want %v", got, want)
	}
	if m.IsZero() {
		t.Error("IsZero = true, want false")
	}
}

func TestNewMethodShapes(t *testing.T) {
	shapes := []descriptor.Shape{
		descriptor.Unary,
		descriptor.ServerStreaming,
		descriptor.ClientStreaming,
		descriptor.BidiStreaming,
	}
	for _, shape := range shapes {
		m, err := descriptor.NewMethod("svc.Method", shape)
		if err != nil {
			t.Fatalf("NewMethod(shape=%d): %v", shape, err)
		}
		if m.Shape() != shape {
			t.Errorf("Shape = %v, want %v", m.Shape(), shape)
		}
	}
}

func TestNewMethodErrors(t *testing.T) {
	cases := []struct {
		name     string
		fullName string
		shape    descriptor.Shape
	}{
		{"empty", "", descriptor.Unary},
		{"no_dot", "Echo", descriptor.Unary},
		{"trailing_dot", "echo.v1.EchoService.", descriptor.Unary},
		{"leading_dot", ".Echo", descriptor.Unary},
		{"empty_segment", "echo..Echo", descriptor.Unary},
		{"only_dot", ".", descriptor.Unary},
		{"invalid_shape", "svc.Method", descriptor.Shape(99)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m, err := descriptor.NewMethod(tc.fullName, tc.shape)
			if err == nil {
				t.Fatalf("NewMethod(%q, %v) succeeded: %+v", tc.fullName, tc.shape, m)
			}
			if !m.IsZero() {
				t.Errorf("failed NewMethod returned non-zero Method")
			}
		})
	}
}

func TestMethodIsZero(t *testing.T) {
	var m descriptor.Method
	if !m.IsZero() {
		t.Error("zero Method.IsZero() = false, want true")
	}
}

func TestMustMethod(t *testing.T) {
	m := descriptor.MustMethod("svc.Method", descriptor.ClientStreaming)
	if m.FullName() != "svc.Method" || m.Shape() != descriptor.ClientStreaming {
		t.Fatalf("MustMethod returned unexpected Method: fullName=%q shape=%v", m.FullName(), m.Shape())
	}

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("MustMethod did not panic on invalid input")
		}
		msg, ok := r.(string)
		if !ok || msg == "" {
			t.Fatalf("MustMethod panic value = %#v, want non-empty string", r)
		}
	}()
	_ = descriptor.MustMethod("", descriptor.Unary)
}

func TestNewServiceSuccess(t *testing.T) {
	echo, err := descriptor.NewMethod("echo.v1.EchoService.Echo", descriptor.Unary)
	if err != nil {
		t.Fatal(err)
	}
	watch, err := descriptor.NewMethod("echo.v1.EchoService.Watch", descriptor.ServerStreaming)
	if err != nil {
		t.Fatal(err)
	}

	s, err := descriptor.NewService("echo.v1.EchoService", echo, watch)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	if got, want := s.FullName(), "echo.v1.EchoService"; got != want {
		t.Errorf("FullName = %q, want %q", got, want)
	}
	methods := s.Methods()
	if len(methods) != 2 {
		t.Fatalf("Methods len = %d, want 2", len(methods))
	}
	if methods[0].Name() != "Echo" || methods[1].Name() != "Watch" {
		t.Errorf("Methods names = [%s %s], want [Echo Watch]", methods[0].Name(), methods[1].Name())
	}
}

func TestNewServiceEmptyMethods(t *testing.T) {
	s, err := descriptor.NewService("echo.v1.EchoService")
	if err != nil {
		t.Fatalf("NewService with no methods: %v", err)
	}
	if len(s.Methods()) != 0 {
		t.Errorf("Methods len = %d, want 0", len(s.Methods()))
	}
}

func TestNewServiceErrors(t *testing.T) {
	ok, err := descriptor.NewMethod("svc.A", descriptor.Unary)
	if err != nil {
		t.Fatal(err)
	}
	other, err := descriptor.NewMethod("other.A", descriptor.Unary)
	if err != nil {
		t.Fatal(err)
	}
	dup, err := descriptor.NewMethod("svc.A", descriptor.ServerStreaming)
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name     string
		fullName string
		methods  []descriptor.Method
	}{
		{"empty_name", "", []descriptor.Method{ok}},
		{"wrong_service", "svc", []descriptor.Method{other}},
		{"duplicate_name", "svc", []descriptor.Method{ok, dup}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := descriptor.NewService(tc.fullName, tc.methods...)
			if err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestServiceMethodsCopyIsolation(t *testing.T) {
	m, err := descriptor.NewMethod("svc.A", descriptor.Unary)
	if err != nil {
		t.Fatal(err)
	}
	s, err := descriptor.NewService("svc", m)
	if err != nil {
		t.Fatal(err)
	}

	got := s.Methods()
	got[0] = descriptor.Method{}
	again := s.Methods()
	if again[0].IsZero() {
		t.Fatal("mutating Methods() slice mutated Service")
	}
	if again[0].Name() != "A" {
		t.Errorf("Methods()[0].Name = %q, want A", again[0].Name())
	}
}

func TestMustService(t *testing.T) {
	m := descriptor.MustMethod("svc.A", descriptor.Unary)
	s := descriptor.MustService("svc", m)
	if s.FullName() != "svc" || len(s.Methods()) != 1 {
		t.Fatalf("MustService returned unexpected Service: fullName=%q methods=%d", s.FullName(), len(s.Methods()))
	}

	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("MustService did not panic on invalid input")
		}
		msg, ok := r.(string)
		if !ok || msg == "" {
			t.Fatalf("MustService panic value = %#v, want non-empty string", r)
		}
	}()
	_ = descriptor.MustService("")
}

func TestNoExportedFields(t *testing.T) {
	for _, typ := range []reflect.Type{
		reflect.TypeOf(descriptor.Method{}),
		reflect.TypeOf(descriptor.Service{}),
	} {
		for i := 0; i < typ.NumField(); i++ {
			f := typ.Field(i)
			if f.PkgPath == "" {
				t.Errorf("%s.%s is exported; §3.1-14 requires unexported fields", typ.Name(), f.Name)
			}
		}
	}
}

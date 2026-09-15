package status_test

import (
	"errors"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/argos-io/argos/status"
	"google.golang.org/grpc/codes"
)

func TestCodeValuesMatchGRPC(t *testing.T) {
	cases := []struct {
		name string
		got  status.Code
		want codes.Code
	}{
		{"OK", status.OK, codes.OK},
		{"Canceled", status.Canceled, codes.Canceled},
		{"Unknown", status.Unknown, codes.Unknown},
		{"InvalidArgument", status.InvalidArgument, codes.InvalidArgument},
		{"DeadlineExceeded", status.DeadlineExceeded, codes.DeadlineExceeded},
		{"NotFound", status.NotFound, codes.NotFound},
		{"AlreadyExists", status.AlreadyExists, codes.AlreadyExists},
		{"PermissionDenied", status.PermissionDenied, codes.PermissionDenied},
		{"ResourceExhausted", status.ResourceExhausted, codes.ResourceExhausted},
		{"FailedPrecondition", status.FailedPrecondition, codes.FailedPrecondition},
		{"Aborted", status.Aborted, codes.Aborted},
		{"OutOfRange", status.OutOfRange, codes.OutOfRange},
		{"Unimplemented", status.Unimplemented, codes.Unimplemented},
		{"Internal", status.Internal, codes.Internal},
		{"Unavailable", status.Unavailable, codes.Unavailable},
		{"DataLoss", status.DataLoss, codes.DataLoss},
		{"Unauthenticated", status.Unauthenticated, codes.Unauthenticated},
	}
	if len(cases) != 17 {
		t.Fatalf("want 17 codes, got %d", len(cases))
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if uint32(tc.got) != uint32(tc.want) {
				t.Fatalf("status.%s = %d, grpc codes.%s = %d", tc.name, tc.got, tc.name, tc.want)
			}
		})
	}
}

func TestNoProtobufImport(t *testing.T) {
	out, err := exec.Command("go", "list", "-f", "{{.Imports}}", "github.com/argos-io/argos/status").CombinedOutput()
	if err != nil {
		t.Fatalf("go list: %v\n%s", err, out)
	}
	imports := string(out)
	forbidden := []string{
		"google.golang.org/protobuf",
		"google.golang.org/genproto",
		"github.com/golang/protobuf",
	}
	for _, f := range forbidden {
		if strings.Contains(imports, f) {
			t.Fatalf("status must not import %q; go list Imports = %s", f, imports)
		}
	}

	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	// Test file lives in status/; scan only non-test production sources.
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		path := filepath.Join(dir, name)
		f, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		if f.Name.Name != "status" {
			t.Fatalf("%s: package name = %q, want status", path, f.Name.Name)
		}
		for _, imp := range f.Imports {
			path := strings.Trim(imp.Path.Value, `"`)
			for _, forb := range forbidden {
				if path == forb || strings.HasPrefix(path, forb+"/") {
					t.Fatalf("%s imports forbidden %q", name, path)
				}
			}
		}
	}
}

func TestCodeOf(t *testing.T) {
	if status.CodeOf(nil) != status.OK {
		t.Fatalf("CodeOf(nil) = %v, want OK", status.CodeOf(nil))
	}
	err := status.Error(status.NotFound, "no such echo")
	if status.CodeOf(err) != status.NotFound {
		t.Fatalf("got %v", status.CodeOf(err))
	}
	if status.CodeOf(errors.New("x")) != status.Unknown {
		t.Fatal("plain error must be Unknown")
	}
	wrapped := errors.Join(errors.New("outer"), err)
	if status.CodeOf(wrapped) != status.NotFound {
		t.Fatalf("CodeOf(wrapped) = %v, want NotFound", status.CodeOf(wrapped))
	}
}

func TestErrorMessage(t *testing.T) {
	err := status.Error(status.Internal, "boom")
	if err.Error() != "boom" {
		t.Fatalf("Error() = %q", err.Error())
	}
}

func TestErrorAs(t *testing.T) {
	err := status.Error(status.Aborted, "stop")
	var se *status.StatusError
	if !errors.As(err, &se) {
		t.Fatal("errors.As to *status.StatusError failed")
	}
	if se.Code() != status.Aborted {
		t.Fatalf("Code() = %v", se.Code())
	}
	if se.Message() != "stop" {
		t.Fatalf("Message() = %q", se.Message())
	}
}

func TestErrorIsSameCode(t *testing.T) {
	a := status.Error(status.NotFound, "a")
	b := status.Error(status.NotFound, "b")
	if !errors.Is(a, b) {
		t.Fatal("same code errors should match with errors.Is")
	}
	if errors.Is(a, status.Error(status.Unauthenticated, "c")) {
		t.Fatal("different code errors must not match")
	}
	if errors.Is(a, errors.New("plain")) {
		t.Fatal("plain error must not match coded error")
	}
}

func TestErrCardinality(t *testing.T) {
	if !errors.Is(status.ErrCardinality, status.ErrCardinality) {
		t.Fatal("ErrCardinality must match itself")
	}
	if status.CodeOf(status.ErrCardinality) != status.Internal {
		t.Fatalf("CodeOf(ErrCardinality) = %v, want Internal", status.CodeOf(status.ErrCardinality))
	}
	plain := status.Error(status.Internal, "x")
	if errors.Is(plain, status.ErrCardinality) {
		t.Fatal("plain Internal must not match ErrCardinality")
	}
	if errors.Is(status.ErrCardinality, plain) {
		t.Fatal("ErrCardinality must not match plain Internal")
	}
	wrapped := errors.Join(errors.New("wrap"), status.ErrCardinality)
	if !errors.Is(wrapped, status.ErrCardinality) {
		t.Fatal("wrapped ErrCardinality must still match")
	}
	if status.CodeOf(wrapped) != status.Internal {
		t.Fatalf("CodeOf(wrapped ErrCardinality) = %v", status.CodeOf(wrapped))
	}
}

func TestExhaustedSentinels(t *testing.T) {
	if status.CodeOf(status.ErrSessionsExhausted) != status.ResourceExhausted {
		t.Fatalf("CodeOf(ErrSessionsExhausted) = %v", status.CodeOf(status.ErrSessionsExhausted))
	}
	if status.CodeOf(status.ErrCallsExhausted) != status.ResourceExhausted {
		t.Fatalf("CodeOf(ErrCallsExhausted) = %v", status.CodeOf(status.ErrCallsExhausted))
	}
	if errors.Is(status.ErrSessionsExhausted, status.ErrCallsExhausted) {
		t.Fatal("sessions and calls exhausted must be distinguishable")
	}
	if errors.Is(status.ErrCallsExhausted, status.ErrSessionsExhausted) {
		t.Fatal("calls and sessions exhausted must be distinguishable")
	}
	plain := status.Error(status.ResourceExhausted, "x")
	if errors.Is(plain, status.ErrSessionsExhausted) || errors.Is(status.ErrSessionsExhausted, plain) {
		t.Fatal("plain ResourceExhausted must not match ErrSessionsExhausted")
	}
	if errors.Is(plain, status.ErrCallsExhausted) || errors.Is(status.ErrCallsExhausted, plain) {
		t.Fatal("plain ResourceExhausted must not match ErrCallsExhausted")
	}
	if !errors.Is(status.ErrSessionsExhausted, status.ErrSessionsExhausted) {
		t.Fatal("ErrSessionsExhausted must match itself")
	}
	if !errors.Is(status.ErrCallsExhausted, status.ErrCallsExhausted) {
		t.Fatal("ErrCallsExhausted must match itself")
	}
}

func TestWithDetailsNil(t *testing.T) {
	err := status.WithDetails(nil, status.Detail{TypeURL: "t", Value: []byte("v")})
	if err == nil {
		t.Fatal("WithDetails(nil) must return an error")
	}
	if status.CodeOf(err) == status.OK {
		t.Fatal("WithDetails(nil) must not yield OK")
	}
}

func TestDetailsDeepCopy(t *testing.T) {
	val := []byte("payload")
	base := status.Error(status.Internal, "boom")
	with := status.WithDetails(base, status.Detail{TypeURL: "type.googleapis.com/x", Value: val})

	val[0] = 'X'
	got := status.DetailsOf(with)
	if len(got) != 1 {
		t.Fatalf("len(DetailsOf) = %d", len(got))
	}
	if string(got[0].Value) != "payload" {
		t.Fatalf("WithDetails did not deep-copy Value: %q", got[0].Value)
	}

	got[0].Value[0] = 'Y'
	got2 := status.DetailsOf(with)
	if string(got2[0].Value) != "payload" {
		t.Fatalf("DetailsOf did not deep-copy Value: %q", got2[0].Value)
	}
}

func TestWithDetailsPreservesSentinel(t *testing.T) {
	with := status.WithDetails(status.ErrCardinality, status.Detail{TypeURL: "t", Value: []byte("v")})
	if !errors.Is(with, status.ErrCardinality) {
		t.Fatal("WithDetails must preserve ErrCardinality for errors.Is")
	}
	if status.CodeOf(with) != status.Internal {
		t.Fatalf("CodeOf = %v", status.CodeOf(with))
	}
	if len(status.DetailsOf(with)) != 1 {
		t.Fatal("details missing")
	}
}

func TestPackageName(t *testing.T) {
	// Compile-time: imported as status. Also assert via source in TestNoProtobufImport.
	_ = status.OK
}

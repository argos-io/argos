package status

import (
	"errors"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
)

func TestCodeValuesMatchGRPC(t *testing.T) {
	cases := []struct {
		name string
		got  Code
		want codes.Code
	}{
		{"OK", OK, codes.OK},
		{"Canceled", Canceled, codes.Canceled},
		{"Unknown", Unknown, codes.Unknown},
		{"InvalidArgument", InvalidArgument, codes.InvalidArgument},
		{"DeadlineExceeded", DeadlineExceeded, codes.DeadlineExceeded},
		{"NotFound", NotFound, codes.NotFound},
		{"AlreadyExists", AlreadyExists, codes.AlreadyExists},
		{"PermissionDenied", PermissionDenied, codes.PermissionDenied},
		{"ResourceExhausted", ResourceExhausted, codes.ResourceExhausted},
		{"FailedPrecondition", FailedPrecondition, codes.FailedPrecondition},
		{"Aborted", Aborted, codes.Aborted},
		{"OutOfRange", OutOfRange, codes.OutOfRange},
		{"Unimplemented", Unimplemented, codes.Unimplemented},
		{"Internal", Internal, codes.Internal},
		{"Unavailable", Unavailable, codes.Unavailable},
		{"DataLoss", DataLoss, codes.DataLoss},
		{"Unauthenticated", Unauthenticated, codes.Unauthenticated},
	}
	if len(cases) != 17 {
		t.Fatalf("want 17 codes, got %d", len(cases))
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if uint32(tc.got) != uint32(tc.want) {
				t.Fatalf("%s = %d, grpc codes.%s = %d", tc.name, tc.got, tc.name, tc.want)
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
	if CodeOf(nil) != OK {
		t.Fatalf("CodeOf(nil) = %v, want OK", CodeOf(nil))
	}
	err := Error(NotFound, "no such echo")
	if CodeOf(err) != NotFound {
		t.Fatalf("got %v", CodeOf(err))
	}
	if CodeOf(errors.New("x")) != Unknown {
		t.Fatal("plain error must be Unknown")
	}
	wrapped := errors.Join(errors.New("outer"), err)
	if CodeOf(wrapped) != NotFound {
		t.Fatalf("CodeOf(wrapped) = %v, want NotFound", CodeOf(wrapped))
	}
}

func TestErrorMessage(t *testing.T) {
	err := Error(Internal, "boom")
	if err.Error() != "boom" {
		t.Fatalf("Error() = %q", err.Error())
	}
}

func TestErrorAs(t *testing.T) {
	err := Error(Aborted, "stop")
	var se *StatusError
	if !errors.As(err, &se) {
		t.Fatal("errors.As to *StatusError failed")
	}
	if se.Code() != Aborted {
		t.Fatalf("Code() = %v", se.Code())
	}
	if se.Message() != "stop" {
		t.Fatalf("Message() = %q", se.Message())
	}
}

func TestErrorIsSameCode(t *testing.T) {
	a := Error(NotFound, "a")
	b := Error(NotFound, "b")
	if !errors.Is(a, b) {
		t.Fatal("same code errors should match with errors.Is")
	}
	if errors.Is(a, Error(Unauthenticated, "c")) {
		t.Fatal("different code errors must not match")
	}
	if errors.Is(a, errors.New("plain")) {
		t.Fatal("plain error must not match coded error")
	}
}

func TestErrCardinality(t *testing.T) {
	if !errors.Is(ErrCardinality, ErrCardinality) {
		t.Fatal("ErrCardinality must match itself")
	}
	if CodeOf(ErrCardinality) != Internal {
		t.Fatalf("CodeOf(ErrCardinality) = %v, want Internal", CodeOf(ErrCardinality))
	}
	plain := Error(Internal, "x")
	if errors.Is(plain, ErrCardinality) {
		t.Fatal("plain Internal must not match ErrCardinality")
	}
	if errors.Is(ErrCardinality, plain) {
		t.Fatal("ErrCardinality must not match plain Internal")
	}
	wrapped := errors.Join(errors.New("wrap"), ErrCardinality)
	if !errors.Is(wrapped, ErrCardinality) {
		t.Fatal("wrapped ErrCardinality must still match")
	}
	if CodeOf(wrapped) != Internal {
		t.Fatalf("CodeOf(wrapped ErrCardinality) = %v", CodeOf(wrapped))
	}
}

func TestExhaustedSentinels(t *testing.T) {
	if CodeOf(ErrSessionsExhausted) != ResourceExhausted {
		t.Fatalf("CodeOf(ErrSessionsExhausted) = %v", CodeOf(ErrSessionsExhausted))
	}
	if CodeOf(ErrCallsExhausted) != ResourceExhausted {
		t.Fatalf("CodeOf(ErrCallsExhausted) = %v", CodeOf(ErrCallsExhausted))
	}
	if errors.Is(ErrSessionsExhausted, ErrCallsExhausted) {
		t.Fatal("sessions and calls exhausted must be distinguishable")
	}
	if errors.Is(ErrCallsExhausted, ErrSessionsExhausted) {
		t.Fatal("calls and sessions exhausted must be distinguishable")
	}
	plain := Error(ResourceExhausted, "x")
	if errors.Is(plain, ErrSessionsExhausted) || errors.Is(ErrSessionsExhausted, plain) {
		t.Fatal("plain ResourceExhausted must not match ErrSessionsExhausted")
	}
	if errors.Is(plain, ErrCallsExhausted) || errors.Is(ErrCallsExhausted, plain) {
		t.Fatal("plain ResourceExhausted must not match ErrCallsExhausted")
	}
	if !errors.Is(ErrSessionsExhausted, ErrSessionsExhausted) {
		t.Fatal("ErrSessionsExhausted must match itself")
	}
	if !errors.Is(ErrCallsExhausted, ErrCallsExhausted) {
		t.Fatal("ErrCallsExhausted must match itself")
	}
}

func TestWithDetailsNil(t *testing.T) {
	err := WithDetails(nil, Detail{TypeURL: "t", Value: []byte("v")})
	if err == nil {
		t.Fatal("WithDetails(nil) must return an error")
	}
	if CodeOf(err) == OK {
		t.Fatal("WithDetails(nil) must not yield OK")
	}
}

func TestDetailsDeepCopy(t *testing.T) {
	val := []byte("payload")
	base := Error(Internal, "boom")
	with := WithDetails(base, Detail{TypeURL: "type.googleapis.com/x", Value: val})

	val[0] = 'X'
	got := DetailsOf(with)
	if len(got) != 1 {
		t.Fatalf("len(DetailsOf) = %d", len(got))
	}
	if string(got[0].Value) != "payload" {
		t.Fatalf("WithDetails did not deep-copy Value: %q", got[0].Value)
	}

	got[0].Value[0] = 'Y'
	got2 := DetailsOf(with)
	if string(got2[0].Value) != "payload" {
		t.Fatalf("DetailsOf did not deep-copy Value: %q", got2[0].Value)
	}
}

func TestWithDetailsPreservesSentinel(t *testing.T) {
	with := WithDetails(ErrCardinality, Detail{TypeURL: "t", Value: []byte("v")})
	if !errors.Is(with, ErrCardinality) {
		t.Fatal("WithDetails must preserve ErrCardinality for errors.Is")
	}
	if CodeOf(with) != Internal {
		t.Fatalf("CodeOf = %v", CodeOf(with))
	}
	if len(DetailsOf(with)) != 1 {
		t.Fatal("details missing")
	}
}

func TestPackageName(t *testing.T) {
	// Compile-time: imported as  Also assert via source in TestNoProtobufImport.
	_ = OK
}

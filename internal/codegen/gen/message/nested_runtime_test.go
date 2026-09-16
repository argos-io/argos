package message_test

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/argos-io/argos/internal/codegen/frontend/proto"
	"github.com/argos-io/argos/internal/codegen/gen/message"
	"github.com/argos-io/argos/internal/codegen/gogenrun"
)

// nestedCheckMain asserts the runtime descriptor binding of every generated
// type. Source-text assertions cannot see a mis-ordered MessageInfos slice:
// only running the protobuf runtime does.
const nestedCheckMain = `package main

import (
	"fmt"
	"os"

	pb "%s"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}

func check(goName string, got, want string) {
	if got != want {
		fail("%%s bound to descriptor %%q, want %%q", goName, got, want)
	}
}

func main() {
	check("Alpha", string((&pb.Alpha{}).ProtoReflect().Descriptor().FullName()), "nested.v1.Alpha")
	check("Omega", string((&pb.Omega{}).ProtoReflect().Descriptor().FullName()), "nested.v1.Omega")
	check("Alpha_Beta", string((&pb.Alpha_Beta{}).ProtoReflect().Descriptor().FullName()), "nested.v1.Alpha.Beta")
	check("Alpha_Beta_Gamma", string((&pb.Alpha_Beta_Gamma{}).ProtoReflect().Descriptor().FullName()), "nested.v1.Alpha.Beta.Gamma")
	check("Alpha_Kind", string(pb.Alpha_Kind(0).Descriptor().FullName()), "nested.v1.Alpha.Kind")
	check("TopKind", string(pb.TopKind(0).Descriptor().FullName()), "nested.v1.TopKind")

	// A map field depends on its entry, and the entry depends on the value
	// type. Both are separate depIdxs rows, and a missing one shifts every
	// following row, so the value descriptor would resolve to whatever the
	// previous row named instead.
	md := (&pb.Alpha{}).ProtoReflect().Descriptor()
	betas := md.Fields().ByName("betas")
	if betas == nil || !betas.IsMap() {
		fail("Alpha.betas is not bound to a map field: %%v", betas)
	}
	betasValue := betas.MapValue()
	if betasValue == nil || betasValue.Kind() != protoreflect.MessageKind {
		fail("Alpha.betas map value kind = %%v, want %%v", betasValue.Kind(), protoreflect.MessageKind)
	}
	check("Alpha.betas map value", string(betasValue.Message().FullName()), "nested.v1.Alpha.Beta")
	check("Alpha.betas map key", betas.MapKey().Kind().String(), "string")
	check("Alpha.plain map value", md.Fields().ByName("plain").MapValue().Kind().String(), "int64")
	kindsValue := md.Fields().ByName("kinds").MapValue()
	if kindsValue == nil || kindsValue.Kind() != protoreflect.EnumKind {
		fail("Alpha.kinds map value kind = %%v, want %%v", kindsValue.Kind(), protoreflect.EnumKind)
	}
	check("Alpha.kinds map value", string(kindsValue.Enum().FullName()), "nested.v1.Alpha.Kind")

	// Wire round trip through the generated types.
	in := &pb.Alpha{
		Beta:  &pb.Alpha_Beta{Gamma: &pb.Alpha_Beta_Gamma{G: 7}, B: 3},
		Kind:  pb.Alpha_KIND_ONE,
		Top:   pb.TopKind_TOP_KIND_READY,
		Betas: map[string]*pb.Alpha_Beta{"k": {B: 9}},
		Plain: map[int32]int64{5: 11},
		Kinds: map[string]pb.Alpha_Kind{"k": pb.Alpha_KIND_ONE},
	}
	raw, err := proto.Marshal(in)
	if err != nil {
		fail("marshal: %%v", err)
	}
	if len(raw) == 0 {
		fail("marshal produced no bytes; Alpha is bound to the wrong descriptor")
	}
	var out pb.Alpha
	if err := proto.Unmarshal(raw, &out); err != nil {
		fail("unmarshal: %%v", err)
	}
	if !proto.Equal(in, &out) {
		fail("round trip mismatch:\n in=%%v\nout=%%v", in, &out)
	}

	// protojson resolves map value types through descriptors, so it catches a
	// wrong map-entry value dependency that the wire format cannot.
	js, err := protojson.Marshal(in)
	if err != nil {
		fail("protojson marshal: %%v", err)
	}
	var back pb.Alpha
	if err := protojson.Unmarshal(js, &back); err != nil {
		fail("protojson unmarshal: %%v", err)
	}
	if !proto.Equal(in, &back) {
		fail("protojson round trip mismatch:\n in=%%v\nout=%%v", in, &back)
	}
	fmt.Println("OK")
}
`

// TestGeneratedNestedDescriptorsBindAtRuntime generates nested.proto, compiles
// it together with a checker and runs it. It is the regression test for the
// flattened-descriptor-order bug: a mis-ordered MessageInfos slice silently
// binds a Go type to a different message, which makes proto.Marshal return
// zero bytes without an error.
func TestGeneratedNestedDescriptorsBindAtRuntime(t *testing.T) {
	if testing.Short() {
		t.Skip("compiles and runs generated code")
	}
	root, err := filepath.Abs(filepath.Join("..", "..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}

	files, err := proto.Frontend{ImportPaths: []string{filepath.Join("testdata")}}.Parse(
		t.Context(),
		[]string{"nested.proto"},
	)
	if err != nil {
		t.Fatalf("parse nested proto: %v", err)
	}
	generated, err := message.Generate(files[0])
	if err != nil {
		t.Fatalf("generate nested messages: %v", err)
	}

	// The scratch package lives inside the repository module so it resolves
	// through the same go.mod/go.sum. A leading dot keeps it out of ./... .
	dir := gogenrun.DirName("nested-runtime")
	if err := gogenrun.WriteFiles(root, dir, map[string]string{
		"nestedv1/nested.pb.go": string(generated),
		"main.go":               fmt.Sprintf(nestedCheckMain, "github.com/argos-io/argos/"+dir+"/nestedv1"),
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = gogenrun.Remove(root, dir) })

	out, err := gogenrun.Run(root, dir)
	if err != nil {
		t.Fatalf("running generated code failed: %v\n%s", err, out)
	}
	if got := string(out); got != "OK\n" {
		t.Fatalf("generated checker output = %q, want %q", got, "OK\n")
	}
}

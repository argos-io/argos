package stubgen_test

import (
	"go/parser"
	"go/token"
	"strings"
	"testing"

	"github.com/argos-io/argos/internal/codegen/gen/stubgen"
	"github.com/argos-io/argos/internal/codegen/ir"
)

func TestGenerateAllStreamingShapes(t *testing.T) {
	file := ir.File{
		GoPackage: "sample",
		InputBase: "sample",
		Services: []ir.Service{{
			GoName: "ChatService",
			Methods: []ir.Method{
				{GoName: "Unary", InputType: "Request", OutputType: "Response"},
				{GoName: "Watch", InputType: "Request", OutputType: "Event", ServerStream: true},
				{GoName: "Collect", InputType: "Request", OutputType: "Response", ClientStream: true},
				{GoName: "Chat", InputType: "Request", OutputType: "Event", ClientStream: true, ServerStream: true},
			},
		}},
	}

	got, err := stubgen.Generate(file)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if _, err := parser.ParseFile(token.NewFileSet(), "sample.argos.go", got, parser.AllErrors); err != nil {
		t.Fatalf("generated source is not valid Go: %v", err)
	}

	source := string(got)
	for _, want := range []string{
		"Watch(context.Context, *Request, ChatService_WatchServer) error",
		"Collect(context.Context, ChatService_CollectServer) error",
		"Chat(context.Context, ChatService_ChatServer) error",
		"Recv() (*Request, error)",
		"Send(*Event) error",
		"OpenStream(ctx, chatCollect, stream.CallClientStreaming)",
		"OpenStream(ctx, chatChat, stream.CallBidiStreaming)",
		"server.MethodInfo{Method: chatWatch, Kind: stream.CallServerStreaming}",
	} {
		if !strings.Contains(source, want) {
			t.Fatalf("generated source missing %q\n%s", want, source)
		}
	}
}

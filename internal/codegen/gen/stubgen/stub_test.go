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
		GoPackage:    "sample",
		ProtoPackage: "sample.v1",
		InputBase:    "sample",
		Services: []ir.Service{{
			GoName:   "ChatService",
			FullName: "sample.v1.ChatService",
			Methods: []ir.Method{
				{GoName: "Unary", FullName: "sample.v1.ChatService.Unary", InputType: "Request", OutputType: "Response"},
				{GoName: "Watch", FullName: "sample.v1.ChatService.Watch", InputType: "Request", OutputType: "Event", ServerStream: true},
				{GoName: "Collect", FullName: "sample.v1.ChatService.Collect", InputType: "Request", OutputType: "Response", ClientStream: true},
				{GoName: "Chat", FullName: "sample.v1.ChatService.Chat", InputType: "Request", OutputType: "Event", ClientStream: true, ServerStream: true},
			},
		}},
	}

	got, err := stubgen.Generate(file)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if _, err := parser.ParseFile(token.NewFileSet(), "sample.argos.go", got, parser.AllErrors); err != nil {
		t.Fatalf("generated source is not valid Go: %v\n%s", err, got)
	}

	source := string(got)
	for _, want := range []string{
		"ChatService_Unary",
		"descriptor.MustMethod(",
		"ChatServiceDesc",
		"descriptor.MustService(",
		"descriptor.Unary",
		"descriptor.ServerStreaming",
		"descriptor.ClientStreaming",
		"descriptor.BidiStreaming",
		"Watch(context.Context, *Request, ChatService_WatchServer) error",
		"Collect(context.Context, ChatService_CollectServer) error",
		"Chat(context.Context, ChatService_ChatServer) error",
		"RegisterChatService(s *server.Server, impl ChatServiceServer) error",
		"s.Register(ChatServiceDesc, map[string]filter.Handler{",
		`Open(ctx, ChatService_Unary)`,
		`Open(ctx, ChatService_Collect)`,
		`Open(ctx, ChatService_Chat)`,
		"status.ErrCardinality",
		"errors.Is(err, stream.ErrSendClosed)",
		"errors.Is(err, io.EOF)",
		"Header() (metadata.Metadata, error)",
		"Trailer() metadata.Metadata",
		"NewChatServiceClient(c *client.Client)",
	} {
		if !strings.Contains(source, want) {
			t.Fatalf("generated source missing %q\n%s", want, source)
		}
	}
	for _, unwanted := range []string{
		"switch method",
		"case ChatService_",
		"errs.",
		"argos.Option",
		"OpenStream(",
		"server.Service",
		"svc.Register(",
		"const (",
	} {
		if strings.Contains(source, unwanted) {
			t.Fatalf("generated source still contains %q\n%s", unwanted, source)
		}
	}
}

func TestGenerateSameMethodNameAcrossServicesCompiles(t *testing.T) {
	file := ir.File{
		GoPackage:    "sample",
		ProtoPackage: "sample.v1",
		InputBase:    "sample",
		Services: []ir.Service{
			{
				GoName:   "AlphaService",
				FullName: "sample.v1.AlphaService",
				Methods: []ir.Method{{
					GoName: "Echo", FullName: "sample.v1.AlphaService.Echo",
					InputType: "Request", OutputType: "Response",
				}},
			},
			{
				GoName:   "BetaService",
				FullName: "sample.v1.BetaService",
				Methods: []ir.Method{{
					GoName: "Echo", FullName: "sample.v1.BetaService.Echo",
					InputType: "Request", OutputType: "Response",
				}},
			},
		},
	}

	got, err := stubgen.Generate(file)
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if _, err := parser.ParseFile(token.NewFileSet(), "sample.argos.go", got, parser.AllErrors); err != nil {
		t.Fatalf("generated source is not valid Go: %v\n%s", err, got)
	}
	source := string(got)
	for _, want := range []string{
		"AlphaService_Echo",
		"BetaService_Echo",
		"AlphaServiceDesc",
		"BetaServiceDesc",
		"RegisterAlphaService",
		"RegisterBetaService",
	} {
		if !strings.Contains(source, want) {
			t.Fatalf("generated source missing %q\n%s", want, source)
		}
	}
}

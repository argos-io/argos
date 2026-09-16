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
		"NewChatServiceClient(opts ...argos.ClientOption) (ChatServiceClient, error)",
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

// The constructor carries the service name so a caller never has to repeat it,
// and it owns the Client it built, which Close has to reach.
func TestGenerateClientConstructorCarriesServiceName(t *testing.T) {
	got, err := stubgen.Generate(oneUnaryService("OwnerService", "Do"))
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if _, err := parser.ParseFile(token.NewFileSet(), "sample.argos.go", got, parser.AllErrors); err != nil {
		t.Fatalf("generated source is not valid Go: %v\n%s", err, got)
	}

	source := string(got)
	for _, want := range []string{
		`"github.com/argos-io/argos"`,
		`argos.WithServiceName("sample.v1.OwnerService")`,
		"client.New(append([]argos.ClientOption{",
		"return &ownerServiceClient{c: c}, nil",
		"func (c *ownerServiceClient) Close() error {",
		"return c.c.Close()",
	} {
		if !strings.Contains(source, want) {
			t.Fatalf("generated source missing %q\n%s", want, source)
		}
	}
	// The interface has to expose Close, or the Client the constructor built is
	// unreachable through the value it returns.
	iface := source[strings.Index(source, "type OwnerServiceClient interface {"):]
	iface = iface[:strings.Index(iface, "}")]
	if !strings.Contains(iface, "Close() error") {
		t.Fatalf("OwnerServiceClient does not declare Close:\n%s", iface)
	}
}

// An RPC named Close would be declared twice with two signatures, in the
// interface and on the struct. The generator must say so instead of writing a
// file that no compiler accepts.
func TestGenerateRejectsRPCNamedClose(t *testing.T) {
	_, err := stubgen.Generate(oneUnaryService("LifecycleService", "Close"))
	if err == nil {
		t.Fatal("Generate accepted an RPC named Close")
	}
	for _, want := range []string{"sample.v1.LifecycleService", "Close", "LifecycleServiceClient"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q does not name %q", err, want)
		}
	}
}

// The constructor passes an appended option slice as a variadic and the client
// half pulls in an import the server half never names. Neither is visible to a
// source-text assertion: only the type checker rejects them.
func TestGeneratedClientConstructorsCompile(t *testing.T) {
	if testing.Short() {
		t.Skip("compiles generated code")
	}
	compileStub(t, oneUnaryService("OwnerService", "Do"), `	if c, err := sample.NewOwnerServiceClient(); err == nil {
		if err := c.Close(); err != nil {
			fmt.Println("Close:", err)
			return
		}
	}
	fmt.Println("ok")
`)
}

func oneUnaryService(serviceGoName, methodGoName string) ir.File {
	return ir.File{
		GoPackage:    "sample",
		ProtoPackage: "sample.v1",
		InputBase:    "sample",
		Services: []ir.Service{{
			GoName:   serviceGoName,
			FullName: "sample.v1." + serviceGoName,
			Methods: []ir.Method{{
				GoName:    methodGoName,
				FullName:  "sample.v1." + serviceGoName + "." + methodGoName,
				InputType: "Request", OutputType: "Response",
			}},
		}},
	}
}

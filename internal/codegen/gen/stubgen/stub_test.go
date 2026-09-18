package stubgen

import (
	"go/parser"
	"go/token"
	"strings"
	"testing"

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

	got, err := Generate(file)
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
		"ChatServiceHandlers(impl ChatServiceServer) map[string]filter.Handler",
		"return map[string]filter.Handler{",
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
		// The streaming wrapper's Close (c.call.Close) is not the client-level
		// Close the stub no longer emits.
		"c.c.Close()",
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

	got, err := Generate(file)
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
		"AlphaServiceHandlers",
		"BetaServiceHandlers",
	} {
		if !strings.Contains(source, want) {
			t.Fatalf("generated source missing %q\n%s", want, source)
		}
	}
}

// The constructor carries the service name so a caller never has to repeat it.
// It also hands back a plain handle over the caller-owned transport axis: it
// owns no connections, so the stub exposes no client-level Close.
func TestGenerateClientConstructorCarriesServiceName(t *testing.T) {
	got, err := Generate(oneUnaryService("OwnerService", "Do"))
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
	} {
		if !strings.Contains(source, want) {
			t.Fatalf("generated source missing %q\n%s", want, source)
		}
	}
	// The client interface must not declare Close: the value the constructor
	// returns holds nothing to release.
	iface := source[strings.Index(source, "type OwnerServiceClient interface {"):]
	iface = iface[:strings.Index(iface, "}")]
	if strings.Contains(iface, "Close") {
		t.Fatalf("OwnerServiceClient still declares Close:\n%s", iface)
	}
	for _, unwanted := range []string{
		"c.c.Close()",
		"func (c *ownerServiceClient) Close()",
		"releasing its session pool",
	} {
		if strings.Contains(source, unwanted) {
			t.Fatalf("generated source still contains %q\n%s", unwanted, source)
		}
	}
}

// Nothing in any shape claims the name Close for the client the constructor
// returns: the client interface declares only the RPCs. The streaming wrapper's
// Close (c.call.Close) lives on <Service>_<Method>Client, a different type, so
// an RPC named Close is accepted and compiles.
func TestGenerateAcceptsRPCNamedCloseForEveryShape(t *testing.T) {
	if testing.Short() {
		t.Skip("compiles generated code")
	}
	for _, shape := range []struct {
		name         string
		clientStream bool
		serverStream bool
	}{
		{"unary", false, false},
		{"server_stream", false, true},
		{"client_stream", true, false},
		{"bidi", true, true},
	} {
		t.Run(shape.name, func(t *testing.T) {
			file := closeNamedService(shape.clientStream, shape.serverStream)
			source, err := Generate(file)
			if err != nil {
				t.Fatalf("Generate: %v", err)
			}
			if strings.Contains(string(source), "c.c.Close()") {
				t.Fatalf("generated source emits a client-level Close:\n%s", source)
			}
			compileStub(t, file, "\t_ = sample.LifecycleService_Close.FullName()\n\tfmt.Println(\"ok\")\n")
		})
	}
}

func closeNamedService(clientStream, serverStream bool) ir.File {
	return ir.File{
		GoPackage:    "sample",
		ProtoPackage: "sample.v1",
		InputBase:    "sample",
		Services: []ir.Service{{
			GoName:   "LifecycleService",
			FullName: "sample.v1.LifecycleService",
			Methods: []ir.Method{{
				GoName:       "Close",
				FullName:     "sample.v1.LifecycleService.Close",
				InputType:    "Request",
				OutputType:   "Response",
				ClientStream: clientStream,
				ServerStream: serverStream,
			}},
		}},
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
		_ = c.Do
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

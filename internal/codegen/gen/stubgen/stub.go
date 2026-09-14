// Package stubgen renders ir.File into *.argos.go source.
package stubgen

import (
	"fmt"
	"go/format"
	"strings"
	"unicode"

	"github.com/argos-io/argos/internal/codegen/ir"
)

// Generate returns Go source for one IR file.
func Generate(file ir.File) ([]byte, error) {
	packageName, err := ir.PackageName(file.GoPackage)
	if err != nil {
		return nil, fmt.Errorf("codegen: %w", err)
	}
	if err := ir.ValidateNames(&file); err != nil {
		return nil, fmt.Errorf("codegen: %w", err)
	}
	if file.StubName() == "" {
		return nil, fmt.Errorf("codegen: stub output name is required")
	}
	if len(file.Services) == 0 {
		return nil, fmt.Errorf("codegen: no services in %s", file.StubName())
	}

	var b strings.Builder
	b.WriteString("package ")
	b.WriteString(packageName)
	b.WriteString("\n\n")
	for _, svc := range file.Services {
		for _, method := range svc.Methods {
			if err := validateMethod(svc, method); err != nil {
				return nil, err
			}
		}
	}

	b.WriteString("import (\n")
	b.WriteString("\t\"context\"\n\n")
	b.WriteString("\t\"github.com/argos-io/argos\"\n")
	b.WriteString("\t\"github.com/argos-io/argos/client\"\n")
	b.WriteString("\t\"github.com/argos-io/argos/errs\"\n")
	b.WriteString("\t\"github.com/argos-io/argos/server\"\n")
	b.WriteString("\t\"github.com/argos-io/argos/stream\"\n")
	b.WriteString(")\n\n")

	for _, svc := range file.Services {
		writeService(&b, file, svc)
	}
	source := []byte(strings.TrimRight(b.String(), "\n") + "\n")
	formatted, err := format.Source(source)
	if err != nil {
		return nil, fmt.Errorf("codegen: format stub: %w", err)
	}
	return formatted, nil
}

func validateMethod(_ ir.Service, _ ir.Method) error { return nil }

func writeService(b *strings.Builder, file ir.File, svc ir.Service) {
	prefix := servicePrefix(svc.GoName)

	constNames := make([]string, len(svc.Methods))
	maxLen := 0
	for i, method := range svc.Methods {
		name := prefix + method.GoName
		constNames[i] = name
		if len(name) > maxLen {
			maxLen = len(name)
		}
	}
	b.WriteString("const (\n")
	for i, method := range svc.Methods {
		fullName := methodFullName(file, svc, method)
		b.WriteString("\t")
		b.WriteString(constNames[i])
		if pad := maxLen - len(constNames[i]); pad > 0 {
			b.WriteString(strings.Repeat(" ", pad))
		}
		b.WriteString(" = ")
		b.WriteString(strconvQuote(fullName))
		b.WriteString("\n")
	}
	b.WriteString(")\n\n")

	writeServerInterface(b, svc)
	writeRegister(b, svc, prefix)
	writeStreamServers(b, svc)
	writeClientInterface(b, svc)
	writeClient(b, svc, prefix)
	writeStreamClients(b, svc)
}

func methodFullName(file ir.File, svc ir.Service, method ir.Method) string {
	if method.FullName != "" {
		return method.FullName
	}
	if file.ProtoPackage != "" {
		return fmt.Sprintf("%s.%s/%s", file.ProtoPackage, svc.GoName, method.GoName)
	}
	return fmt.Sprintf("%s/%s", svc.GoName, method.GoName)
}

func writeServerInterface(b *strings.Builder, svc ir.Service) {
	short := serviceShortName(svc.GoName)
	b.WriteString("// ")
	b.WriteString(svc.GoName)
	b.WriteString("Server implements the ")
	b.WriteString(short)
	b.WriteString(" service.\n")
	b.WriteString("type ")
	b.WriteString(svc.GoName)
	b.WriteString("Server interface {\n")
	for _, method := range svc.Methods {
		writeServerMethod(b, svc, method)
	}
	b.WriteString("}\n\n")

	for _, method := range svc.Methods {
		if !method.ClientStream && !method.ServerStream {
			continue
		}
		streamType := streamServerType(svc.GoName, method.GoName)
		b.WriteString("// ")
		b.WriteString(streamType)
		b.WriteString(" sends and receives messages for a ")
		b.WriteString(method.GoName)
		b.WriteString(" call.\n")
		b.WriteString("type ")
		b.WriteString(streamType)
		b.WriteString(" interface {\n")
		if method.ClientStream {
			b.WriteString("\tRecv() (*")
			b.WriteString(method.InputType)
			b.WriteString(", error)\n")
		}
		if method.ClientStream || method.ServerStream {
			b.WriteString("\tSend(*")
			b.WriteString(method.OutputType)
			b.WriteString(") error\n")
		}
		b.WriteString("}\n\n")
	}
}

func writeServerMethod(b *strings.Builder, svc ir.Service, method ir.Method) {
	switch {
	case !method.ClientStream && !method.ServerStream:
		b.WriteString("\t")
		b.WriteString(method.GoName)
		b.WriteString("(context.Context, *")
		b.WriteString(method.InputType)
		b.WriteString(") (*")
		b.WriteString(method.OutputType)
		b.WriteString(", error)\n")
	case !method.ClientStream && method.ServerStream:
		streamType := streamServerType(svc.GoName, method.GoName)
		b.WriteString("\t")
		b.WriteString(method.GoName)
		b.WriteString("(context.Context, *")
		b.WriteString(method.InputType)
		b.WriteString(", ")
		b.WriteString(streamType)
		b.WriteString(") error\n")
	case method.ClientStream:
		streamType := streamServerType(svc.GoName, method.GoName)
		b.WriteString("\t")
		b.WriteString(method.GoName)
		b.WriteString("(context.Context, ")
		b.WriteString(streamType)
		b.WriteString(") error\n")
	}
}

func writeRegister(b *strings.Builder, svc ir.Service, prefix string) {
	registerName := "Register" + svc.GoName
	b.WriteString("// ")
	b.WriteString(registerName)
	b.WriteString(" registers impl's method dispatcher with svc.\n")
	b.WriteString("func ")
	b.WriteString(registerName)
	b.WriteString("(svc *server.Service, impl ")
	b.WriteString(svc.GoName)
	b.WriteString("Server) {\n")
	b.WriteString("\tsvc.RegisterWithMethods(\n")
	b.WriteString("\t\tfunc(ctx context.Context, method string, st stream.Stream) error {\n")
	b.WriteString("\t\t\tswitch method {\n")
	for _, method := range svc.Methods {
		b.WriteString("\t\t\tcase ")
		b.WriteString(prefix + method.GoName)
		b.WriteString(":\n")
		writeRegisterCase(b, svc, method)
	}
	b.WriteString("\t\t\tdefault:\n")
	b.WriteString("\t\t\t\treturn errs.Error(errs.Unimplemented, \"unknown method\")\n")
	b.WriteString("\t\t\t}\n")
	b.WriteString("\t\t},\n")
	for _, method := range svc.Methods {
		b.WriteString("\t\tserver.MethodInfo{Method: ")
		b.WriteString(prefix + method.GoName)
		b.WriteString(", Kind: ")
		b.WriteString(callKind(method))
		b.WriteString("},\n")
	}
	b.WriteString("\t)\n")
	b.WriteString("}\n\n")
}

func writeRegisterCase(b *strings.Builder, svc ir.Service, method ir.Method) {
	switch {
	case !method.ClientStream && !method.ServerStream:
		b.WriteString("\t\t\t\tin := new(")
		b.WriteString(method.InputType)
		b.WriteString(")\n")
		b.WriteString("\t\t\t\tif err := st.Recv(in); err != nil {\n")
		b.WriteString("\t\t\t\t\treturn err\n")
		b.WriteString("\t\t\t\t}\n")
		b.WriteString("\t\t\t\tresp, err := impl.")
		b.WriteString(method.GoName)
		b.WriteString("(ctx, in)\n")
		b.WriteString("\t\t\t\tif err != nil {\n")
		b.WriteString("\t\t\t\t\treturn err\n")
		b.WriteString("\t\t\t\t}\n")
		b.WriteString("\t\t\t\treturn st.Send(resp)\n")
	case !method.ClientStream && method.ServerStream:
		b.WriteString("\t\t\t\tin := new(")
		b.WriteString(method.InputType)
		b.WriteString(")\n")
		b.WriteString("\t\t\t\tif err := st.Recv(in); err != nil {\n")
		b.WriteString("\t\t\t\t\treturn err\n")
		b.WriteString("\t\t\t\t}\n")
		wrapper := streamServerStruct(svc.GoName, method.GoName)
		b.WriteString("\t\t\t\treturn impl.")
		b.WriteString(method.GoName)
		b.WriteString("(ctx, in, &")
		b.WriteString(wrapper)
		b.WriteString("{Stream: st})\n")
	case method.ClientStream:
		wrapper := streamServerStruct(svc.GoName, method.GoName)
		b.WriteString("\t\t\t\treturn impl.")
		b.WriteString(method.GoName)
		b.WriteString("(ctx, &")
		b.WriteString(wrapper)
		b.WriteString("{Stream: st})\n")
	}
}

func writeStreamServers(b *strings.Builder, svc ir.Service) {
	for _, method := range svc.Methods {
		if !method.ClientStream && !method.ServerStream {
			continue
		}
		wrapper := streamServerStruct(svc.GoName, method.GoName)
		b.WriteString("type ")
		b.WriteString(wrapper)
		b.WriteString(" struct {\n")
		b.WriteString("\tstream.Stream\n")
		b.WriteString("}\n\n")
		if method.ClientStream {
			b.WriteString("func (s *")
			b.WriteString(wrapper)
			b.WriteString(") Recv() (*")
			b.WriteString(method.InputType)
			b.WriteString(", error) {\n")
			b.WriteString("\tin := new(")
			b.WriteString(method.InputType)
			b.WriteString(")\n")
			b.WriteString("\tif err := s.Stream.Recv(in); err != nil {\n")
			b.WriteString("\t\treturn nil, err\n")
			b.WriteString("\t}\n")
			b.WriteString("\treturn in, nil\n")
			b.WriteString("}\n\n")
		}
		if method.ClientStream || method.ServerStream {
			b.WriteString("func (s *")
			b.WriteString(wrapper)
			b.WriteString(") Send(event *")
			b.WriteString(method.OutputType)
			b.WriteString(") error {\n")
			b.WriteString("\treturn s.Stream.Send(event)\n")
			b.WriteString("}\n\n")
		}
	}
}

func writeClientInterface(b *strings.Builder, svc ir.Service) {
	short := serviceShortName(svc.GoName)
	b.WriteString("// ")
	b.WriteString(svc.GoName)
	b.WriteString("Client calls the ")
	b.WriteString(short)
	b.WriteString(" service.\n")
	b.WriteString("type ")
	b.WriteString(svc.GoName)
	b.WriteString("Client interface {\n")
	for _, method := range svc.Methods {
		writeClientMethod(b, svc, method)
	}
	b.WriteString("}\n\n")

	for _, method := range svc.Methods {
		if !method.ClientStream && !method.ServerStream {
			continue
		}
		streamType := streamClientType(svc.GoName, method.GoName)
		b.WriteString("// ")
		b.WriteString(streamType)
		b.WriteString(" sends and receives messages for a ")
		b.WriteString(method.GoName)
		b.WriteString(" call.\n")
		b.WriteString("type ")
		b.WriteString(streamType)
		b.WriteString(" interface {\n")
		if method.ClientStream {
			b.WriteString("\tSend(*")
			b.WriteString(method.InputType)
			b.WriteString(") error\n")
			b.WriteString("\tCloseSend() error\n")
		}
		if method.ClientStream || method.ServerStream {
			b.WriteString("\tRecv() (*")
			b.WriteString(method.OutputType)
			b.WriteString(", error)\n")
			b.WriteString("\tClose() error\n")
		}
		b.WriteString("}\n\n")
	}
}

func writeClientMethod(b *strings.Builder, svc ir.Service, method ir.Method) {
	switch {
	case !method.ClientStream && !method.ServerStream:
		b.WriteString("\t")
		b.WriteString(method.GoName)
		b.WriteString("(context.Context, *")
		b.WriteString(method.InputType)
		b.WriteString(") (*")
		b.WriteString(method.OutputType)
		b.WriteString(", error)\n")
	case !method.ClientStream && method.ServerStream:
		streamType := streamClientType(svc.GoName, method.GoName)
		b.WriteString("\t")
		b.WriteString(method.GoName)
		b.WriteString("(context.Context, *")
		b.WriteString(method.InputType)
		b.WriteString(") ")
		b.WriteString(streamType)
		b.WriteString("\n")
	case method.ClientStream:
		streamType := streamClientType(svc.GoName, method.GoName)
		b.WriteString("\t")
		b.WriteString(method.GoName)
		b.WriteString("(context.Context) ")
		b.WriteString(streamType)
		b.WriteString("\n")
	}
}

func writeClient(b *strings.Builder, svc ir.Service, prefix string) {
	clientType := prefix + "Client"
	constructor := "New" + svc.GoName + "Client"
	short := serviceShortName(svc.GoName)
	b.WriteString("// ")
	b.WriteString(constructor)
	b.WriteString(" creates an ")
	b.WriteString(short)
	b.WriteString(" service client.\n")
	b.WriteString("func ")
	b.WriteString(constructor)
	b.WriteString("(opts ...argos.Option) ")
	b.WriteString(svc.GoName)
	b.WriteString("Client {\n")
	b.WriteString("\treturn &")
	b.WriteString(clientType)
	b.WriteString("{c: client.New(opts...)}\n")
	b.WriteString("}\n\n")
	b.WriteString("type ")
	b.WriteString(clientType)
	b.WriteString(" struct {\n")
	b.WriteString("\tc *client.Client\n")
	b.WriteString("}\n\n")

	for _, method := range svc.Methods {
		writeClientMethodImpl(b, svc, method, prefix, clientType)
	}
}

func writeClientMethodImpl(b *strings.Builder, svc ir.Service, method ir.Method, prefix, clientType string) {
	constName := prefix + method.GoName
	switch {
	case !method.ClientStream && !method.ServerStream:
		b.WriteString("func (c *")
		b.WriteString(clientType)
		b.WriteString(") ")
		b.WriteString(method.GoName)
		b.WriteString("(ctx context.Context, req *")
		b.WriteString(method.InputType)
		b.WriteString(") (*")
		b.WriteString(method.OutputType)
		b.WriteString(", error) {\n")
		b.WriteString("\tvar resp *")
		b.WriteString(method.OutputType)
		b.WriteString("\n")
		b.WriteString("\terr := c.c.Open(ctx, ")
		b.WriteString(constName)
		b.WriteString(", func(st stream.Stream) error {\n")
		b.WriteString("\t\tif err := st.Send(req); err != nil {\n")
		b.WriteString("\t\t\treturn err\n")
		b.WriteString("\t\t}\n")
		b.WriteString("\t\tif err := st.CloseSend(); err != nil {\n")
		b.WriteString("\t\t\treturn err\n")
		b.WriteString("\t\t}\n")
		b.WriteString("\t\tresp = new(")
		b.WriteString(method.OutputType)
		b.WriteString(")\n")
		b.WriteString("\t\treturn st.Recv(resp)\n")
		b.WriteString("\t})\n")
		b.WriteString("\treturn resp, err\n")
		b.WriteString("}\n\n")
	case !method.ClientStream && method.ServerStream:
		streamType := streamClientType(svc.GoName, method.GoName)
		wrapper := streamClientStruct(svc.GoName, method.GoName)
		b.WriteString("func (c *")
		b.WriteString(clientType)
		b.WriteString(") ")
		b.WriteString(method.GoName)
		b.WriteString("(ctx context.Context, req *")
		b.WriteString(method.InputType)
		b.WriteString(") ")
		b.WriteString(streamType)
		b.WriteString(" {\n")
		b.WriteString("\twc := &")
		b.WriteString(wrapper)
		b.WriteString("{call: c.c.OpenStream(ctx, ")
		b.WriteString(constName)
		b.WriteString(", stream.CallServerStreaming), initialDone: make(chan struct{})}\n")
		b.WriteString("\tgo func() {\n")
		b.WriteString("\t\terr := wc.call.Send(req)\n")
		b.WriteString("\t\tif err == nil {\n")
		b.WriteString("\t\t\terr = wc.call.CloseSend()\n")
		b.WriteString("\t\t}\n")
		b.WriteString("\t\twc.initialErr = err\n")
		b.WriteString("\t\tclose(wc.initialDone)\n")
		b.WriteString("\t}()\n")
		b.WriteString("\treturn wc\n")
		b.WriteString("}\n\n")
	case method.ClientStream:
		streamType := streamClientType(svc.GoName, method.GoName)
		wrapper := streamClientStruct(svc.GoName, method.GoName)
		b.WriteString("func (c *")
		b.WriteString(clientType)
		b.WriteString(") ")
		b.WriteString(method.GoName)
		b.WriteString("(ctx context.Context) ")
		b.WriteString(streamType)
		b.WriteString(" {\n")
		b.WriteString("\treturn &")
		b.WriteString(wrapper)
		b.WriteString("{call: c.c.OpenStream(ctx, ")
		b.WriteString(constName)
		b.WriteString(", ")
		b.WriteString(callKind(method))
		b.WriteString(")}\n")
		b.WriteString("}\n\n")
	}
}

func writeStreamClients(b *strings.Builder, svc ir.Service) {
	for _, method := range svc.Methods {
		if !method.ClientStream && !method.ServerStream {
			continue
		}
		wrapper := streamClientStruct(svc.GoName, method.GoName)
		b.WriteString("type ")
		b.WriteString(wrapper)
		b.WriteString(" struct {\n")
		b.WriteString("\tcall *client.CallStream\n")
		if !method.ClientStream && method.ServerStream {
			b.WriteString("\tinitialDone chan struct{}\n")
			b.WriteString("\tinitialErr  error\n")
		}
		b.WriteString("}\n\n")

		if method.ClientStream {
			b.WriteString("func (c *")
			b.WriteString(wrapper)
			b.WriteString(") Send(msg *")
			b.WriteString(method.InputType)
			b.WriteString(") error {\n")
			b.WriteString("\treturn c.call.Send(msg)\n")
			b.WriteString("}\n\n")
			b.WriteString("func (c *")
			b.WriteString(wrapper)
			b.WriteString(") CloseSend() error {\n")
			b.WriteString("\treturn c.call.CloseSend()\n")
			b.WriteString("}\n\n")
		}
		if method.ClientStream || method.ServerStream {
			b.WriteString("func (c *")
			b.WriteString(wrapper)
			b.WriteString(") Recv() (*")
			b.WriteString(method.OutputType)
			b.WriteString(", error) {\n")
			if !method.ClientStream {
				b.WriteString("\t<-c.initialDone\n")
				b.WriteString("\tif c.initialErr != nil {\n")
				b.WriteString("\t\treturn nil, c.initialErr\n")
				b.WriteString("\t}\n")
			}
			b.WriteString("\tevent := new(")
			b.WriteString(method.OutputType)
			b.WriteString(")\n")
			b.WriteString("\tif err := c.call.Recv(event); err != nil {\n")
			b.WriteString("\t\treturn nil, err\n")
			b.WriteString("\t}\n")
			if !method.ServerStream {
				b.WriteString("\t_ = c.call.Close()\n")
			}
			b.WriteString("\treturn event, nil\n")
			b.WriteString("}\n\n")
		}
		b.WriteString("func (c *")
		b.WriteString(wrapper)
		b.WriteString(") Close() error {\n")
		b.WriteString("\treturn c.call.Close()\n")
		b.WriteString("}\n\n")
	}
}

func callKind(method ir.Method) string {
	switch {
	case method.ClientStream && method.ServerStream:
		return "stream.CallBidiStreaming"
	case method.ClientStream:
		return "stream.CallClientStreaming"
	case method.ServerStream:
		return "stream.CallServerStreaming"
	default:
		return "stream.CallUnary"
	}
}

func servicePrefix(serviceGoName string) string {
	name := strings.TrimSuffix(serviceGoName, "Service")
	if name == "" {
		return ""
	}
	runes := []rune(name)
	runes[0] = unicode.ToLower(runes[0])
	return string(runes)
}

func serviceShortName(serviceGoName string) string {
	return strings.TrimSuffix(serviceGoName, "Service")
}

func streamServerType(serviceGoName, methodGoName string) string {
	return serviceGoName + "_" + methodGoName + "Server"
}

func streamClientType(serviceGoName, methodGoName string) string {
	return serviceGoName + "_" + methodGoName + "Client"
}

func streamServerStruct(serviceGoName, methodGoName string) string {
	return lowerFirst(serviceGoName) + methodGoName + "Server"
}

func streamClientStruct(serviceGoName, methodGoName string) string {
	return lowerFirst(serviceGoName) + methodGoName + "Client"
}

func lowerFirst(name string) string {
	runes := []rune(name)
	if len(runes) != 0 {
		runes[0] = unicode.ToLower(runes[0])
	}
	return string(runes)
}

func strconvQuote(s string) string {
	return fmt.Sprintf("%q", s)
}

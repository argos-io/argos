// Package stub renders ir.File into *.argos.go source.
package stubgen

import (
	"fmt"
	"strings"
	"unicode"

	"github.com/argos-io/argos/internal/codegen/ir"
)

// Generate returns Go source for one IR file.
func Generate(file ir.File) ([]byte, error) {
	if file.GoPackage == "" {
		return nil, fmt.Errorf("codegen: go_package is required")
	}
	if file.StubName() == "" {
		return nil, fmt.Errorf("codegen: stub output name is required")
	}
	if len(file.Services) == 0 {
		return nil, fmt.Errorf("codegen: no services in %s", file.StubName())
	}

	var b strings.Builder
	b.WriteString("package ")
	b.WriteString(file.GoPackage)
	b.WriteString("\n\n")

	needsIO := false
	for _, svc := range file.Services {
		for _, method := range svc.Methods {
			if method.ServerStream {
				needsIO = true
			}
			if err := validateMethod(svc, method); err != nil {
				return nil, err
			}
		}
	}

	b.WriteString("import (\n")
	b.WriteString("\t\"context\"\n")
	if needsIO {
		b.WriteString("\t\"io\"\n")
	}
	b.WriteString("\n")
	b.WriteString("\t\"github.com/argos-io/argos\"\n")
	b.WriteString(")\n\n")

	for _, svc := range file.Services {
		writeService(&b, file, svc)
	}
	return []byte(strings.TrimRight(b.String(), "\n") + "\n"), nil
}

func validateMethod(svc ir.Service, method ir.Method) error {
	switch {
	case !method.ClientStream && !method.ServerStream:
		return nil
	case method.ServerStream && !method.ClientStream:
		return nil
	default:
		return fmt.Errorf("codegen: unsupported streaming on %s/%s", svc.GoName, method.GoName)
	}
}

func writeService(b *strings.Builder, file ir.File, svc ir.Service) {
	prefix := servicePrefix(svc.GoName)
	for _, method := range svc.Methods {
		if method.FullName == "" && file.ProtoPackage != "" {
			method.FullName = fmt.Sprintf("%s.%s/%s", file.ProtoPackage, svc.GoName, method.GoName)
		}
	}

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
		fullName := method.FullName
		if fullName == "" {
			fullName = fmt.Sprintf("%s/%s", svc.GoName, method.GoName)
		}
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
		if !method.ServerStream {
			continue
		}
		streamType := streamServerType(svc.GoName, method.GoName)
		b.WriteString("// ")
		b.WriteString(streamType)
		b.WriteString(" sends events from a ")
		b.WriteString(method.GoName)
		b.WriteString(" call.\n")
		b.WriteString("type ")
		b.WriteString(streamType)
		b.WriteString(" interface {\n")
		b.WriteString("\tSend(*")
		b.WriteString(method.OutputType)
		b.WriteString(") error\n")
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
	case method.ServerStream && !method.ClientStream:
		streamType := streamServerType(svc.GoName, method.GoName)
		b.WriteString("\t")
		b.WriteString(method.GoName)
		b.WriteString("(context.Context, *")
		b.WriteString(method.InputType)
		b.WriteString(", ")
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
	b.WriteString("(svc *argos.Service, impl ")
	b.WriteString(svc.GoName)
	b.WriteString("Server) {\n")
	b.WriteString("\tsvc.Register(func(ctx context.Context, method string, st argos.Stream) error {\n")
	b.WriteString("\t\tswitch method {\n")
	for _, method := range svc.Methods {
		b.WriteString("\t\tcase ")
		b.WriteString(prefix + method.GoName)
		b.WriteString(":\n")
		writeRegisterCase(b, svc, method)
	}
	b.WriteString("\t\tdefault:\n")
	b.WriteString("\t\t\treturn argos.Error(argos.Unimplemented, \"unknown method\")\n")
	b.WriteString("\t\t}\n")
	b.WriteString("\t})\n")
	b.WriteString("}\n\n")
}

func writeRegisterCase(b *strings.Builder, svc ir.Service, method ir.Method) {
	b.WriteString("\t\t\tin := new(")
	b.WriteString(method.InputType)
	b.WriteString(")\n")
	b.WriteString("\t\t\tif err := st.Recv(in); err != nil {\n")
	b.WriteString("\t\t\t\treturn err\n")
	b.WriteString("\t\t\t}\n")
	switch {
	case !method.ClientStream && !method.ServerStream:
		b.WriteString("\t\t\tresp, err := impl.")
		b.WriteString(method.GoName)
		b.WriteString("(ctx, in)\n")
		b.WriteString("\t\t\tif err != nil {\n")
		b.WriteString("\t\t\t\treturn err\n")
		b.WriteString("\t\t\t}\n")
		b.WriteString("\t\t\treturn st.Send(resp)\n")
	case method.ServerStream && !method.ClientStream:
		wrapper := streamServerStruct(method.GoName)
		b.WriteString("\t\t\treturn impl.")
		b.WriteString(method.GoName)
		b.WriteString("(ctx, in, &")
		b.WriteString(wrapper)
		b.WriteString("{Stream: st})\n")
	}
}

func writeStreamServers(b *strings.Builder, svc ir.Service) {
	for _, method := range svc.Methods {
		if !method.ServerStream {
			continue
		}
		wrapper := streamServerStruct(method.GoName)
		b.WriteString("type ")
		b.WriteString(wrapper)
		b.WriteString(" struct {\n")
		b.WriteString("\targos.Stream\n")
		b.WriteString("}\n\n")
		b.WriteString("func (s *")
		b.WriteString(wrapper)
		b.WriteString(") Send(event *")
		b.WriteString(method.OutputType)
		b.WriteString(") error {\n")
		b.WriteString("\treturn s.Stream.Send(event)\n")
		b.WriteString("}\n\n")
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
		if !method.ServerStream {
			continue
		}
		streamType := streamClientType(svc.GoName, method.GoName)
		b.WriteString("// ")
		b.WriteString(streamType)
		b.WriteString(" receives events from a ")
		b.WriteString(method.GoName)
		b.WriteString(" call.\n")
		b.WriteString("type ")
		b.WriteString(streamType)
		b.WriteString(" interface {\n")
		b.WriteString("\tRecv() (*")
		b.WriteString(method.OutputType)
		b.WriteString(", error)\n")
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
	case method.ServerStream && !method.ClientStream:
		streamType := streamClientType(svc.GoName, method.GoName)
		b.WriteString("\t")
		b.WriteString(method.GoName)
		b.WriteString("(context.Context, *")
		b.WriteString(method.InputType)
		b.WriteString(") ")
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
	b.WriteString("{c: argos.NewClient(opts...)}\n")
	b.WriteString("}\n\n")
	b.WriteString("type ")
	b.WriteString(clientType)
	b.WriteString(" struct {\n")
	b.WriteString("\tc *argos.Client\n")
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
		b.WriteString(", func(st argos.Stream) error {\n")
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
	case method.ServerStream && !method.ClientStream:
		streamType := streamClientType(svc.GoName, method.GoName)
		wrapper := streamClientStruct(method.GoName)
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
		b.WriteString("{ch: make(chan *")
		b.WriteString(method.OutputType)
		b.WriteString(")}\n")
		b.WriteString("\tgo func() {\n")
		b.WriteString("\t\tdefer close(wc.ch)\n")
		b.WriteString("\t\twc.err = c.c.Open(ctx, ")
		b.WriteString(constName)
		b.WriteString(", func(st argos.Stream) error {\n")
		b.WriteString("\t\t\tif err := st.Send(req); err != nil {\n")
		b.WriteString("\t\t\t\treturn err\n")
		b.WriteString("\t\t\t}\n")
		b.WriteString("\t\t\tif err := st.CloseSend(); err != nil {\n")
		b.WriteString("\t\t\t\treturn err\n")
		b.WriteString("\t\t\t}\n")
		b.WriteString("\t\t\tfor {\n")
		b.WriteString("\t\t\t\tevent := new(")
		b.WriteString(method.OutputType)
		b.WriteString(")\n")
		b.WriteString("\t\t\t\tswitch err := st.Recv(event); err {\n")
		b.WriteString("\t\t\t\tcase nil:\n")
		b.WriteString("\t\t\t\t\twc.ch <- event\n")
		b.WriteString("\t\t\t\tcase io.EOF:\n")
		b.WriteString("\t\t\t\t\treturn nil\n")
		b.WriteString("\t\t\t\tdefault:\n")
		b.WriteString("\t\t\t\t\treturn err\n")
		b.WriteString("\t\t\t\t}\n")
		b.WriteString("\t\t\t}\n")
		b.WriteString("\t\t})\n")
		b.WriteString("\t}()\n")
		b.WriteString("\treturn wc\n")
		b.WriteString("}\n\n")
	}
}

func writeStreamClients(b *strings.Builder, svc ir.Service) {
	for _, method := range svc.Methods {
		if !method.ServerStream {
			continue
		}
		wrapper := streamClientStruct(method.GoName)
		b.WriteString("type ")
		b.WriteString(wrapper)
		b.WriteString(" struct {\n")
		b.WriteString("\tch  chan *")
		b.WriteString(method.OutputType)
		b.WriteString("\n")
		b.WriteString("\terr error\n")
		b.WriteString("}\n\n")
		b.WriteString("func (c *")
		b.WriteString(wrapper)
		b.WriteString(") Recv() (*")
		b.WriteString(method.OutputType)
		b.WriteString(", error) {\n")
		b.WriteString("\tevent, ok := <-c.ch\n")
		b.WriteString("\tif !ok {\n")
		b.WriteString("\t\tif c.err != nil {\n")
		b.WriteString("\t\t\treturn nil, c.err\n")
		b.WriteString("\t\t}\n")
		b.WriteString("\t\treturn nil, io.EOF\n")
		b.WriteString("\t}\n")
		b.WriteString("\treturn event, nil\n")
		b.WriteString("}\n\n")
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

func streamServerStruct(methodGoName string) string {
	runes := []rune(methodGoName)
	runes[0] = unicode.ToLower(runes[0])
	return string(runes) + "Server"
}

func streamClientStruct(methodGoName string) string {
	runes := []rune(methodGoName)
	runes[0] = unicode.ToLower(runes[0])
	return string(runes) + "Client"
}

func strconvQuote(s string) string {
	return fmt.Sprintf("%q", s)
}

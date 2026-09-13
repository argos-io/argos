// Package proto parses .proto files into codegen IR using protocompile.
package proto

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/bufbuild/protocompile"
	"github.com/bufbuild/protocompile/linker"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"

	"github.com/argos-io/argos/internal/codegen/frontend"
	"github.com/argos-io/argos/internal/codegen/ir"
)

// Frontend implements frontend.Frontend for protobuf IDL.
type Frontend struct {
	ImportPaths []string
}

func (Frontend) Name() string { return "proto" }

// Parse compiles proto files and returns one IR file per input that defines services.
func (f Frontend) Parse(ctx context.Context, inputs []string) ([]ir.File, error) {
	if len(inputs) == 0 {
		return nil, fmt.Errorf("proto frontend: no input files")
	}
	paths := f.ImportPaths
	if len(paths) == 0 {
		paths = []string{"."}
	}
	compiler := protocompile.Compiler{
		Resolver: &protocompile.SourceResolver{ImportPaths: paths},
	}
	compileNames := make([]string, len(inputs))
	for i, input := range inputs {
		name, err := compileName(input, paths)
		if err != nil {
			return nil, err
		}
		compileNames[i] = name
	}
	linked, err := compiler.Compile(ctx, compileNames...)
	if err != nil {
		return nil, fmt.Errorf("proto frontend: compile: %w", err)
	}

	var out []ir.File
	for i := range inputs {
		base := filepath.Base(compileNames[i])
		inputBase := strings.TrimSuffix(base, filepath.Ext(base))
		file, err := findFile(linked, base)
		if err != nil {
			return nil, err
		}
		if file.Services().Len() == 0 {
			continue
		}
		irFile, err := fileToIR(file, inputBase)
		if err != nil {
			return nil, err
		}
		out = append(out, irFile)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("proto frontend: no services found in inputs")
	}
	return out, nil
}

func findFile(files linker.Files, base string) (linker.File, error) {
	for _, file := range files {
		if filepath.Base(file.Path()) == base {
			return file, nil
		}
	}
	return nil, fmt.Errorf("proto frontend: compiled file %q not found", base)
}

func fileToIR(file linker.File, inputBase string) (ir.File, error) {
	goPackage, err := parseGoPackage(file)
	if err != nil {
		return ir.File{}, err
	}
	protoPkg := string(file.Package())
	fdp := protodesc.ToFileDescriptorProto(file)
	descBytes, err := proto.Marshal(fdp)
	if err != nil {
		return ir.File{}, fmt.Errorf("proto frontend: marshal descriptor: %w", err)
	}
	out := ir.File{
		IRVersion:      ir.Version2,
		Source:         ir.SourceProto,
		GoPackage:      goPackage,
		ProtoPackage:   protoPkg,
		InputBase:      inputBase,
		FileDescriptor: descBytes,
		Outputs: ir.Outputs{
			Stub:     inputBase + ".argos.go",
			Messages: inputBase + ".pb.go",
		},
		OutputName: inputBase + ".argos.go",
		Messages:   messagesFromFile(file),
	}
	for i := 0; i < file.Services().Len(); i++ {
		svc := file.Services().Get(i)
		svcName := string(svc.Name())
		service := ir.Service{GoName: svcName}
		for j := 0; j < svc.Methods().Len(); j++ {
			method := svc.Methods().Get(j)
			methodName := string(method.Name())
			inputType, err := messageGoName(method.Input())
			if err != nil {
				return ir.File{}, err
			}
			outputType, err := messageGoName(method.Output())
			if err != nil {
				return ir.File{}, err
			}
			service.Methods = append(service.Methods, ir.Method{
				GoName:       methodName,
				FullName:     fmt.Sprintf("%s.%s/%s", protoPkg, svcName, methodName),
				InputType:    inputType,
				OutputType:   outputType,
				ServerStream: method.IsStreamingServer(),
				ClientStream: method.IsStreamingClient(),
			})
		}
		out.Services = append(out.Services, service)
	}
	return out, nil
}

func messagesFromFile(file linker.File) []ir.Message {
	var out []ir.Message
	for i := 0; i < file.Messages().Len(); i++ {
		msg := file.Messages().Get(i)
		im := ir.Message{GoName: string(msg.Name())}
		for j := 0; j < msg.Fields().Len(); j++ {
			field := msg.Fields().Get(j)
			im.Fields = append(im.Fields, irField(field))
		}
		out = append(out, im)
	}
	return out
}

func irField(field protoreflect.FieldDescriptor) ir.Field {
	kind := scalarKind(field)
	if field.IsList() {
		kind = "repeated " + kind
	}
	f := ir.Field{
		GoName: exportName(string(field.Name())),
		Number: int32(field.Number()),
		Kind:   kind,
	}
	if field.Message() != nil {
		f.MessageType = string(field.Message().Name())
	}
	if field.Enum() != nil {
		f.EnumType = string(field.Enum().Name())
	}
	return f
}

func scalarKind(field protoreflect.FieldDescriptor) string {
	switch field.Kind() {
	case protoreflect.StringKind:
		return "string"
	case protoreflect.BytesKind:
		return "bytes"
	case protoreflect.BoolKind:
		return "bool"
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return "int32"
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return "int64"
	case protoreflect.EnumKind:
		return "enum"
	case protoreflect.MessageKind:
		return "message"
	default:
		return "string"
	}
}

func exportName(name string) string {
	if name == "" {
		return name
	}
	return strings.ToUpper(name[:1]) + name[1:]
}

func parseGoPackage(file linker.File) (string, error) {
	opts, ok := file.Options().(*descriptorpb.FileOptions)
	if !ok || opts == nil || opts.GoPackage == nil {
		return "", fmt.Errorf("proto frontend: %s missing go_package option", file.Path())
	}
	raw := opts.GetGoPackage()
	if raw == "" {
		return "", fmt.Errorf("proto frontend: %s empty go_package", file.Path())
	}
	if i := strings.IndexByte(raw, ';'); i >= 0 {
		return raw[i+1:], nil
	}
	if i := strings.LastIndex(raw, "/"); i >= 0 {
		return raw[i+1:], nil
	}
	return raw, nil
}

func messageGoName(desc protoreflect.MessageDescriptor) (string, error) {
	if desc == nil {
		return "", fmt.Errorf("proto frontend: missing message type")
	}
	return string(desc.Name()), nil
}

func compileName(input string, importPaths []string) (string, error) {
	absInput, err := filepath.Abs(input)
	if err != nil {
		return "", err
	}
	for _, p := range importPaths {
		absImport, err := filepath.Abs(p)
		if err != nil {
			continue
		}
		rel, err := filepath.Rel(absImport, absInput)
		if err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			return rel, nil
		}
	}
	return filepath.Base(input), nil
}

var _ frontend.Frontend = Frontend{}

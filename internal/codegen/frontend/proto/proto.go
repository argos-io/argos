// Package proto parses .proto files into codegen IR using protocompile.
package proto

import (
	"context"
	"fmt"
	"path"
	"path/filepath"
	"strings"
	"unicode"

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
	if ctx == nil {
		return nil, fmt.Errorf("proto frontend: nil context")
	}
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
	// An input outside every --proto-path falls back to its basename, so two
	// distinct files can claim the same compile name and the compiler resolves
	// one of them twice. Silently generating from the wrong descriptor is worse
	// than refusing, so reject the collision and name both inputs.
	claimed := make(map[string]string, len(inputs))
	for i, input := range inputs {
		name, err := compileName(input, paths)
		if err != nil {
			return nil, err
		}
		if prev, dup := claimed[name]; dup {
			return nil, fmt.Errorf(
				"proto frontend: %q and %q both compile as %q; put them under a --proto-path so their paths stay distinct",
				prev, input, name)
		}
		claimed[name] = input
		compileNames[i] = name
	}
	linked, err := compiler.Compile(ctx, compileNames...)
	if err != nil {
		return nil, fmt.Errorf("proto frontend: compile: %w", err)
	}
	type inputFile struct {
		index int
		file  linker.File
	}
	serviceFiles := make([]inputFile, 0, len(inputs))
	for i := range inputs {
		file, err := findFile(linked, compileNames[i])
		if err != nil {
			return nil, err
		}
		if file.Services().Len() != 0 {
			serviceFiles = append(serviceFiles, inputFile{index: i, file: file})
		}
	}
	if len(serviceFiles) == 0 {
		return nil, fmt.Errorf("proto frontend: no services found in inputs")
	}
	descriptorSet, err := marshalDescriptorSet(linked)
	if err != nil {
		return nil, err
	}

	var out []ir.File
	for _, input := range serviceFiles {
		i := input.index
		file := input.file
		base := path.Base(compileNames[i])
		inputBase := strings.TrimSuffix(base, path.Ext(base))
		irFile, err := fileToIR(file, inputBase, descriptorSet)
		if err != nil {
			return nil, err
		}
		out = append(out, irFile)
	}
	return out, nil
}

func findFile(files linker.Files, name string) (linker.File, error) {
	want := filepath.ToSlash(filepath.Clean(name))
	var byBase []linker.File
	for _, file := range files {
		filePath := filepath.ToSlash(filepath.Clean(file.Path()))
		if filePath == want {
			return file, nil
		}
		if path.Base(filePath) == path.Base(want) {
			byBase = append(byBase, file)
		}
	}
	if len(byBase) == 1 {
		return byBase[0], nil
	}
	if len(byBase) > 1 {
		return nil, fmt.Errorf("proto frontend: compiled file %q is ambiguous", name)
	}
	return nil, fmt.Errorf("proto frontend: compiled file %q not found", name)
}

func fileToIR(file linker.File, inputBase string, descriptorSet []byte) (ir.File, error) {
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
		IRVersion:         ir.Version2,
		Source:            ir.SourceProto,
		GoPackage:         goPackage,
		ProtoPackage:      protoPkg,
		InputBase:         inputBase,
		FileDescriptor:    descBytes,
		FileDescriptorSet: descriptorSet,
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
		svcFullName := string(svc.FullName())
		service := ir.Service{
			GoName:   exportName(svcName),
			FullName: svcFullName,
		}
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
			shape := ir.ShapeFromStreams(method.IsStreamingClient(), method.IsStreamingServer())
			clientStream, serverStream := ir.StreamsFromShape(shape)
			service.Methods = append(service.Methods, ir.Method{
				GoName:       exportName(methodName),
				FullName:     string(method.FullName()),
				InputType:    inputType,
				OutputType:   outputType,
				Shape:        shape,
				ClientStream: clientStream,
				ServerStream: serverStream,
			})
		}
		out.Services = append(out.Services, service)
	}
	return out, nil
}

func marshalDescriptorSet(files linker.Files) ([]byte, error) {
	set := &descriptorpb.FileDescriptorSet{}
	for _, file := range files {
		set.File = append(set.File, protodesc.ToFileDescriptorProto(file))
	}
	data, err := proto.Marshal(set)
	if err != nil {
		return nil, fmt.Errorf("proto frontend: marshal descriptor set: %w", err)
	}
	return data, nil
}

func messagesFromFile(file linker.File) []ir.Message {
	var out []ir.Message
	for i := 0; i < file.Messages().Len(); i++ {
		msg := file.Messages().Get(i)
		im := ir.Message{GoName: exportName(string(msg.Name()))}
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
	case protoreflect.Int32Kind:
		return "int32"
	case protoreflect.Sint32Kind:
		return "sint32"
	case protoreflect.Sfixed32Kind:
		return "sfixed32"
	case protoreflect.Int64Kind:
		return "int64"
	case protoreflect.Sint64Kind:
		return "sint64"
	case protoreflect.Sfixed64Kind:
		return "sfixed64"
	case protoreflect.Uint32Kind:
		return "uint32"
	case protoreflect.Fixed32Kind:
		return "fixed32"
	case protoreflect.Uint64Kind:
		return "uint64"
	case protoreflect.Fixed64Kind:
		return "fixed64"
	case protoreflect.FloatKind:
		return "float"
	case protoreflect.DoubleKind:
		return "double"
	case protoreflect.EnumKind:
		return "enum"
	case protoreflect.MessageKind:
		return "message"
	default:
		return "string"
	}
}

func exportName(name string) string {
	var b strings.Builder
	upperNext := true
	for _, r := range name {
		if r == '_' {
			upperNext = true
			continue
		}
		if upperNext {
			r = unicode.ToUpper(r)
			upperNext = false
		}
		b.WriteRune(r)
	}
	return b.String()
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
	name, err := ir.PackageName(raw)
	if err != nil {
		return "", fmt.Errorf("proto frontend: %s: %w", file.Path(), err)
	}
	return name, nil
}

func messageGoName(desc protoreflect.MessageDescriptor) (string, error) {
	if desc == nil {
		return "", fmt.Errorf("proto frontend: missing message type")
	}
	var parts []string
	for current := desc; current != nil; {
		parts = append([]string{exportName(string(current.Name()))}, parts...)
		parent, ok := current.Parent().(protoreflect.MessageDescriptor)
		if !ok {
			break
		}
		current = parent
	}
	return strings.Join(parts, "_"), nil
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
			return filepath.ToSlash(rel), nil
		}
	}
	return filepath.Base(input), nil
}

var _ frontend.Frontend = Frontend{}

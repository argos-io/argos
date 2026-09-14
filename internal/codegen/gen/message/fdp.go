package message

import (
	"fmt"
	"path"
	"strings"
	"unicode"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"

	"github.com/argos-io/argos/internal/codegen/ir"
)

func fileDescriptor(file ir.File) (protoreflect.FileDescriptor, error) {
	if len(file.FileDescriptorSet) > 0 {
		fdset := &descriptorpb.FileDescriptorSet{}
		if err := proto.Unmarshal(file.FileDescriptorSet, fdset); err != nil {
			return nil, fmt.Errorf("message: decode file_descriptor_set: %w", err)
		}
		files, err := protodesc.NewFiles(fdset)
		if err != nil {
			return nil, fmt.Errorf("message: link file_descriptor_set: %w", err)
		}
		name := file.InputBase + ".proto"
		if len(file.FileDescriptor) > 0 {
			fdp := &descriptorpb.FileDescriptorProto{}
			if err := proto.Unmarshal(file.FileDescriptor, fdp); err != nil {
				return nil, fmt.Errorf("message: decode file_descriptor: %w", err)
			}
			if fdp.GetName() != "" {
				name = fdp.GetName()
			}
		}
		fd, err := files.FindFileByPath(name)
		if err == nil {
			return fd, nil
		}
		// Plugin IR may carry only an input base while the descriptor set keeps
		// the original relative directory. Accept a unique basename match, but
		// reject ambiguity rather than generating from the wrong file.
		var match protoreflect.FileDescriptor
		for _, candidate := range fdset.GetFile() {
			if path.Base(candidate.GetName()) != path.Base(name) {
				continue
			}
			linked, linkErr := files.FindFileByPath(candidate.GetName())
			if linkErr != nil {
				continue
			}
			if match != nil {
				return nil, fmt.Errorf("message: file_descriptor_set basename %q is ambiguous", path.Base(name))
			}
			match = linked
		}
		if match == nil {
			return nil, fmt.Errorf("message: find %q in file_descriptor_set: %w", name, err)
		}
		return match, nil
	}
	if len(file.FileDescriptor) > 0 {
		fdp := &descriptorpb.FileDescriptorProto{}
		if err := proto.Unmarshal(file.FileDescriptor, fdp); err != nil {
			return nil, fmt.Errorf("message: decode file_descriptor: %w", err)
		}
		return protodesc.NewFile(fdp, protoregistry.GlobalFiles)
	}
	if len(file.Messages) == 0 {
		return nil, fmt.Errorf("message: no messages or file_descriptor")
	}
	fdp, err := buildFDPFromIR(file)
	if err != nil {
		return nil, err
	}
	return protodesc.NewFile(fdp, nil)
}

func buildFDPFromIR(file ir.File) (*descriptorpb.FileDescriptorProto, error) {
	name := file.InputBase + ".msg"
	if file.Source == ir.SourceProto {
		name = file.InputBase + ".proto"
	}
	fdp := &descriptorpb.FileDescriptorProto{
		Name:    protoString(name),
		Package: protoString(file.ProtoPackage),
		Syntax:  protoString("proto3"),
	}
	for _, msg := range file.Messages {
		mdp := &descriptorpb.DescriptorProto{Name: protoString(msg.GoName)}
		for _, field := range msg.Fields {
			fdpField, err := fieldToDescriptor(field, file.ProtoPackage)
			if err != nil {
				return nil, err
			}
			mdp.Field = append(mdp.Field, fdpField)
		}
		fdp.MessageType = append(fdp.MessageType, mdp)
	}
	for _, svc := range file.Services {
		sdp := &descriptorpb.ServiceDescriptorProto{Name: protoString(svc.GoName)}
		for _, method := range svc.Methods {
			md := &descriptorpb.MethodDescriptorProto{
				Name:       protoString(method.GoName),
				InputType:  protoString(qualifiedProtoName(file.ProtoPackage, method.InputType)),
				OutputType: protoString(qualifiedProtoName(file.ProtoPackage, method.OutputType)),
			}
			if method.ServerStream {
				md.ServerStreaming = protoBool(true)
			}
			if method.ClientStream {
				md.ClientStreaming = protoBool(true)
			}
			sdp.Method = append(sdp.Method, md)
		}
		fdp.Service = append(fdp.Service, sdp)
	}
	return fdp, nil
}

func qualifiedProtoName(pkg, name string) string {
	if strings.HasPrefix(name, ".") {
		return name
	}
	if pkg == "" {
		return "." + name
	}
	return "." + pkg + "." + name
}

func fieldToDescriptor(field ir.Field, protoPackage string) (*descriptorpb.FieldDescriptorProto, error) {
	if strings.HasPrefix(field.Kind, "repeated ") {
		inner := field
		inner.Kind = strings.TrimPrefix(field.Kind, "repeated ")
		fd, err := fieldToDescriptor(inner, protoPackage)
		if err != nil {
			return nil, err
		}
		fd.Label = descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum()
		return fd, nil
	}
	fd := &descriptorpb.FieldDescriptorProto{
		Name:   protoString(protobufFieldName(field.GoName)),
		Number: protoInt32(field.Number),
	}
	switch field.Kind {
	case "string":
		fd.Type = descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum()
	case "bytes":
		fd.Type = descriptorpb.FieldDescriptorProto_TYPE_BYTES.Enum()
	case "bool":
		fd.Type = descriptorpb.FieldDescriptorProto_TYPE_BOOL.Enum()
	case "int32":
		fd.Type = descriptorpb.FieldDescriptorProto_TYPE_INT32.Enum()
	case "sint32":
		fd.Type = descriptorpb.FieldDescriptorProto_TYPE_SINT32.Enum()
	case "sfixed32":
		fd.Type = descriptorpb.FieldDescriptorProto_TYPE_SFIXED32.Enum()
	case "int64":
		fd.Type = descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum()
	case "sint64":
		fd.Type = descriptorpb.FieldDescriptorProto_TYPE_SINT64.Enum()
	case "sfixed64":
		fd.Type = descriptorpb.FieldDescriptorProto_TYPE_SFIXED64.Enum()
	case "uint32":
		fd.Type = descriptorpb.FieldDescriptorProto_TYPE_UINT32.Enum()
	case "fixed32":
		fd.Type = descriptorpb.FieldDescriptorProto_TYPE_FIXED32.Enum()
	case "uint64":
		fd.Type = descriptorpb.FieldDescriptorProto_TYPE_UINT64.Enum()
	case "fixed64":
		fd.Type = descriptorpb.FieldDescriptorProto_TYPE_FIXED64.Enum()
	case "float":
		fd.Type = descriptorpb.FieldDescriptorProto_TYPE_FLOAT.Enum()
	case "double":
		fd.Type = descriptorpb.FieldDescriptorProto_TYPE_DOUBLE.Enum()
	case "message":
		fd.Type = descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum()
		fd.TypeName = protoString(qualifiedProtoName(protoPackage, field.MessageType))
	case "enum":
		fd.Type = descriptorpb.FieldDescriptorProto_TYPE_ENUM.Enum()
		fd.TypeName = protoString(qualifiedProtoName(protoPackage, field.EnumType))
	default:
		return nil, fmt.Errorf("message: unsupported field kind %q", field.Kind)
	}
	return fd, nil
}

func protobufFieldName(goName string) string {
	if goName == "" {
		return goName
	}
	runes := []rune(goName)
	runes[0] = unicode.ToLower(runes[0])
	return string(runes)
}

func protoString(s string) *string { return &s }
func protoInt32(n int32) *int32    { return &n }
func protoBool(v bool) *bool       { return &v }

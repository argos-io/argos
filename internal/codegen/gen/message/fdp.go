package message

import (
	"fmt"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"

	"github.com/argos-io/argos/internal/codegen/ir"
)

func fileDescriptor(file ir.File) (protoreflect.FileDescriptor, error) {
	if len(file.FileDescriptor) > 0 {
		fdp := &descriptorpb.FileDescriptorProto{}
		if err := proto.Unmarshal(file.FileDescriptor, fdp); err != nil {
			return nil, fmt.Errorf("message: decode file_descriptor: %w", err)
		}
		return protodesc.NewFile(fdp, nil)
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
				InputType:  protoString("." + file.ProtoPackage + "." + method.InputType),
				OutputType: protoString("." + file.ProtoPackage + "." + method.OutputType),
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
	case "int64":
		fd.Type = descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum()
	case "message":
		fd.Type = descriptorpb.FieldDescriptorProto_TYPE_MESSAGE.Enum()
		typeName := field.MessageType
		if !strings.HasPrefix(typeName, ".") {
			typeName = "." + protoPackage + "." + typeName
		}
		fd.TypeName = protoString(typeName)
	case "enum":
		fd.Type = descriptorpb.FieldDescriptorProto_TYPE_ENUM.Enum()
		typeName := field.EnumType
		if !strings.HasPrefix(typeName, ".") {
			typeName = "." + protoPackage + "." + typeName
		}
		fd.TypeName = protoString(typeName)
	default:
		return nil, fmt.Errorf("message: unsupported field kind %q", field.Kind)
	}
	return fd, nil
}

func protobufFieldName(goName string) string {
	if goName == "" {
		return goName
	}
	return strings.ToLower(goName[:1]) + goName[1:]
}

func protoString(s string) *string { return &s }
func protoInt32(n int32) *int32    { return &n }
func protoBool(v bool) *bool       { return &v }

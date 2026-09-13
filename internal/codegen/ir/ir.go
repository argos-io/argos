// Package ir is the intermediate representation for argos stub generation.
package ir

import "fmt"

const (
	Version1 = 1
	Version2 = 2
)

const (
	SourceProto  = "proto"
	SourcePlugin = "plugin"
	SourceIR     = "ir"
)

// File describes generated Go units (*.pb.go / *.msg.go + *.argos.go).
type File struct {
	IRVersion    int       `json:"ir_version,omitempty"`
	Source       string    `json:"source,omitempty"`
	GoPackage    string    `json:"go_package"`
	GoModule     string    `json:"go_module,omitempty"`
	ProtoPackage string    `json:"proto_package,omitempty"`
	InputBase    string    `json:"input_base,omitempty"`
	Outputs      Outputs   `json:"outputs,omitempty"`
	OutputName   string    `json:"output_name,omitempty"` // legacy stub output name
	Messages     []Message `json:"messages,omitempty"`
	Services     []Service `json:"services"`
	// FileDescriptor is a serialized google.protobuf.FileDescriptorProto (proto frontend).
	FileDescriptor []byte `json:"file_descriptor,omitempty"`
}

// Outputs names message and stub artifacts.
type Outputs struct {
	Messages string `json:"messages,omitempty"`
	Stub     string `json:"stub,omitempty"`
}

// Message is one generated message type.
type Message struct {
	GoName string  `json:"go_name"`
	Fields []Field `json:"fields,omitempty"`
}

// Field is one field on a message.
type Field struct {
	GoName      string `json:"go_name"`
	Number      int32  `json:"number"`
	Kind        string `json:"kind"`
	MessageType string `json:"message_type,omitempty"`
	EnumType    string `json:"enum_type,omitempty"`
}

// Service is one protobuf-style service definition.
type Service struct {
	GoName  string   `json:"go_name"`
	Methods []Method `json:"methods"`
}

// Method is one RPC on a service.
type Method struct {
	GoName       string `json:"go_name"`
	FullName     string `json:"full_name"`
	InputType    string `json:"input_type"`
	OutputType   string `json:"output_type"`
	ServerStream bool   `json:"server_stream"`
	ClientStream bool   `json:"client_stream"`
}

// Version returns the IR version (1 when unset and no messages).
func (f *File) Version() int {
	if f.IRVersion != 0 {
		return f.IRVersion
	}
	if len(f.Messages) > 0 || len(f.FileDescriptor) > 0 {
		return Version2
	}
	return Version1
}

// StubName returns the stub output filename.
func (f *File) StubName() string {
	if f.Outputs.Stub != "" {
		return f.Outputs.Stub
	}
	if f.OutputName != "" {
		return f.OutputName
	}
	if f.InputBase != "" {
		return f.InputBase + ".argos.go"
	}
	return ""
}

// MessagesName returns the message output filename.
func (f *File) MessagesName() string {
	if f.Outputs.Messages != "" {
		return f.Outputs.Messages
	}
	if f.InputBase == "" {
		return ""
	}
	switch f.Source {
	case SourceProto:
		return f.InputBase + ".pb.go"
	default:
		return f.InputBase + ".msg.go"
	}
}

// GenerateMessages reports whether message output should be produced.
func (f *File) GenerateMessages() bool {
	return f.MessagesName() != "" && (len(f.Messages) > 0 || len(f.FileDescriptor) > 0)
}

// Validate checks IR consistency for generation.
func (f *File) Validate(fromPlugin bool) error {
	if f.GoPackage == "" {
		return fmt.Errorf("ir: missing go_package")
	}
	if len(f.Services) == 0 {
		return fmt.Errorf("ir: no services")
	}
	if f.StubName() == "" {
		return fmt.Errorf("ir: missing stub output name")
	}
	if fromPlugin && f.Version() >= Version2 && len(f.Messages) == 0 && len(f.FileDescriptor) == 0 {
		return fmt.Errorf("ir: plugin IR v2 must include messages or file_descriptor")
	}
	if f.GenerateMessages() {
		if err := validateMessageRefs(f); err != nil {
			return err
		}
	}
	return nil
}

func validateMessageRefs(f *File) error {
	known := map[string]struct{}{}
	for _, msg := range f.Messages {
		known[msg.GoName] = struct{}{}
	}
	for _, svc := range f.Services {
		for _, method := range svc.Methods {
			if _, ok := known[method.InputType]; !ok && len(f.FileDescriptor) == 0 {
				return fmt.Errorf("ir: unknown input type %q", method.InputType)
			}
			if !method.ServerStream {
				if _, ok := known[method.OutputType]; !ok && len(f.FileDescriptor) == 0 {
					return fmt.Errorf("ir: unknown output type %q", method.OutputType)
				}
			} else if _, ok := known[method.OutputType]; !ok && len(f.FileDescriptor) == 0 {
				return fmt.Errorf("ir: unknown stream output type %q", method.OutputType)
			}
		}
	}
	return nil
}

// Normalize fills default output names and version metadata.
func (f *File) Normalize(fromPlugin bool) {
	if f.Source == "" {
		if fromPlugin {
			f.Source = SourcePlugin
		} else if len(f.FileDescriptor) > 0 {
			f.Source = SourceProto
		} else {
			f.Source = SourceIR
		}
	}
	if f.Outputs.Stub == "" && f.OutputName != "" {
		f.Outputs.Stub = f.OutputName
	}
	if f.Outputs.Stub == "" && f.InputBase != "" {
		f.Outputs.Stub = f.InputBase + ".argos.go"
	}
	if f.OutputName == "" {
		f.OutputName = f.StubName()
	}
	if f.GenerateMessages() && f.Outputs.Messages == "" && f.InputBase != "" {
		if f.Source == SourceProto {
			f.Outputs.Messages = f.InputBase + ".pb.go"
		} else {
			f.Outputs.Messages = f.InputBase + ".msg.go"
		}
	}
	if f.Version() >= Version2 && f.IRVersion == 0 {
		f.IRVersion = Version2
	}
}

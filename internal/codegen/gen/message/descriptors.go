package message

import (
	"fmt"
	"strings"
	"unicode"

	"google.golang.org/protobuf/reflect/protoreflect"
)

// descriptorInfo contains the flattened descriptor order expected by
// protoimpl.TypeBuilder and the Go names used for local declarations.
type descriptorInfo struct {
	file protoreflect.FileDescriptor

	messages      []protoreflect.MessageDescriptor
	messageIndex  map[protoreflect.MessageDescriptor]int
	messageGoName map[protoreflect.MessageDescriptor]string

	enums      []protoreflect.EnumDescriptor
	enumIndex  map[protoreflect.EnumDescriptor]int
	enumGoName map[protoreflect.EnumDescriptor]string
}

func newDescriptorInfo(file protoreflect.FileDescriptor) *descriptorInfo {
	info := &descriptorInfo{
		file:          file,
		messageIndex:  make(map[protoreflect.MessageDescriptor]int),
		messageGoName: make(map[protoreflect.MessageDescriptor]string),
		enumIndex:     make(map[protoreflect.EnumDescriptor]int),
		enumGoName:    make(map[protoreflect.EnumDescriptor]string),
	}
	for i := 0; i < file.Enums().Len(); i++ {
		info.addEnum(file.Enums().Get(i), "")
	}
	for i := 0; i < file.Messages().Len(); i++ {
		info.addMessage(file.Messages().Get(i), "")
	}
	return info
}

func (i *descriptorInfo) addEnum(enum protoreflect.EnumDescriptor, parent string) {
	if _, ok := i.enumIndex[enum]; ok {
		return
	}
	index := len(i.enums)
	i.enums = append(i.enums, enum)
	i.enumIndex[enum] = index
	name := exportGoName(string(enum.Name()))
	if parent != "" {
		name = parent + "_" + name
	}
	i.enumGoName[enum] = name
}

func (i *descriptorInfo) addMessage(message protoreflect.MessageDescriptor, parent string) {
	if _, ok := i.messageIndex[message]; ok {
		return
	}
	index := len(i.messages)
	i.messages = append(i.messages, message)
	i.messageIndex[message] = index
	name := exportGoName(string(message.Name()))
	if parent != "" {
		name = parent + "_" + name
	}
	i.messageGoName[message] = name

	// Map-entry descriptors occupy a MessageInfo slot but do not have a Go
	// struct declaration. They have no user-declared nested types.
	if message.IsMapEntry() {
		return
	}
	for n := 0; n < message.Enums().Len(); n++ {
		i.addEnum(message.Enums().Get(n), name)
	}
	for n := 0; n < message.Messages().Len(); n++ {
		i.addMessage(message.Messages().Get(n), name)
	}
}

func (i *descriptorInfo) generatedMessages() []protoreflect.MessageDescriptor {
	messages := make([]protoreflect.MessageDescriptor, 0, len(i.messages))
	for _, message := range i.messages {
		if !message.IsMapEntry() {
			messages = append(messages, message)
		}
	}
	return messages
}

func (i *descriptorInfo) localMessageType(message protoreflect.MessageDescriptor) (string, error) {
	name, ok := i.messageGoName[message]
	if !ok || message.ParentFile() != i.file {
		return "", fmt.Errorf("message: field references message %q outside the generated file; generate imported message packages separately", message.FullName())
	}
	return name, nil
}

func (i *descriptorInfo) localEnumType(enum protoreflect.EnumDescriptor) (string, error) {
	name, ok := i.enumGoName[enum]
	if !ok || enum.ParentFile() != i.file {
		return "", fmt.Errorf("message: field references enum %q outside the generated file; generate imported enum packages separately", enum.FullName())
	}
	return name, nil
}

func (i *descriptorInfo) messageTypeIndex(message protoreflect.MessageDescriptor) (int32, error) {
	index, ok := i.messageIndex[message]
	if !ok {
		return 0, fmt.Errorf("message: unknown message dependency %q", message.FullName())
	}
	return int32(len(i.enums) + index), nil
}

func (i *descriptorInfo) enumTypeIndex(enum protoreflect.EnumDescriptor) (int32, error) {
	index, ok := i.enumIndex[enum]
	if !ok {
		return 0, fmt.Errorf("message: unknown enum dependency %q", enum.FullName())
	}
	return int32(index), nil
}

func scalarGoType(kind protoreflect.Kind) (string, bool) {
	switch kind {
	case protoreflect.BoolKind:
		return "bool", true
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return "int32", true
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return "int64", true
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return "uint32", true
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return "uint64", true
	case protoreflect.FloatKind:
		return "float32", true
	case protoreflect.DoubleKind:
		return "float64", true
	case protoreflect.StringKind:
		return "string", true
	case protoreflect.BytesKind:
		return "[]byte", true
	default:
		return "", false
	}
}

func goFieldType(field protoreflect.FieldDescriptor, info *descriptorInfo) (string, error) {
	if field.IsMap() {
		entry := field.Message()
		key := entry.Fields().ByName("key")
		value := entry.Fields().ByName("value")
		if key == nil || value == nil {
			return "", fmt.Errorf("message: malformed map field %q", field.FullName())
		}
		keyType, err := goMapFieldType(key, info)
		if err != nil {
			return "", err
		}
		valueType, err := goMapFieldType(value, info)
		if err != nil {
			return "", err
		}
		return "map[" + keyType + "]" + valueType, nil
	}

	typeName, err := goSingleFieldType(field, info)
	if err != nil {
		return "", err
	}
	if field.IsList() {
		return "[]" + typeName, nil
	}
	if field.HasOptionalKeyword() && field.Kind() != protoreflect.MessageKind {
		return "*" + typeName, nil
	}
	return typeName, nil
}

func goMapFieldType(field protoreflect.FieldDescriptor, info *descriptorInfo) (string, error) {
	if field.IsList() || field.IsMap() || field.HasOptionalKeyword() {
		return "", fmt.Errorf("message: invalid map entry field %q", field.FullName())
	}
	return goSingleFieldType(field, info)
}

func goSingleFieldType(field protoreflect.FieldDescriptor, info *descriptorInfo) (string, error) {
	if scalar, ok := scalarGoType(field.Kind()); ok {
		return scalar, nil
	}
	switch field.Kind() {
	case protoreflect.EnumKind:
		return info.localEnumType(field.Enum())
	case protoreflect.MessageKind, protoreflect.GroupKind:
		name, err := info.localMessageType(field.Message())
		if err != nil {
			return "", err
		}
		return "*" + name, nil
	default:
		return "", fmt.Errorf("message: unsupported field kind %s", field.Kind())
	}
}

func messagePath(message protoreflect.MessageDescriptor) []int {
	if parent := message.Parent(); parent != nil {
		if parentMessage, ok := parent.(protoreflect.MessageDescriptor); ok {
			path := messagePath(parentMessage)
			return append(path, messageDescriptorIndex(parentMessage.Messages(), message))
		}
	}
	return []int{messageDescriptorIndex(message.ParentFile().Messages(), message)}
}

func enumPath(enum protoreflect.EnumDescriptor) []int {
	if parent := enum.Parent(); parent != nil {
		if parentMessage, ok := parent.(protoreflect.MessageDescriptor); ok {
			path := messagePath(parentMessage)
			return append(path, enumDescriptorIndex(parentMessage.Enums(), enum))
		}
	}
	return []int{enumDescriptorIndex(enum.ParentFile().Enums(), enum)}
}

func messageDescriptorIndex(list protoreflect.MessageDescriptors, target protoreflect.MessageDescriptor) int {
	for n := 0; n < list.Len(); n++ {
		if list.Get(n) == target {
			return n
		}
	}
	return -1
}

func enumDescriptorIndex(list protoreflect.EnumDescriptors, target protoreflect.EnumDescriptor) int {
	for n := 0; n < list.Len(); n++ {
		if list.Get(n) == target {
			return n
		}
	}
	return -1
}

func enumConstantPrefix(enum protoreflect.EnumDescriptor, info *descriptorInfo) string {
	if parent, ok := enum.Parent().(protoreflect.MessageDescriptor); ok {
		return info.messageGoName[parent]
	}
	return info.enumGoName[enum]
}

func enumConstantName(enum protoreflect.EnumDescriptor, value protoreflect.EnumValueDescriptor, info *descriptorInfo) string {
	// Keep the proto value spelling. Besides matching protoc-gen-go, this
	// preserves meaningful separators such as TOP_STATE_READY. The type
	// prefix already makes the package-level identifier exported, even when
	// the value itself is lower-case.
	return enumConstantPrefix(enum, info) + "_" + string(value.Name())
}

func exportGoName(name string) string {
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
	if b.Len() == 0 {
		return "_"
	}
	return b.String()
}

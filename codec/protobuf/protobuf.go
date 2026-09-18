// Package protobuf implements codec.Codec with binary protobuf encoding.
package protobuf

import (
	"fmt"

	"github.com/argos-io/argos/codec"
	"google.golang.org/protobuf/proto"
)

func init() {
	codec.Register("protobuf", func() (codec.Codec, error) { return New(), nil })
}

type codecImpl struct{}

// New returns a Codec that uses binary protobuf.
func New() codec.Codec { return codecImpl{} }

func (codecImpl) CodecName() string { return "protobuf" }

func (codecImpl) Marshal(v any) ([]byte, error) {
	m, err := asMessage(v)
	if err != nil {
		return nil, err
	}
	return proto.Marshal(m)
}

// Unmarshal decodes b into v. b is borrowed only until return; the decoded
// message must not retain aliases of b (proto.Unmarshal copies by default).
func (codecImpl) Unmarshal(b []byte, v any) error {
	m, err := asMessage(v)
	if err != nil {
		return err
	}
	return proto.Unmarshal(b, m)
}

func asMessage(v any) (proto.Message, error) {
	m, ok := v.(proto.Message)
	if !ok {
		return nil, fmt.Errorf("codec/protobuf: %T is not a proto.Message; the binding Codec does not match the message model of these generated types", v)
	}
	return m, nil
}

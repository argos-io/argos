// Package json implements codec.Codec with protobuf's JSON mapping (protojson).
package json

import (
	"fmt"

	"github.com/argos-io/argos/codec"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type codecImpl struct{}

// New returns a Codec that uses protobuf's JSON mapping (protojson).
func New() codec.Codec { return codecImpl{} }

func (codecImpl) CodecName() string { return "json" }

func (codecImpl) Marshal(v any) ([]byte, error) {
	m, err := asMessage(v)
	if err != nil {
		return nil, err
	}
	return protojson.Marshal(m)
}

// Unmarshal decodes b into v. b is borrowed only until return; the decoded
// message must not retain aliases of b.
func (codecImpl) Unmarshal(b []byte, v any) error {
	m, err := asMessage(v)
	if err != nil {
		return err
	}
	return protojson.Unmarshal(b, m)
}

func asMessage(v any) (proto.Message, error) {
	m, ok := v.(proto.Message)
	if !ok {
		return nil, fmt.Errorf("codec/json: %T is not a proto.Message; the binding Codec does not match the message model of these generated types", v)
	}
	return m, nil
}

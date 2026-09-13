package protobuf

import (
	"io"

	"github.com/argos-io/argos/codec"
	"github.com/argos-io/argos/errs"
	"google.golang.org/protobuf/proto"
)

type codecImpl struct{}

// New returns a Codec that uses binary protobuf.
func New() codec.Codec { return codecImpl{} }

func init() {
	codec.Register("protobuf", New)
}

func (codecImpl) Marshal(w io.Writer, v any) error {
	m, err := asMessage(v)
	if err != nil {
		return err
	}
	b, err := proto.Marshal(m)
	if err != nil {
		return err
	}
	_, err = w.Write(b)
	return err
}

func (codecImpl) Unmarshal(r io.Reader, v any) error {
	m, err := asMessage(v)
	if err != nil {
		return err
	}
	b, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	return proto.Unmarshal(b, m)
}

func asMessage(v any) (proto.Message, error) {
	m, ok := v.(proto.Message)
	if !ok {
		return nil, errs.Error(errs.Internal, "codec/protobuf: value is not proto.Message")
	}
	return m, nil
}

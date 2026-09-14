package json

import (
	"io"

	"github.com/argos-io/argos/codec"
	"github.com/argos-io/argos/errs"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type codecImpl struct{}

// New returns a Codec that uses protobuf's JSON mapping (protojson).
func New() codec.Codec { return codecImpl{} }

func (codecImpl) CodecName() string { return "json" }

func init() {
	codec.Register("json", New)
}

func (codecImpl) Marshal(w io.Writer, v any) error {
	m, err := asMessage(v)
	if err != nil {
		return err
	}
	b, err := protojson.Marshal(m)
	if err != nil {
		return err
	}
	n, err := w.Write(b)
	if err != nil {
		return err
	}
	if n != len(b) {
		return io.ErrShortWrite
	}
	return nil
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
	return protojson.Unmarshal(b, m)
}

func asMessage(v any) (proto.Message, error) {
	m, ok := v.(proto.Message)
	if !ok {
		return nil, errs.Error(errs.Internal, "codec/json: value is not proto.Message")
	}
	return m, nil
}

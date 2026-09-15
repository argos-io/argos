package envelope

import (
	"bytes"
	"encoding/binary"
	"testing"
)

// maxFuzzPrefixedBody caps hostile length prefixes that would force
// UnmarshalPrefixed to allocate far beyond the fuzz input size.
// Production MaxFrameSize gating is separate; fuzz only must not crash/OOM.
const maxFuzzPrefixedBody = 1 << 20

func seedGoldenFrames(f *testing.F) {
	f.Helper()
	frames := []Frame{
		{
			Type:   TypeOpen,
			CallID: 0x0102030405060708,
			Method: "/svc/Echo",
			Flags:  0,
			Headers: []Header{
				{Name: "k", Value: "v"},
			},
		},
		{
			Type:   TypeOpen,
			CallID: 42,
			Method: "/svc/Empty",
			Flags:  FlagOpenEnd,
		},
		{
			Type:   TypeHeaders,
			CallID: 7,
			Headers: []Header{
				{Name: "content-type", Value: "application/json"},
				{Name: "x-trace", Value: "abc"},
			},
		},
		{
			Type:   TypeData,
			CallID: 9,
			Data:   []byte("hello"),
		},
		{
			Type:   TypeData,
			CallID: 9,
			Data:   []byte{},
		},
		{
			Type:   TypeEnd,
			CallID: 9,
		},
		{
			Type:    TypeStatus,
			CallID:  99,
			Code:    5,
			Message: "missing",
			Headers: []Header{
				{Name: "grpc-status-details-bin", Value: "x"},
			},
		},
		{
			Type:   TypeStatus,
			CallID: 1,
			Code:   0,
		},
		// Hex fixtures from frame_test (body only / full prefixed).
		{Type: TypeOpen, CallID: 1, Flags: 0, Method: ""},
		{Type: TypeOpen, CallID: 2, Flags: FlagOpenEnd, Method: "m"},
		{Type: TypeStatus, CallID: 99, Code: 13, Message: "err"},
		{
			Type:   TypeStatus,
			CallID: 1,
			Code:   0,
			Headers: []Header{
				{Name: "a", Value: "b"},
			},
		},
	}
	for _, fr := range frames {
		body, err := MarshalFrameBody(fr)
		if err != nil {
			f.Fatalf("MarshalFrameBody: %v", err)
		}
		f.Add(body)
		prefixed, err := MarshalFrame(fr)
		if err != nil {
			f.Fatalf("MarshalFrame: %v", err)
		}
		f.Add(prefixed)
	}
	// Truncation / garbage seeds from frame_test.
	f.Add([]byte(nil))
	f.Add([]byte{})
	f.Add([]byte{1})
	f.Add([]byte{0, 0, 0, 100})
	f.Add([]byte{0, 0, 0xff, 0xff, 1, 2, 3})
}

func FuzzParseFrameBody(f *testing.F) {
	seedGoldenFrames(f)
	f.Fuzz(func(t *testing.T, data []byte) {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("ParseFrameBody panic on %x: %v", data, r)
			}
		}()
		_, _ = ParseFrameBody(data)
	})
}

func FuzzUnmarshalPrefixed(f *testing.F) {
	seedGoldenFrames(f)
	f.Fuzz(func(t *testing.T, data []byte) {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("UnmarshalPrefixed panic on %x: %v", data, r)
			}
		}()
		if len(data) >= 4 {
			n := int(binary.BigEndian.Uint32(data[:4]))
			rest := len(data) - 4
			// Skip inputs that claim a body larger than available bytes and
			// larger than the fuzz budget — make([]byte, n) would OOM.
			if n > rest && n > maxFuzzPrefixedBody {
				return
			}
		}
		_, _ = UnmarshalPrefixed(bytes.NewReader(data))
	})
}

func FuzzParseMetadata(f *testing.F) {
	metaSeeds := [][]Header{
		nil,
		{},
		{{Name: "k", Value: "v"}},
		{
			{Name: "content-type", Value: "application/json"},
			{Name: "x-trace", Value: "abc"},
		},
		{{Name: "a", Value: "b"}},
		{{Name: "grpc-status-details-bin", Value: "x"}},
	}
	for _, hdrs := range metaSeeds {
		raw, err := appendMetadata(nil, hdrs)
		if err != nil {
			f.Fatalf("appendMetadata: %v", err)
		}
		f.Add(raw)
	}
	f.Add([]byte(nil))
	f.Add([]byte{})
	f.Add([]byte{0, 1}) // count=1, no pairs
	f.Add([]byte{0xff, 0xff})

	f.Fuzz(func(t *testing.T, data []byte) {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("parseMetadata panic on %x: %v", data, r)
			}
		}()
		_, _, _ = parseMetadata(data)
	})
}

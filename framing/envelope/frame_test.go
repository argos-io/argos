package envelope_test

import (
	"bytes"
	"encoding/hex"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/argos-io/argos/framing/envelope"
)

func mustDecodeHex(t *testing.T, s string) []byte {
	t.Helper()
	s = strings.ReplaceAll(s, " ", "")
	s = strings.ReplaceAll(s, "\n", "")
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatalf("hex: %v", err)
	}
	return b
}

func TestGoldenRoundTrip(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		frame envelope.Frame
	}{
		{
			name: "OPEN",
			frame: envelope.Frame{
				Type:   envelope.TypeOpen,
				CallID: 0x0102030405060708,
				Method: "/svc/Echo",
				Flags:  0,
				Headers: []envelope.Header{
					{Name: "k", Value: "v"},
				},
			},
		},
		{
			name: "OPEN|END",
			frame: envelope.Frame{
				Type:   envelope.TypeOpen,
				CallID: 42,
				Method: "/svc/Empty",
				Flags:  envelope.FlagOpenEnd,
			},
		},
		{
			name: "HEADERS",
			frame: envelope.Frame{
				Type:   envelope.TypeHeaders,
				CallID: 7,
				Headers: []envelope.Header{
					{Name: "content-type", Value: "application/json"},
					{Name: "x-trace", Value: "abc"},
				},
			},
		},
		{
			name: "DATA",
			frame: envelope.Frame{
				Type:   envelope.TypeData,
				CallID: 9,
				Data:   []byte("hello"),
			},
		},
		{
			name: "DATA_empty",
			frame: envelope.Frame{
				Type:   envelope.TypeData,
				CallID: 9,
				Data:   []byte{},
			},
		},
		{
			name: "END",
			frame: envelope.Frame{
				Type:   envelope.TypeEnd,
				CallID: 9,
			},
		},
		{
			name: "STATUS",
			frame: envelope.Frame{
				Type:    envelope.TypeStatus,
				CallID:  99,
				Code:    5, // NotFound
				Message: "missing",
				Headers: []envelope.Header{
					{Name: "grpc-status-details-bin", Value: "x"},
				},
			},
		},
		{
			name: "STATUS_ok_empty",
			frame: envelope.Frame{
				Type:   envelope.TypeStatus,
				CallID: 1,
				Code:   0,
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			prefixed, err := envelope.MarshalFrame(tc.frame)
			if err != nil {
				t.Fatalf("MarshalFrame: %v", err)
			}
			got, err := envelope.UnmarshalPrefixed(bytes.NewReader(prefixed))
			if err != nil {
				t.Fatalf("UnmarshalPrefixed: %v", err)
			}
			assertFrameEqual(t, got, tc.frame)

			body, err := envelope.MarshalFrameBody(tc.frame)
			if err != nil {
				t.Fatalf("MarshalFrameBody: %v", err)
			}
			got2, err := envelope.ParseFrameBody(body)
			if err != nil {
				t.Fatalf("ParseFrameBody: %v", err)
			}
			assertFrameEqual(t, got2, tc.frame)
		})
	}
}

func TestOpenEndDistinctFromEmptyData(t *testing.T) {
	t.Parallel()
	openEnd := envelope.Frame{
		Type:   envelope.TypeOpen,
		CallID: 1,
		Method: "/m",
		Flags:  envelope.FlagOpenEnd,
	}
	openThenEmptyData := []envelope.Frame{
		{Type: envelope.TypeOpen, CallID: 1, Method: "/m"},
		{Type: envelope.TypeData, CallID: 1, Data: []byte{}},
		{Type: envelope.TypeEnd, CallID: 1},
	}

	bOpenEnd, err := envelope.MarshalFrameBody(openEnd)
	if err != nil {
		t.Fatal(err)
	}
	if openEnd.Flags&envelope.FlagOpenEnd == 0 {
		t.Fatal("OPEN|END must set FlagOpenEnd")
	}
	parsed, err := envelope.ParseFrameBody(bOpenEnd)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Flags&envelope.FlagOpenEnd == 0 {
		t.Fatal("parsed OPEN|END lost FlagOpenEnd")
	}
	if len(parsed.Data) != 0 && parsed.Data != nil {
		// Data must remain unset for OPEN
		t.Fatalf("OPEN must not carry Data, got %v", parsed.Data)
	}

	var batch []byte
	for _, f := range openThenEmptyData {
		batch, err = envelope.AppendFrame(batch, f)
		if err != nil {
			t.Fatal(err)
		}
	}
	if bytes.Equal(bOpenEnd, batch) {
		t.Fatal("OPEN|END bytes must differ from OPEN + empty DATA + END batch")
	}

	// Empty DATA alone is a distinct empty message frame.
	emptyData, err := envelope.MarshalFrameBody(envelope.Frame{
		Type: envelope.TypeData, CallID: 1, Data: []byte{},
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err := envelope.ParseFrameBody(emptyData)
	if err != nil {
		t.Fatal(err)
	}
	if got.Type != envelope.TypeData {
		t.Fatalf("type = %d", got.Type)
	}
	if got.Data == nil || len(got.Data) != 0 {
		t.Fatalf("empty DATA must round-trip as empty slice, got %#v", got.Data)
	}
	if got.Flags&envelope.FlagOpenEnd != 0 {
		t.Fatal("DATA must not look like FlagOpenEnd")
	}
}

func TestHexFixtures(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		hex  string // length-prefixed wire bytes
		want envelope.Frame
	}{
		{
			// body = typ=1 callID=1 flags=0 method="" metadata count=0
			// body len = 1+8+1+2+2 = 14 = 0x0000000e
			name: "OPEN_minimal",
			hex:  "0000000e" + "01" + "0000000000000001" + "00" + "0000" + "0000",
			want: envelope.Frame{
				Type:   envelope.TypeOpen,
				CallID: 1,
				Flags:  0,
				Method: "",
			},
		},
		{
			// OPEN|END with method "m" (len 1), no headers
			// body = 1+8+1+2+1+2 = 15
			name: "OPEN_END_method_m",
			hex:  "0000000f" + "01" + "0000000000000002" + "01" + "0001" + "6d" + "0000",
			want: envelope.Frame{
				Type:   envelope.TypeOpen,
				CallID: 2,
				Flags:  envelope.FlagOpenEnd,
				Method: "m",
			},
		},
		{
			// STATUS code=13 msg="err" trailers empty
			// body = 1+8+4+2+3+2 = 20
			name: "STATUS_internal",
			hex: "00000014" + "05" + "0000000000000063" +
				"0000000d" + "0003" + "657272" + "0000",
			want: envelope.Frame{
				Type:    envelope.TypeStatus,
				CallID:  99,
				Code:    13,
				Message: "err",
			},
		},
		{
			// STATUS with one trailer a=b
			// body = 1+8+4+2+0 + 2+2+1+2+1 = 23
			name: "STATUS_with_trailer",
			hex: "00000017" + "05" + "0000000000000001" +
				"00000000" + "0000" +
				"0001" + "0001" + "61" + "0001" + "62",
			want: envelope.Frame{
				Type:   envelope.TypeStatus,
				CallID: 1,
				Code:   0,
				Headers: []envelope.Header{
					{Name: "a", Value: "b"},
				},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			raw := mustDecodeHex(t, tc.hex)
			got, err := envelope.UnmarshalPrefixed(bytes.NewReader(raw))
			if err != nil {
				t.Fatalf("UnmarshalPrefixed: %v", err)
			}
			assertFrameEqual(t, got, tc.want)

			enc, err := envelope.MarshalFrame(tc.want)
			if err != nil {
				t.Fatalf("MarshalFrame: %v", err)
			}
			if !bytes.Equal(enc, raw) {
				t.Fatalf("marshal mismatch\n got %x\nwant %x", enc, raw)
			}
		})
	}
}

func TestTruncationAndBadLengths(t *testing.T) {
	t.Parallel()
	good, err := envelope.MarshalFrame(envelope.Frame{
		Type:   envelope.TypeOpen,
		CallID: 1,
		Method: "Echo",
		Headers: []envelope.Header{
			{Name: "n", Value: "v"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	t.Run("short_prefix", func(t *testing.T) {
		_, err := envelope.UnmarshalPrefixed(bytes.NewReader(good[:3]))
		if err == nil {
			t.Fatal("expected error")
		}
		if !errors.Is(err, envelope.ErrTruncated) && !errors.Is(err, io.ErrUnexpectedEOF) {
			// wrapped ErrTruncated
			if !strings.Contains(err.Error(), "truncated") && !errors.Is(err, io.EOF) {
				t.Fatalf("want truncated, got %v", err)
			}
		}
	})

	t.Run("short_body", func(t *testing.T) {
		_, err := envelope.UnmarshalPrefixed(bytes.NewReader(good[:len(good)-1]))
		if err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("body_len_too_small", func(t *testing.T) {
		buf := []byte{0, 0, 0, 8, 1, 0, 0, 0, 0, 0, 0, 0} // length 8 < 9
		_, err := envelope.UnmarshalPrefixed(bytes.NewReader(buf))
		if err == nil || !errors.Is(err, envelope.ErrInvalidLength) {
			t.Fatalf("want ErrInvalidLength, got %v", err)
		}
	})

	t.Run("truncated_method", func(t *testing.T) {
		body, err := envelope.MarshalFrameBody(envelope.Frame{
			Type: envelope.TypeOpen, CallID: 1, Method: "ab",
		})
		if err != nil {
			t.Fatal(err)
		}
		// Drop last method byte.
		_, err = envelope.ParseFrameBody(body[:len(body)-1])
		if err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("bad_metadata_count", func(t *testing.T) {
		// typ + callID + flags + method"" + count=1 but no pairs
		body := []byte{
			1, 0, 0, 0, 0, 0, 0, 0, 1, // typ+callID
			0,    // flags
			0, 0, // method len 0
			0, 1, // count 1
		}
		_, err := envelope.ParseFrameBody(body)
		if err == nil {
			t.Fatal("expected error")
		}
	})

	t.Run("unknown_type", func(t *testing.T) {
		body := make([]byte, 9)
		body[0] = 99
		_, err := envelope.ParseFrameBody(body)
		if err == nil || !errors.Is(err, envelope.ErrUnknownType) {
			t.Fatalf("want ErrUnknownType, got %v", err)
		}
	})

	t.Run("END_with_payload", func(t *testing.T) {
		body := []byte{4, 0, 0, 0, 0, 0, 0, 0, 1, 0xff}
		_, err := envelope.ParseFrameBody(body)
		if err == nil || !errors.Is(err, envelope.ErrEndPayload) {
			t.Fatalf("want ErrEndPayload, got %v", err)
		}
	})

	t.Run("no_panic_on_garbage", func(t *testing.T) {
		garbage := [][]byte{
			nil,
			{},
			{1},
			{0, 0, 0, 100},
			mustDecodeHex(t, "0000ffff010203"),
		}
		for _, g := range garbage {
			func() {
				defer func() {
					if r := recover(); r != nil {
						t.Fatalf("panic on %x: %v", g, r)
					}
				}()
				_, _ = envelope.ParseFrameBody(g)
				_, _ = envelope.UnmarshalPrefixed(bytes.NewReader(g))
			}()
		}
	})
}

func TestDeepCopyOnParse(t *testing.T) {
	t.Parallel()
	body, err := envelope.MarshalFrameBody(envelope.Frame{
		Type:   envelope.TypeData,
		CallID: 1,
		Data:   []byte("xyz"),
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err := envelope.ParseFrameBody(body)
	if err != nil {
		t.Fatal(err)
	}
	got.Data[0] = 'X'
	if body[9] != 'x' {
		t.Fatal("ParseFrameBody Data aliases input")
	}

	openBody, err := envelope.MarshalFrameBody(envelope.Frame{
		Type:   envelope.TypeOpen,
		CallID: 1,
		Method: "ab",
		Headers: []envelope.Header{
			{Name: "k", Value: "v"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	gotOpen, err := envelope.ParseFrameBody(openBody)
	if err != nil {
		t.Fatal(err)
	}
	// Mutating returned strings is not possible in Go (immutable), but
	// mutating Headers slice must not affect a second parse.
	gotOpen.Headers[0].Name = "mutated"
	gotOpen2, err := envelope.ParseFrameBody(openBody)
	if err != nil {
		t.Fatal(err)
	}
	if gotOpen2.Headers[0].Name != "k" {
		t.Fatal("header name mutated across parses")
	}
}

func TestValidateLimits(t *testing.T) {
	t.Parallel()
	f := envelope.Frame{
		Type:   envelope.TypeOpen,
		CallID: 1,
		Method: "m",
		Headers: []envelope.Header{
			{Name: "name", Value: "value"},
		},
	}
	if err := f.Validate(0, 0); err != nil {
		t.Fatalf("disabled limits: %v", err)
	}
	if err := f.Validate(1, 0); err == nil || !errors.Is(err, envelope.ErrFrameTooLarge) {
		t.Fatalf("want ErrFrameTooLarge, got %v", err)
	}
	if err := f.Validate(0, 1); err == nil || !errors.Is(err, envelope.ErrMetaTooLarge) {
		t.Fatalf("want ErrMetaTooLarge, got %v", err)
	}
}

func TestAppendFrameBatch(t *testing.T) {
	t.Parallel()
	frames := []envelope.Frame{
		{Type: envelope.TypeOpen, CallID: 1, Method: "/m", Flags: 0},
		{Type: envelope.TypeData, CallID: 1, Data: []byte("x")},
		{Type: envelope.TypeEnd, CallID: 1},
	}
	var dst, want []byte
	for _, f := range frames {
		var err error
		dst, err = envelope.AppendFrame(dst, f)
		if err != nil {
			t.Fatal(err)
		}
		body, err := envelope.MarshalFrameBody(f)
		if err != nil {
			t.Fatal(err)
		}
		want = append(want, body...)
	}
	if !bytes.Equal(dst, want) {
		t.Fatalf("AppendFrame batch\n got %x\nwant %x", dst, want)
	}
}

func assertFrameEqual(t *testing.T, got, want envelope.Frame) {
	t.Helper()
	if got.Type != want.Type {
		t.Fatalf("Type: got %d want %d", got.Type, want.Type)
	}
	if got.CallID != want.CallID {
		t.Fatalf("CallID: got %d want %d", got.CallID, want.CallID)
	}
	if got.Flags != want.Flags {
		t.Fatalf("Flags: got %d want %d", got.Flags, want.Flags)
	}
	if got.Method != want.Method {
		t.Fatalf("Method: got %q want %q", got.Method, want.Method)
	}
	if got.Code != want.Code {
		t.Fatalf("Code: got %d want %d", got.Code, want.Code)
	}
	if got.Message != want.Message {
		t.Fatalf("Message: got %q want %q", got.Message, want.Message)
	}
	if len(got.Headers) != len(want.Headers) {
		t.Fatalf("Headers len: got %d want %d", len(got.Headers), len(want.Headers))
	}
	for i := range want.Headers {
		if got.Headers[i] != want.Headers[i] {
			t.Fatalf("Headers[%d]: got %+v want %+v", i, got.Headers[i], want.Headers[i])
		}
	}
	if want.Type == envelope.TypeData {
		if !bytes.Equal(got.Data, want.Data) {
			t.Fatalf("Data: got %q want %q", got.Data, want.Data)
		}
		// Empty DATA must be a non-nil empty slice after parse for clarity.
		if want.Data != nil && len(want.Data) == 0 && got.Data == nil {
			t.Fatal("empty DATA parsed as nil; want empty slice")
		}
	}
}

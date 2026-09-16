package grpc_test

import (
	"encoding/base64"
	"math"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/framing"
	grpcframing "github.com/argos-io/argos/framing/grpc"
	"github.com/argos-io/argos/metadata"
	"github.com/argos-io/argos/transport"
)

func TestReuseConcurrent(t *testing.T) {
	t.Parallel()
	f, err := grpcframing.New()
	if err != nil {
		t.Fatal(err)
	}
	if got := f.Reuse(); got != framing.Concurrent {
		t.Fatalf("Reuse() = %v, want Concurrent", got)
	}
}

func TestMethodPath(t *testing.T) {
	t.Parallel()
	m := descriptor.MustMethod("echo.v1.EchoService.Echo", descriptor.Unary)
	got := grpcframing.MethodPath(m)
	want := "/echo.v1.EchoService/Echo"
	if got != want {
		t.Fatalf("MethodPath = %q, want %q", got, want)
	}
}

func TestParseMethodPath(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		path        string
		wantService string
		wantMethod  string
		wantErr     bool
	}{
		{"/echo.v1.EchoService/Echo", "echo.v1.EchoService", "Echo", false},
		{"/pkg.Svc/Method", "pkg.Svc", "Method", false},
		{"Echo", "", "", true},
		{"/Echo", "", "", true},
		{"/", "", "", true},
		{"//Echo", "", "", true},
		{"/svc/", "", "", true},
	} {
		svc, meth, err := grpcframing.ParseMethodPath(tc.path)
		if tc.wantErr {
			if err == nil {
				t.Errorf("ParseMethodPath(%q) err=nil, want error", tc.path)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParseMethodPath(%q): %v", tc.path, err)
			continue
		}
		if svc != tc.wantService || meth != tc.wantMethod {
			t.Errorf("ParseMethodPath(%q) = (%q,%q), want (%q,%q)",
				tc.path, svc, meth, tc.wantService, tc.wantMethod)
		}
	}
}

func TestContentTypeAndSubtype(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		subtype string
		wantCT  string
	}{
		{"", "application/grpc"},
		{"proto", "application/grpc+proto"},
		{"json", "application/grpc+json"},
	} {
		if got := grpcframing.ContentType(tc.subtype); got != tc.wantCT {
			t.Errorf("ContentType(%q) = %q, want %q", tc.subtype, got, tc.wantCT)
		}
		gotSub, ok := grpcframing.ContentSubtype(tc.wantCT)
		if !ok || gotSub != tc.subtype {
			t.Errorf("ContentSubtype(%q) = (%q,%v), want (%q,true)", tc.wantCT, gotSub, ok, tc.subtype)
		}
	}
	if _, ok := grpcframing.ContentSubtype("application/json"); ok {
		t.Fatal("ContentSubtype(application/json) should be invalid")
	}
	if _, ok := grpcframing.ContentSubtype("application/grpc-web"); ok {
		t.Fatal("ContentSubtype(application/grpc-web) should be invalid")
	}
	sub, ok := grpcframing.ContentSubtype("application/grpc;charset=utf-8")
	if !ok || sub != "charset=utf-8" {
		t.Fatalf("ContentSubtype(; form) = (%q,%v)", sub, ok)
	}
}

func TestCodecContentSubtype(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		in, want string
	}{
		{"", "proto"},
		{"protobuf", "proto"},
		{"proto", "proto"},
		{"json", "json"},
		{"JSON", "json"},
		{"custom", "custom"},
	} {
		if got := grpcframing.CodecContentSubtype(tc.in); got != tc.want {
			t.Errorf("CodecContentSubtype(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestEncodeDecodeTimeout(t *testing.T) {
	t.Parallel()
	// Encode golden cases matching grpc-go EncodeDuration.
	for _, tc := range []struct {
		in  string
		out string
	}{
		{"12345678ns", "12345678n"},
		{"123456789ns", "123457u"},
		{"12345678us", "12345678u"},
		{"123456789us", "123457m"},
		{"12345678ms", "12345678m"},
		{"123456789ms", "123457S"},
		{"12345678s", "12345678S"},
		{"123456789s", "2057614M"},
		{"12345678m", "12345678M"},
		{"123456789m", "2057614H"},
	} {
		d, err := time.ParseDuration(tc.in)
		if err != nil {
			t.Fatalf("ParseDuration(%s): %v", tc.in, err)
		}
		got := grpcframing.EncodeTimeout(d)
		if got != tc.out {
			t.Errorf("EncodeTimeout(%s) = %q, want %q", tc.in, got, tc.out)
		}
	}
	if got := grpcframing.EncodeTimeout(0); got != "0n" {
		t.Errorf("EncodeTimeout(0) = %q, want 0n", got)
	}
	if got := grpcframing.EncodeTimeout(-time.Second); got != "0n" {
		t.Errorf("EncodeTimeout(-1s) = %q, want 0n", got)
	}

	// Decode golden cases matching grpc-go decodeTimeout.
	for _, tc := range []struct {
		s       string
		d       time.Duration
		wantErr bool
	}{
		{"00000001n", time.Nanosecond, false},
		{"10u", time.Microsecond * 10, false},
		{"00000010m", time.Millisecond * 10, false},
		{"1234S", time.Second * 1234, false},
		{"00000001M", time.Minute, false},
		{"09999999S", time.Second * 9999999, false},
		{"99999999S", time.Second * 99999999, false},
		{"99999999M", time.Minute * 99999999, false},
		{"2562047H", time.Hour * 2562047, false},
		{"2562048H", time.Duration(math.MaxInt64), false},
		{"99999999H", time.Duration(math.MaxInt64), false},
		{"-1S", 0, true},
		{"1234x", 0, true},
		{"1234s", 0, true},
		{"1234", 0, true},
		{"1", 0, true},
		{"", 0, true},
		{"9a1S", 0, true},
		{"0S", 0, false},
		{"00000000S", 0, false},
		{"000000000S", 0, true},
	} {
		d, err := grpcframing.DecodeTimeout(tc.s)
		gotErr := err != nil
		if d != tc.d || gotErr != tc.wantErr {
			t.Errorf("DecodeTimeout(%q) = %d, err=%v; want %d, wantErr=%v",
				tc.s, int64(d), err, int64(tc.d), tc.wantErr)
		}
	}
}

func TestReservedHeaders(t *testing.T) {
	t.Parallel()
	reserved := []string{
		":path", ":authority", ":method", ":scheme", ":status",
		"content-type", "user-agent", "te",
		"grpc-timeout", "grpc-status", "grpc-message",
		"grpc-encoding", "grpc-message-type",
	}
	for _, h := range reserved {
		if !grpcframing.IsReservedHeader(h) {
			t.Errorf("IsReservedHeader(%q) = false, want true", h)
		}
	}
	// Intentionally not reserved (grpc-go): may travel as user metadata.
	notReserved := []string{
		"grpc-previous-rpc-attempts",
		"grpc-retry-pushback-ms",
		"grpc-accept-encoding",
		"x-custom",
		"authorization",
	}
	for _, h := range notReserved {
		if grpcframing.IsReservedHeader(h) {
			t.Errorf("IsReservedHeader(%q) = true, want false", h)
		}
	}
	if !grpcframing.IsWhitelistedHeader(":authority") || !grpcframing.IsWhitelistedHeader("user-agent") {
		t.Fatal("whitelist missing :authority or user-agent")
	}
	if grpcframing.IsWhitelistedHeader("content-type") {
		t.Fatal("content-type must not be whitelisted into user metadata")
	}
}

func TestBinaryMetadataEncodeDecode(t *testing.T) {
	t.Parallel()
	raw := string([]byte{0x00, 0xff, 0xfe, 'a', ','})
	md := metadata.Metadata{
		"x-trace-bin": {raw},
		"x-ascii":     {"hello"},
	}
	hs := grpcframing.EncodeMetadata(md)
	byName := headersByName(hs)
	if len(byName["x-trace-bin"]) != 1 {
		t.Fatalf("x-trace-bin headers = %v", byName["x-trace-bin"])
	}
	wire := byName["x-trace-bin"][0]
	// Unpadded base64 on the wire.
	if strings.Contains(wire, "=") {
		t.Fatalf("encoded -bin must be unpadded, got %q", wire)
	}
	wantWire := base64.RawStdEncoding.EncodeToString([]byte(raw))
	if wire != wantWire {
		t.Fatalf("x-trace-bin wire = %q, want %q", wire, wantWire)
	}
	if byName["x-ascii"][0] != "hello" {
		t.Fatalf("x-ascii = %q", byName["x-ascii"])
	}

	got, err := grpcframing.DecodeMetadata(hs)
	if err != nil {
		t.Fatalf("DecodeMetadata: %v", err)
	}
	if got["x-trace-bin"][0] != raw {
		t.Fatalf("round-trip -bin = %q, want %q", got["x-trace-bin"][0], raw)
	}

	// Padded accept on decode.
	padded := base64.StdEncoding.EncodeToString([]byte(raw))
	got2, err := grpcframing.DecodeMetadata(transport.Headers{
		{Name: "x-trace-bin", Value: padded},
	})
	if err != nil {
		t.Fatalf("DecodeMetadata padded: %v", err)
	}
	if got2["x-trace-bin"][0] != raw {
		t.Fatalf("padded decode = %q, want %q", got2["x-trace-bin"][0], raw)
	}

	// Comma-joined -bin values split on decode (§7.4).
	a := base64.RawStdEncoding.EncodeToString([]byte("one"))
	b := base64.RawStdEncoding.EncodeToString([]byte("two"))
	got3, err := grpcframing.DecodeMetadata(transport.Headers{
		{Name: "x-multi-bin", Value: a + "," + b},
	})
	if err != nil {
		t.Fatalf("DecodeMetadata comma: %v", err)
	}
	if !reflect.DeepEqual(got3["x-multi-bin"], []string{"one", "two"}) {
		t.Fatalf("comma-split = %v, want [one two]", got3["x-multi-bin"])
	}
}

func TestReservedKeysStrippedOnEncode(t *testing.T) {
	t.Parallel()
	md := metadata.Metadata{
		"content-type":  {"application/grpc"},
		"te":            {"trailers"},
		"grpc-timeout":  {"1S"},
		"grpc-status":   {"0"},
		":authority":    {"example.com"},
		"x-ok":          {"1"},
		"Authorization": {"Bearer t"}, // mixed case → lower
	}
	hs := grpcframing.EncodeMetadata(md)
	byName := headersByName(hs)
	for _, bad := range []string{"content-type", "te", "grpc-timeout", "grpc-status", ":authority"} {
		if _, ok := byName[bad]; ok {
			t.Errorf("reserved key %q leaked into EncodeMetadata output", bad)
		}
	}
	if byName["x-ok"][0] != "1" {
		t.Errorf("x-ok missing: %v", byName)
	}
	if byName["authorization"][0] != "Bearer t" {
		t.Errorf("authorization = %v", byName["authorization"])
	}
}

func TestReservedKeysSkippedOnDecode(t *testing.T) {
	t.Parallel()
	hs := transport.Headers{
		{Name: "content-type", Value: "application/grpc+proto"},
		{Name: "grpc-timeout", Value: "1S"},
		{Name: "te", Value: "trailers"},
		{Name: "user-agent", Value: "argos/test"},
		{Name: ":authority", Value: "example.com"},
		{Name: "x-custom", Value: "v"},
	}
	got, err := grpcframing.DecodeMetadata(hs)
	if err != nil {
		t.Fatalf("DecodeMetadata: %v", err)
	}
	if _, ok := got["content-type"]; ok {
		t.Fatal("content-type must not enter user metadata")
	}
	if _, ok := got["grpc-timeout"]; ok {
		t.Fatal("grpc-timeout must not enter user metadata")
	}
	if got["user-agent"][0] != "argos/test" {
		t.Fatalf("user-agent whitelist = %v", got["user-agent"])
	}
	if got[":authority"][0] != "example.com" {
		t.Fatalf(":authority whitelist = %v", got[":authority"])
	}
	if got["x-custom"][0] != "v" {
		t.Fatalf("x-custom = %v", got["x-custom"])
	}
}

func TestBuildAndParseRequestPreface(t *testing.T) {
	t.Parallel()
	m := descriptor.MustMethod("echo.v1.EchoService.Echo", descriptor.Unary)
	raw := string([]byte{0x01, 0x02, 0xff})
	preface := grpcframing.BuildRequestPreface(grpcframing.PrefaceOptions{
		Method:            m,
		Outgoing:          metadata.Metadata{"x-trace-bin": {raw}, "x-id": {"42"}, "content-type": {"evil"}},
		Timeout:           1500 * time.Millisecond,
		ContentSubtype:    "proto",
		SendCompressor:    "gzip",
		AcceptCompressors: []string{"gzip", "identity"},
	})
	if preface.RequestTarget != "/echo.v1.EchoService/Echo" {
		t.Fatalf("RequestTarget = %q", preface.RequestTarget)
	}
	byName := headersByName(preface.Headers)
	if byName["content-type"][0] != "application/grpc+proto" {
		t.Fatalf("content-type = %v", byName["content-type"])
	}
	if byName["te"][0] != "trailers" {
		t.Fatalf("te = %v", byName["te"])
	}
	if byName["grpc-timeout"][0] != grpcframing.EncodeTimeout(1500*time.Millisecond) {
		t.Fatalf("grpc-timeout = %v", byName["grpc-timeout"])
	}
	if byName["grpc-encoding"][0] != "gzip" {
		t.Fatalf("grpc-encoding = %v", byName["grpc-encoding"])
	}
	if byName["grpc-accept-encoding"][0] != "gzip,identity" {
		t.Fatalf("grpc-accept-encoding = %v", byName["grpc-accept-encoding"])
	}
	if _, ok := byName["content-type"]; !ok {
		t.Fatal("missing content-type")
	}
	// User-supplied reserved content-type must not duplicate / override as metadata.
	// (protocol content-type is the only one; EncodeMetadata stripped the evil one)
	if len(byName["content-type"]) != 1 {
		t.Fatalf("content-type count = %d, want 1", len(byName["content-type"]))
	}
	if byName["x-id"][0] != "42" {
		t.Fatalf("x-id = %v", byName["x-id"])
	}
	decodedBin, err := base64.RawStdEncoding.DecodeString(byName["x-trace-bin"][0])
	if err != nil || string(decodedBin) != raw {
		t.Fatalf("x-trace-bin wire decode = %q err=%v", decodedBin, err)
	}

	info, err := grpcframing.ParseRequestHeaders(preface.RequestTarget, preface.Headers)
	if err != nil {
		t.Fatalf("ParseRequestHeaders: %v", err)
	}
	if info.Service != "echo.v1.EchoService" || info.Method != "Echo" {
		t.Fatalf("method = %s/%s", info.Service, info.Method)
	}
	if info.ContentSubtype != "proto" {
		t.Fatalf("ContentSubtype = %q", info.ContentSubtype)
	}
	if !info.HasTimeout || info.Timeout != 1500*time.Millisecond {
		// EncodeTimeout may round; compare via re-decode of wire value.
		want, _ := grpcframing.DecodeTimeout(byName["grpc-timeout"][0])
		if info.Timeout != want {
			t.Fatalf("Timeout = %v, want %v", info.Timeout, want)
		}
	}
	if info.Encoding != "gzip" {
		t.Fatalf("Encoding = %q", info.Encoding)
	}
	if info.AcceptEncoding != "gzip,identity" {
		t.Fatalf("AcceptEncoding = %q", info.AcceptEncoding)
	}
	if info.Metadata["x-id"][0] != "42" {
		t.Fatalf("metadata x-id = %v", info.Metadata["x-id"])
	}
	if info.Metadata["x-trace-bin"][0] != raw {
		t.Fatalf("metadata x-trace-bin = %q", info.Metadata["x-trace-bin"][0])
	}
	if _, ok := info.Metadata["content-type"]; ok {
		t.Fatal("content-type leaked into ParseRequestHeaders metadata")
	}
	if _, ok := info.Metadata["te"]; ok {
		t.Fatal("te leaked into metadata")
	}
}

func TestParseRequestHeadersErrors(t *testing.T) {
	t.Parallel()
	_, err := grpcframing.ParseRequestHeaders("/svc/m", nil)
	if err == nil || !strings.Contains(err.Error(), "content-type") {
		t.Fatalf("missing content-type err = %v", err)
	}
	_, err = grpcframing.ParseRequestHeaders("/svc/m", transport.Headers{
		{Name: "content-type", Value: "text/plain"},
	})
	if err == nil || !strings.Contains(err.Error(), "content-type") {
		t.Fatalf("bad content-type err = %v", err)
	}
	_, err = grpcframing.ParseRequestHeaders("/svc/m", transport.Headers{
		{Name: "content-type", Value: "application/grpc"},
		{Name: "grpc-timeout", Value: "bogus"},
	})
	if err == nil || !strings.Contains(err.Error(), "grpc-timeout") {
		t.Fatalf("bad timeout err = %v", err)
	}
	_, err = grpcframing.ParseRequestHeaders("bad", transport.Headers{
		{Name: "content-type", Value: "application/grpc"},
	})
	if err == nil {
		t.Fatal("expected invalid path error")
	}
}

func TestEncodeDecodeResponseHeadersAndTrailers(t *testing.T) {
	t.Parallel()
	md := metadata.Metadata{
		"x-server":    {"v1"},
		"x-bin-bin":   {string([]byte{0xde, 0xad})},
		"grpc-status": {"0"}, // stripped
	}
	hs := grpcframing.EncodeResponseHeaders("json", md, "")
	byName := headersByName(hs)
	if byName["content-type"][0] != "application/grpc+json" {
		t.Fatalf("content-type = %v", byName["content-type"])
	}
	if _, ok := byName["grpc-status"]; ok {
		t.Fatal("grpc-status must not appear in response headers from EncodeResponseHeaders")
	}
	gotMD, subtype, enc, err := grpcframing.DecodeResponseHeaders(hs)
	if err != nil {
		t.Fatalf("DecodeResponseHeaders: %v", err)
	}
	if subtype != "json" || enc != "" {
		t.Fatalf("subtype=%q enc=%q", subtype, enc)
	}
	if gotMD["x-server"][0] != "v1" {
		t.Fatalf("x-server = %v", gotMD["x-server"])
	}
	if gotMD["x-bin-bin"][0] != string([]byte{0xde, 0xad}) {
		t.Fatalf("x-bin-bin = %q", gotMD["x-bin-bin"][0])
	}

	tr := grpcframing.EncodeTrailers(metadata.Metadata{
		"x-trailer": {"t"},
		"te":        {"nope"},
	})
	trMD, err := grpcframing.DecodeTrailers(tr)
	if err != nil {
		t.Fatalf("DecodeTrailers: %v", err)
	}
	if trMD["x-trailer"][0] != "t" {
		t.Fatalf("trailer = %v", trMD)
	}
	if _, ok := trMD["te"]; ok {
		t.Fatal("te leaked into trailers metadata")
	}
}

func TestBuildRequestPrefaceOmitsZeroTimeout(t *testing.T) {
	t.Parallel()
	m := descriptor.MustMethod("svc.M", descriptor.Unary)
	p := grpcframing.BuildRequestPreface(grpcframing.PrefaceOptions{
		Method:         m,
		ContentSubtype: "proto",
	})
	byName := headersByName(p.Headers)
	if _, ok := byName["grpc-timeout"]; ok {
		t.Fatal("zero Timeout must omit grpc-timeout")
	}
}

func headersByName(hs transport.Headers) map[string][]string {
	out := make(map[string][]string)
	for _, h := range hs {
		out[h.Name] = append(out[h.Name], h.Value)
	}
	for k := range out {
		sort.Strings(out[k])
	}
	return out
}

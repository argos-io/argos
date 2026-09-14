package argos

import (
	"context"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/argos-io/argos/codec"
	"github.com/argos-io/argos/filter"
	"github.com/argos-io/argos/stream"
	"github.com/argos-io/argos/transport"
)

type stubTransport struct{}

func (stubTransport) ListenAndServe(
	context.Context,
	func(context.Context, string, transport.Framer) error,
	...transport.ServerOption,
) error {
	return nil
}

func (stubTransport) Open(context.Context, string, ...transport.ClientOption) (transport.Framer, error) {
	return nil, nil
}

type stubCodec struct{}

func (stubCodec) Marshal(io.Writer, any) error   { return nil }
func (stubCodec) Unmarshal(io.Reader, any) error { return nil }

type namedTransport struct{ name string }

func (t namedTransport) TransportName() string { return t.name }

func (namedTransport) ListenAndServe(
	context.Context,
	func(context.Context, string, transport.Framer) error,
	...transport.ServerOption,
) error {
	return nil
}

func (namedTransport) Open(context.Context, string, ...transport.ClientOption) (transport.Framer, error) {
	return nil, nil
}

type namedCodec struct{ name string }

func (c namedCodec) CodecName() string            { return c.name }
func (namedCodec) Marshal(io.Writer, any) error   { return nil }
func (namedCodec) Unmarshal(io.Reader, any) error { return nil }

func TestNewConfigMergesOptions(t *testing.T) {
	tr := stubTransport{}
	c := stubCodec{}
	var filterCalled bool
	f := func(context.Context, string, stream.Stream, filter.Handler) error {
		filterCalled = true
		return nil
	}

	cfg := NewConfig(
		WithTransport(tr),
		WithCodec(c),
		WithFilter(f),
		WithListenAddress(":8080"),
		WithTarget("ip://127.0.0.1:9090"),
		WithClientTransportOption(transport.WithDialAddress("override")),
	)

	if cfg.Transport != tr {
		t.Fatal("Transport not set")
	}
	if cfg.Codec != c {
		t.Fatal("Codec not set")
	}
	if len(cfg.Filters) != 1 {
		t.Fatalf("Filters len = %d, want 1", len(cfg.Filters))
	}
	_ = cfg.Filters[0](context.Background(), "m", nil, func(context.Context, string, stream.Stream) error {
		return nil
	})
	if !filterCalled {
		t.Fatal("filter was not stored")
	}
	if cfg.ClientTarget != "ip://127.0.0.1:9090" {
		t.Fatalf("ClientTarget = %q", cfg.ClientTarget)
	}
	if len(cfg.ServerTransportOpts) != 1 {
		t.Fatalf("ServerTransportOpts len = %d, want 1", len(cfg.ServerTransportOpts))
	}
	serverOpts := transport.ApplyServerOptions(cfg.ServerTransportOpts)
	if serverOpts.ListenAddress != ":8080" {
		t.Fatalf("ListenAddress = %q", serverOpts.ListenAddress)
	}
	if len(cfg.ClientTransportOpts) != 1 {
		t.Fatalf("ClientTransportOpts len = %d, want 1", len(cfg.ClientTransportOpts))
	}
	clientOpts := transport.ApplyClientOptions(cfg.ClientTransportOpts)
	if clientOpts.DialAddress != "override" {
		t.Fatalf("DialAddress = %q", clientOpts.DialAddress)
	}
}

func TestWithCodecAcceptsInterface(t *testing.T) {
	var c codec.Codec = stubCodec{}
	cfg := NewConfig(WithCodec(c))
	if cfg.Codec == nil {
		t.Fatal("Codec not set")
	}
}

func TestWithTransportInstanceTyped(t *testing.T) {
	tr := stubTransport{}
	cfg := NewConfig(WithTransportInstance(tr))
	if cfg.Transport != tr {
		t.Fatal("Transport not set")
	}
}

func TestWithTransportNamedTyped(t *testing.T) {
	transport.Register("test-tr-named", func() transport.Transport { return stubTransport{} })
	cfg := NewConfig(WithTransportNamed("test-tr-named"))
	tr, err := cfg.ResolveTransport()
	if err != nil {
		t.Fatal(err)
	}
	if tr == nil {
		t.Fatal("nil transport")
	}
}

func TestWithTransportByName(t *testing.T) {
	transport.Register("test-tr", func() transport.Transport { return stubTransport{} })
	cfg := NewConfig(WithTransport("test-tr"))
	tr, err := cfg.ResolveTransport()
	if err != nil {
		t.Fatal(err)
	}
	if tr == nil {
		t.Fatal("nil transport")
	}
}

func TestResolveCodecMissing(t *testing.T) {
	cfg := NewConfig()
	_, err := cfg.ResolveCodec()
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestResolveTransportMissing(t *testing.T) {
	cfg := NewConfig()
	_, err := cfg.ResolveTransport()
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestResolveCodecUnknownName(t *testing.T) {
	cfg := NewConfig(WithCodecNamed("missing-codec"))
	_, err := cfg.ResolveCodec()
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestWithCodecByName(t *testing.T) {
	codec.Register("test-codec", func() codec.Codec { return stubCodec{} })
	cfg := NewConfig(WithCodec("test-codec"))
	cd, err := cfg.ResolveCodec()
	if err != nil {
		t.Fatal(err)
	}
	if cd == nil {
		t.Fatal("nil codec")
	}
}

func TestResolveRejectsNilInstancesAndFactories(t *testing.T) {
	var tr *stubTransport
	cfg := NewConfig(WithTransport(tr))
	if _, err := cfg.ResolveTransport(); err == nil {
		t.Fatal("ResolveTransport accepted a typed-nil instance")
	}

	var cd *stubCodec
	cfg = NewConfig(WithCodec(cd))
	if _, err := cfg.ResolveCodec(); err == nil {
		t.Fatal("ResolveCodec accepted a typed-nil instance")
	}

	transport.Register("test-nil-transport", func() transport.Transport { return nil })
	cfg = NewConfig(WithTransportNamed("test-nil-transport"))
	if _, err := cfg.ResolveTransport(); err == nil {
		t.Fatal("ResolveTransport accepted a nil factory result")
	}

	codec.Register("test-nil-codec", func() codec.Codec { return nil })
	cfg = NewConfig(WithCodecNamed("test-nil-codec"))
	if _, err := cfg.ResolveCodec(); err == nil {
		t.Fatal("ResolveCodec accepted a nil factory result")
	}
}

func TestNewConfigRejectsNilOptionAndFilter(t *testing.T) {
	cfg := NewConfig(nil)
	if _, err := cfg.ResolveTransport(); err == nil || !strings.Contains(err.Error(), "nil option") {
		t.Fatalf("nil option error = %v", err)
	}

	cfg = NewConfig(WithFilter(nil))
	if _, err := cfg.ResolveCodec(); err == nil || !strings.Contains(err.Error(), "nil filter") {
		t.Fatalf("nil filter error = %v", err)
	}
}

func TestWithTransportInvalidTypeReturnsError(t *testing.T) {
	cfg := NewConfig(WithTransport(123))
	_, err := cfg.ResolveTransport()
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "WithTransport") {
		t.Fatalf("error = %v", err)
	}
}

func TestWithCodecInvalidTypeReturnsError(t *testing.T) {
	cfg := NewConfig(WithCodec(123))
	_, err := cfg.ResolveCodec()
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "WithCodec") {
		t.Fatalf("error = %v", err)
	}
}

func TestSplitConfigErrorsDoNotCrossContaminate(t *testing.T) {
	cfg := NewConfig(WithTransport(123), WithCodec(stubCodec{}))
	if _, err := cfg.ResolveCodec(); err != nil {
		t.Fatalf("ResolveCodec: %v", err)
	}
	if _, err := cfg.ResolveTransport(); err == nil {
		t.Fatal("expected transport error")
	}

	cfg = NewConfig(WithTransport(stubTransport{}), WithCodec(123))
	if _, err := cfg.ResolveTransport(); err != nil {
		t.Fatalf("ResolveTransport: %v", err)
	}
	if _, err := cfg.ResolveCodec(); err == nil {
		t.Fatal("expected codec error")
	}
}

func TestCodecForCallUsesCachedCodec(t *testing.T) {
	cfg := NewConfig(WithCodec(stubCodec{}))
	cd, err := cfg.CodecForCall()
	if err != nil {
		t.Fatal(err)
	}
	if cd == nil {
		t.Fatal("nil codec")
	}
}

func TestValidateCompatibility(t *testing.T) {
	tests := []struct {
		name       string
		transport  string
		codec      string
		namedCodec bool
		wantError  bool
		errorText  string
	}{
		{name: "http1 json", transport: "http1", codec: "json", namedCodec: true},
		{name: "http1 protobuf", transport: "http1", codec: "protobuf", namedCodec: true, wantError: true, errorText: "requires codec \"json\""},
		{name: "http1 unnamed", transport: "http1", wantError: true, errorText: "requires a named \"json\" codec"},
		{name: "http1 empty codec identity", transport: "http1", codec: "", namedCodec: true, wantError: true, errorText: "requires a named \"json\" codec"},
		{name: "http2 protobuf", transport: "http2", codec: "protobuf", namedCodec: true},
		{name: "http2 json", transport: "http2", codec: "json", namedCodec: true, wantError: true, errorText: "requires codec \"protobuf\""},
		{name: "telnet json", transport: "telnet", codec: "json", namedCodec: true},
		{name: "telnet protobuf", transport: "telnet", codec: "protobuf", namedCodec: true, wantError: true, errorText: "requires codec \"json\""},
		{name: "tcp unnamed", transport: "tcp"},
		{name: "tcp custom", transport: "tcp", codec: "custom", namedCodec: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var cd codec.Codec = stubCodec{}
			if tt.namedCodec {
				cd = namedCodec{name: tt.codec}
			}
			cfg := NewConfig(WithTransport(namedTransport{name: tt.transport}), WithCodec(cd))
			err := cfg.ValidateCompatibility()
			if tt.wantError {
				if err == nil || !strings.Contains(err.Error(), tt.errorText) {
					t.Fatalf("error = %v, want substring %q", err, tt.errorText)
				}
				return
			}
			if err != nil {
				t.Fatalf("ValidateCompatibility: %v", err)
			}
		})
	}
}

func TestResolveTransportMissingMessage(t *testing.T) {
	cfg := NewConfig()
	_, err := cfg.ResolveTransport()
	if err == nil || !strings.Contains(err.Error(), "binding needs WithTransport") {
		t.Fatalf("error = %v", err)
	}
}

func TestZeroConfigConcurrentResolution(t *testing.T) {
	name := "test-zero-config-transport"
	transport.Register(name, func() transport.Transport { return stubTransport{} })
	cfg := Config{transportName: name}

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := cfg.ResolveTransport(); err != nil {
				t.Errorf("ResolveTransport: %v", err)
			}
		}()
	}
	wg.Wait()
}

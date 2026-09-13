package transport_test

import (
	"testing"

	"github.com/argos-io/argos/transport"
	"github.com/argos-io/argos/transport/http2"
)

func TestBuiltinHTTP2Registered(t *testing.T) {
	_ = http2.New // ensure package linked
	if transport.Get("http2") == nil {
		t.Fatal("http2 not registered")
	}
	newTR := transport.Get("http2")
	if newTR == nil {
		t.Fatal("nil factory")
	}
	if tr := newTR(); tr == nil {
		t.Fatal("nil transport")
	}
}

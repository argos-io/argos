package framing

import (
	"errors"
	"reflect"
	"regexp"
	"testing"
)

// Compile-time: ServerCall embeds Call.
var _ Call = (ServerCall)(nil)

var compressField = regexp.MustCompile(`(?i)compress`)

func TestConfigHasNoCompressionFields(t *testing.T) {
	t.Parallel()
	typ := reflect.TypeOf(Config{})
	for i := 0; i < typ.NumField(); i++ {
		name := typ.Field(i).Name
		if compressField.MatchString(name) {
			t.Fatalf("Config field %q matches (?i)compress; §3.1-10 forbids compression in Config", name)
		}
	}
}

func TestSentinelsDistinguishable(t *testing.T) {
	t.Parallel()
	sentinels := []error{
		ErrSessionBusy,
		ErrSessionSpent,
		ErrCallRejected,
	}
	for i, a := range sentinels {
		if a == nil {
			t.Fatalf("sentinel %d is nil", i)
		}
		if !errors.Is(a, a) {
			t.Fatalf("errors.Is(%v, itself) = false", a)
		}
		for j, b := range sentinels {
			if i == j {
				continue
			}
			if errors.Is(a, b) {
				t.Fatalf("errors.Is(%v, %v) = true; sentinels must be mutually distinguishable", a, b)
			}
		}
	}
}

func TestReuseModelValues(t *testing.T) {
	t.Parallel()
	if OneCallPerConn != 0 || Sequential != 1 || Concurrent != 2 {
		t.Fatalf("ReuseModel iota: OneCallPerConn=%d Sequential=%d Concurrent=%d",
			OneCallPerConn, Sequential, Concurrent)
	}
}

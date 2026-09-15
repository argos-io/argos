package framing_test

import (
	"errors"
	"reflect"
	"regexp"
	"testing"

	"github.com/argos-io/argos/framing"
)

// Compile-time: ServerCall embeds Call.
var _ framing.Call = (framing.ServerCall)(nil)

var compressField = regexp.MustCompile(`(?i)compress`)

func TestConfigHasNoCompressionFields(t *testing.T) {
	t.Parallel()
	typ := reflect.TypeOf(framing.Config{})
	for i := 0; i < typ.NumField(); i++ {
		name := typ.Field(i).Name
		if compressField.MatchString(name) {
			t.Fatalf("Config field %q matches (?i)compress; §3.1-10 forbids compression in framing.Config", name)
		}
	}
}

func TestSentinelsDistinguishable(t *testing.T) {
	t.Parallel()
	sentinels := []error{
		framing.ErrSessionBusy,
		framing.ErrSessionSpent,
		framing.ErrCallRejected,
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
	if framing.OneCallPerConn != 0 || framing.Sequential != 1 || framing.Concurrent != 2 {
		t.Fatalf("ReuseModel iota: OneCallPerConn=%d Sequential=%d Concurrent=%d",
			framing.OneCallPerConn, framing.Sequential, framing.Concurrent)
	}
}

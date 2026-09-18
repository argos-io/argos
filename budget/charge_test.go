package budget

import (
	"testing"

	"github.com/argos-io/argos/status"
)

func TestChargeSliceExhausts(t *testing.T) {
	b := New(10)
	if _, err := ChargeSlice(b, make([]byte, 8)); err != nil {
		t.Fatal(err)
	}
	if _, err := ChargeSlice(b, make([]byte, 8)); err != status.ErrCallsExhausted {
		t.Fatalf("got %v", err)
	}
}

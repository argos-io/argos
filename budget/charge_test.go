package budget_test

import (
	"testing"

	"github.com/argos-io/argos/budget"
	"github.com/argos-io/argos/status"
)

func TestChargeSliceExhausts(t *testing.T) {
	b := budget.New(10)
	if _, err := budget.ChargeSlice(b, make([]byte, 8)); err != nil {
		t.Fatal(err)
	}
	if _, err := budget.ChargeSlice(b, make([]byte, 8)); err != status.ErrCallsExhausted {
		t.Fatalf("got %v", err)
	}
}

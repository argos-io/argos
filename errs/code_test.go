package errs

import (
	"errors"
	"testing"
)

func TestCodeOf(t *testing.T) {
	err := Error(NotFound, "no such echo")
	if CodeOf(err) != NotFound {
		t.Fatalf("got %v", CodeOf(err))
	}
	if CodeOf(errors.New("x")) != Unknown {
		t.Fatal("plain error must be Unknown")
	}
}

func TestErrorMessage(t *testing.T) {
	err := Error(Internal, "boom")
	if err.Error() != "boom" {
		t.Fatalf("Error() = %q", err.Error())
	}
}

func TestErrorIsSameCode(t *testing.T) {
	a := Error(NotFound, "a")
	b := Error(NotFound, "b")
	if !errors.Is(a, b) {
		t.Fatal("same code errors should match with errors.Is")
	}
	if errors.Is(a, Error(Unauthenticated, "c")) {
		t.Fatal("different code errors must not match")
	}
	if errors.Is(a, errors.New("plain")) {
		t.Fatal("plain error must not match coded error")
	}
}

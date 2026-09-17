package retry_test

import (
	"testing"

	"github.com/argos-io/argos/framing/grpc/retry"
	"github.com/argos-io/argos/status"
	"github.com/argos-io/argos/stream"
)

type fakeStream struct{}

func (fakeStream) Recv(any) error   { return nil }
func (fakeStream) Send(any) error   { return nil }
func (fakeStream) HalfClose() error { return nil }

func TestAttemptsRetriesUnavailable(t *testing.T) {
	var calls int
	st, err := retry.Attempts(retry.Policy{MaxAttempts: 3}, func() (stream.Stream, error) {
		calls++
		if calls < 2 {
			return nil, status.Error(status.Unavailable, "down")
		}
		return fakeStream{}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if st == nil {
		t.Fatal("nil stream")
	}
	if calls != 2 {
		t.Fatalf("calls=%d want 2", calls)
	}
}

// Package retry provides helpers for gRPC-style retries (option A: compose in
// application code; not built into the core runtime).
//
// OpenFilter chains permit each layer to call next at most once, so multi-attempt
// dial/open belongs in a wrapper around the terminal OpenFunc or around
// client.Open — use Attempts for that.
package retry

import (
	"github.com/argos-io/argos/status"
	"github.com/argos-io/argos/stream"
)

// Policy configures which failures may be retried.
type Policy struct {
	// MaxAttempts includes the first try (minimum 1).
	MaxAttempts int
	// Retriable is called with status.CodeOf(err). When nil, Unavailable and
	// DeadlineExceeded are retriable.
	Retriable func(status.Code) bool
}

// Attempts runs open up to MaxAttempts when errors are retriable.
func Attempts(p Policy, open func() (stream.Stream, error)) (stream.Stream, error) {
	if p.MaxAttempts < 1 {
		p.MaxAttempts = 1
	}
	retriable := p.Retriable
	if retriable == nil {
		retriable = func(c status.Code) bool {
			return c == status.Unavailable || c == status.DeadlineExceeded
		}
	}
	var lastErr error
	for attempt := 0; attempt < p.MaxAttempts; attempt++ {
		st, err := open()
		if err == nil {
			return st, nil
		}
		if !retriable(status.CodeOf(err)) {
			return nil, err
		}
		lastErr = err
	}
	return nil, lastErr
}

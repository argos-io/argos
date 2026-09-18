package client

import (
	"context"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/argos-io/argos"
	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/filter"
	"github.com/argos-io/argos/status"
	"github.com/argos-io/argos/stream"
)

// scriptedStream blocks in Recv until released, then returns a scripted result.
// It stands in for any decorated Stream whose operation completes successfully
// at the same moment the call context is cancelled.
type scriptedStream struct {
	stream.Stream
	entered chan struct{}
	release chan struct{}
	once    *sync.Once
	result  error
	payload []byte
}

func (s *scriptedStream) Recv(v any) error {
	s.once.Do(func() { close(s.entered) })
	<-s.release
	if s.result != nil {
		return s.result
	}
	*(v.(*[]byte)) = append([]byte(nil), s.payload...)
	return nil
}

// A cancel that lands while Recv is in flight must not rewrite a result the
// stream already produced. The previous implementation abandoned the operation
// and returned the ctx error, so a delivered message was reported as a failure.
func TestRecvKeepsStreamResultWhenCancelRaces(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		result  error
		payload []byte
		want    error // nil means success
		wantMsg string
	}{
		{name: "success", payload: []byte("delivered")},
		{name: "eof", result: io.EOF, want: io.EOF},
		{name: "remote_status", result: status.Error(status.NotFound, "gone"), want: status.Error(status.NotFound, "gone"), wantMsg: "gone"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var dials atomic.Int64
			entered := make(chan struct{})
			release := make(chan struct{})
			var once sync.Once
			cli, err := newClientLoopback(t, sequentialLoopback(t, &dials),
				argos.WithServiceName(testService),
				argos.WithMaxConcurrentCalls(4),
				argos.WithOpenFilter(func(ctx context.Context, m descriptor.Method, next filter.OpenFunc) (stream.Stream, error) {
					st, err := next(ctx, m)
					if err != nil {
						return nil, err
					}
					return &scriptedStream{
						Stream: st, entered: entered, release: release,
						once: &once, result: tc.result, payload: tc.payload,
					}, nil
				}),
				argos.WithTarget(testTarget),
			)
			if err != nil {
				t.Fatalf("New: %v", err)
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			cs, err := cli.Open(ctx, testMethod(t))
			if err != nil {
				t.Fatalf("Open: %v", err)
			}
			defer cs.Close()
			if err := cs.Send([]byte("q")); err != nil {
				t.Fatalf("Send: %v", err)
			}
			if err := cs.HalfClose(); err != nil {
				t.Fatalf("HalfClose: %v", err)
			}

			go func() {
				<-entered
				cancel()
				// Let the ctx arm become the only ready one before the stream
				// result exists, which is the window the old code raced in.
				time.Sleep(20 * time.Millisecond)
				close(release)
			}()

			var got []byte
			err = cs.Recv(&got)
			if tc.want == nil {
				if err != nil {
					t.Fatalf("Recv = %v; a delivered message must not be reported as a failure", err)
				}
				if string(got) != string(tc.payload) {
					t.Fatalf("Recv payload = %q, want %q", got, tc.payload)
				}
				return
			}
			if err == nil {
				t.Fatalf("Recv = nil, want %v", tc.want)
			}
			if !errors.Is(err, tc.want) {
				t.Fatalf("Recv = %v, want %v", err, tc.want)
			}
			if tc.wantMsg != "" && err.Error() != tc.wantMsg {
				t.Fatalf("Recv message = %q, want %q", err.Error(), tc.wantMsg)
			}
			if errors.Is(err, context.Canceled) {
				t.Fatal("Recv returned context.Canceled, masking the stream result")
			}
		})
	}
}

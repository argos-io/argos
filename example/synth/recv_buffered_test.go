package synth

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/filter"
	"github.com/argos-io/argos/stream"
)

// TestRecvKeepsBufferedResponseWhenLoopAlreadyFinished drives Recv only after
// the whole response is buffered and recvLoop has closed done — the window
// where a select over both the inbox and done drops the response.
func TestRecvKeepsBufferedResponseWhenLoopAlreadyFinished(t *testing.T) {
	handlers := map[string]filter.Handler{
		"Ping": func(ctx context.Context, m descriptor.Method, st stream.Stream) error {
			var in []byte
			if err := st.Recv(&in); err != nil {
				return err
			}
			_ = st.Recv(&in)
			return st.Send([]byte("ping-ok"))
		},
		"Echo": func(ctx context.Context, m descriptor.Method, st stream.Stream) error {
			var in []byte
			if err := st.Recv(&in); err != nil {
				return err
			}
			_ = st.Recv(&in)
			return st.Send(in)
		},
		"Exclusive": func(ctx context.Context, m descriptor.Method, st stream.Stream) error {
			return nil
		},
	}
	addr, fn := startSynthServer(t, handlers)
	cli := newSynthClient(t, addr, fn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ping := descriptor.MustMethod(MethodPing, descriptor.Unary)
	const rounds = 20
	for i := 0; i < rounds; i++ {
		cs, err := cli.Open(ctx, ping)
		if err != nil {
			t.Fatalf("round %d: Open: %v", i, err)
		}
		if err := cs.Send([]byte("x")); err != nil {
			t.Fatalf("round %d: Send: %v", i, err)
		}
		if err := cs.HalfClose(); err != nil {
			t.Fatalf("round %d: HalfClose: %v", i, err)
		}
		// Let recvLoop buffer DATA+STATUS and exit before the caller reads.
		time.Sleep(25 * time.Millisecond)
		var out []byte
		err = cs.Recv(&out)
		if errors.Is(err, io.EOF) {
			t.Fatalf("round %d: first Recv returned io.EOF; the buffered response was dropped", i)
		}
		if err != nil {
			t.Fatalf("round %d: Recv: %v", i, err)
		}
		if string(out) != "ping-ok" {
			t.Fatalf("round %d: out = %q, want %q", i, out, "ping-ok")
		}
		_ = cs.Close()
	}
}

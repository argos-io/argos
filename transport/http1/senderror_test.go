package http1

import (
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/argos-io/argos/transport"
)

// newTestClientCarrier returns a client side exchange whose request-body write
// half can be broken on demand, modelling net/http no longer reading a request
// body it has given up on (early response, peer refused the body).
func newTestClientCarrier() (*clientCarrier, *io.PipeWriter) {
	pr, pw := io.Pipe()
	return &clientCarrier{
		pw:     pw,
		pr:     pr,
		cancel: func() {},
		ready:  make(chan struct{}),
	}, pw
}

func breakRequestBodyWrite(c *clientCarrier, cause error) {
	_ = c.pr.CloseWithError(cause)
}

func responseWithBody(body string) *http.Response {
	return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(body))}
}

// The request body write failed but the response is still readable: this is a
// finished send direction, not a dead exchange. Framing keeps the carrier and
// reports stream.ErrSendClosed to the caller.
func TestWriteFailureWithResponseKeepsReceiveOpen(t *testing.T) {
	car, _ := newTestClientCarrier()
	car.finish(responseWithBody("denied"), nil)

	breakRequestBodyWrite(car, errors.New("http1: peer stopped reading the request body"))

	_, err := car.Write([]byte("chunk"))
	if err == nil {
		t.Fatal("Write succeeded after the request body reader was closed")
	}
	var se transport.SendError
	if !errors.As(err, &se) {
		t.Fatalf("Write error %v (%T) is not a transport.SendError", err, err)
	}
	if !se.ReceiveOpen() {
		t.Fatal("ReceiveOpen() = false although the response had already arrived")
	}
	if _, serr := car.ResponseStatus(); serr != nil {
		t.Fatalf("ResponseStatus after a send failure: %v", serr)
	}
}

// Nothing has been decided yet: the conservative rule applies and framing must
// keep reading rather than tear the exchange down.
func TestWriteFailureBeforeAnyResponseIsConservative(t *testing.T) {
	car, _ := newTestClientCarrier()
	breakRequestBodyWrite(car, errors.New("http1: request body dropped"))

	_, err := car.Write([]byte("chunk"))
	if err == nil {
		t.Fatal("Write succeeded after the request body reader was closed")
	}
	var se transport.SendError
	if !errors.As(err, &se) {
		t.Fatalf("Write error %v (%T) is not a transport.SendError", err, err)
	}
	if !se.ReceiveOpen() {
		t.Fatal("ReceiveOpen() = false; an undecided exchange must stay readable")
	}
}

// The round trip already failed, so no response can arrive: the exchange is
// unrecoverable and the receive direction is closed.
func TestWriteFailureAfterRoundTripErrorReportsReceiveClosed(t *testing.T) {
	car, _ := newTestClientCarrier()
	car.finish(nil, errors.New("http1: dial failed"))
	breakRequestBodyWrite(car, errors.New("http1: request body dropped"))

	_, err := car.Write([]byte("chunk"))
	if err == nil {
		t.Fatal("Write succeeded after the request body reader was closed")
	}
	var se transport.SendError
	if !errors.As(err, &se) {
		t.Fatalf("Write error %v (%T) is not a transport.SendError", err, err)
	}
	if se.ReceiveOpen() {
		t.Fatal("ReceiveOpen() = true although the round trip already failed")
	}
}

// An aborted carrier is closed in both directions.
func TestWriteFailureAfterAbortReportsReceiveClosed(t *testing.T) {
	car, _ := newTestClientCarrier()
	_ = car.Abort()

	_, err := car.Write([]byte("chunk"))
	if err == nil {
		t.Fatal("Write succeeded on an aborted carrier")
	}
	var se transport.SendError
	if !errors.As(err, &se) {
		t.Fatalf("Write error %v (%T) is not a transport.SendError", err, err)
	}
	if se.ReceiveOpen() {
		t.Fatal("ReceiveOpen() = true on an aborted carrier")
	}
}

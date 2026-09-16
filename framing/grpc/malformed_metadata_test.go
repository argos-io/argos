package grpc

import (
	"errors"
	"io"
	"testing"

	"github.com/argos-io/argos/metadata"
	"github.com/argos-io/argos/transport"
)

// mdCarrier is the minimal carrier finishClientRecv reads from: response
// status, headers and trailers.
type mdCarrier struct {
	status   int
	headers  transport.Headers
	trailers transport.Headers
}

func (c *mdCarrier) Abort() error                                { return nil }
func (c *mdCarrier) ResponseStatus() (int, error)                { return c.status, nil }
func (c *mdCarrier) ResponseHeaders() (transport.Headers, error) { return c.headers, nil }
func (c *mdCarrier) ResponseTrailers() (transport.Headers, error) {
	return c.trailers, nil
}

// A malformed -bin trailer used to leave DecodeMetadata's result nil, and the
// merge of trailers-only user keys into it panicked with "assignment to entry
// in nil map" on the client's receive path — a remote crash. The malformed
// value must now be skipped while every other header survives.
func TestFinishClientRecvToleratesMalformedBinaryTrailer(t *testing.T) {
	md := metadata.New(metadata.RoleInitiator, nil)
	c := &call{
		carrier: &mdCarrier{
			status: httpOK,
			headers: transport.Headers{
				{Name: "content-type", Value: "application/grpc"},
				{Name: "x-user", Value: "v"},
			},
			trailers: transport.Headers{
				{Name: "grpc-status", Value: "0"},
				{Name: "weird-bin", Value: "!!!!"},
			},
		},
		md:        md,
		initiator: true,
	}

	payload, release, err := c.finishClientRecv()
	if payload != nil || release != nil {
		t.Fatalf("payload = %v, release non-nil = %v; want both nil", payload, release != nil)
	}
	if !errors.Is(err, io.EOF) {
		t.Fatalf("finishClientRecv err = %v, want io.EOF", err)
	}
	if got := md.IncomingHeaders()["x-user"]; len(got) != 1 || got[0] != "v" {
		t.Fatalf("well-formed header lost: x-user = %v", got)
	}
}

// DecodeMetadata keeps well-formed entries when one binary value is corrupt.
func TestDecodeMetadataSkipsMalformedValueOnly(t *testing.T) {
	good := []byte{0x01, 0x02, 0x03}
	hs := transport.Headers{
		{Name: "good-bin", Value: encodeMetadataValue("good-bin", string(good))},
		{Name: "weird-bin", Value: "!!!!"},
		{Name: "plain", Value: "keep"},
		{Name: "grpc-status", Value: "0"}, // reserved: dropped
	}
	md, err := DecodeMetadata(hs)
	if err == nil {
		t.Fatal("malformed binary value must be reported")
	}
	if md == nil {
		t.Fatal("DecodeMetadata returned a nil map; callers merge into it")
	}
	if got := md["plain"]; len(got) != 1 || got[0] != "keep" {
		t.Fatalf("plain = %v, want [keep]", got)
	}
	if got := md["good-bin"]; len(got) != 1 || got[0] != string(good) {
		t.Fatalf("good-bin = %v, want the decoded value", got)
	}
	if _, ok := md["weird-bin"]; ok {
		t.Fatal("malformed value must not be exposed")
	}
	if _, ok := md["grpc-status"]; ok {
		t.Fatal("reserved header must stay hidden")
	}
}

package wholebody

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/framing"
	"github.com/argos-io/argos/metadata"
	"github.com/argos-io/argos/status"
	"github.com/argos-io/argos/transport"
)

// clientSession is a Concurrent wholebody client session over StreamConn.
type clientSession struct {
	conn  transport.StreamConn
	cfg   framing.Config
	codec string

	mu       sync.Mutex
	closed   bool
	reusable bool
	inFlight int
}

func (s *clientSession) Reusable() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reusable && !s.closed
}

func (s *clientSession) markBad() {
	s.mu.Lock()
	s.reusable = false
	s.mu.Unlock()
}

func (s *clientSession) endFlight() {
	s.mu.Lock()
	if s.inFlight > 0 {
		s.inFlight--
	}
	s.mu.Unlock()
}

func (s *clientSession) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.reusable = false
	s.mu.Unlock()
	return s.conn.Close()
}

func (s *clientSession) OpenCall(ctx context.Context, m descriptor.Method, spec framing.CallSpec) (framing.Call, error) {
	if m.IsZero() {
		return nil, status.Error(status.InvalidArgument, "framing/wholebody: zero Method")
	}
	if m.Shape() != descriptor.Unary {
		return nil, status.Error(status.Unimplemented, fmt.Sprintf(
			"framing/wholebody: shape %v unsupported (Framing=wholebody Shape=%v; only Unary)",
			m.Shape(), m.Shape()))
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	s.mu.Lock()
	if s.closed || !s.reusable {
		s.mu.Unlock()
		return nil, status.Error(status.Unavailable, "framing/wholebody: session not reusable")
	}
	s.inFlight++
	s.mu.Unlock()

	var outgoing metadata.Metadata
	if spec.Metadata != nil {
		outgoing = spec.Metadata.OutgoingHeaders()
		_ = metadata.FreezeOutgoingHeaders(spec.Metadata)
	}
	preface := BuildRequestPreface(m, s.codec, outgoing)

	car, err := s.conn.OpenStream(ctx, preface)
	if err != nil {
		s.endFlight()
		s.markBad()
		return nil, err
	}
	bs, ok := car.(transport.ByteStreamCarrier)
	if !ok {
		_ = car.Abort()
		s.endFlight()
		return nil, fmt.Errorf("framing/wholebody: OpenStream Carrier is not ByteStreamCarrier")
	}
	if _, ok := car.(transport.SendCloser); !ok {
		_ = car.Abort()
		s.endFlight()
		return nil, fmt.Errorf("framing/wholebody: OpenStream Carrier missing SendCloser")
	}
	if _, ok := car.(transport.ResponseHeaderReader); !ok {
		_ = car.Abort()
		s.endFlight()
		return nil, fmt.Errorf("framing/wholebody: OpenStream Carrier missing ResponseHeaderReader")
	}

	return &call{
		client:    s,
		carrier:   car,
		body:      bs,
		method:    m.FullName(),
		md:        spec.Metadata,
		cfg:       s.cfg,
		maxMsg:    s.cfg.MaxMessageSize,
		codec:     s.codec,
		initiator: true,
		localCar:  true,
	}, nil
}

// serverSession wraps one HTTP request CarrierConn. AcceptCall succeeds once.
type serverSession struct {
	conn    transport.CarrierConn
	carrier transport.Carrier
	cfg     framing.Config
	codec   string

	mu       sync.Mutex
	closed   bool
	accepted bool
}

func (s *serverSession) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()
	return s.conn.Close()
}

func (s *serverSession) AcceptCall(ctx context.Context, spec framing.CallSpec) (framing.ServerCall, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, io.EOF
	}
	if s.accepted {
		s.mu.Unlock()
		return nil, io.EOF
	}
	s.accepted = true
	s.mu.Unlock()

	rh, ok := s.carrier.(transport.RequestHeaderReader)
	if !ok {
		return nil, fmt.Errorf("framing/wholebody: server Carrier missing RequestHeaderReader")
	}
	bs, ok := s.carrier.(transport.ByteStreamCarrier)
	if !ok {
		return nil, fmt.Errorf("framing/wholebody: server Carrier is not ByteStreamCarrier")
	}
	if _, ok := s.carrier.(transport.UnaryResponseWriter); !ok {
		return nil, fmt.Errorf("framing/wholebody: server Carrier missing UnaryResponseWriter")
	}

	svc, meth, err := ParseMethodPath(rh.RequestTarget())
	if err != nil {
		return nil, fmt.Errorf("%w: %w", framing.ErrCallRejected,
			status.Error(status.InvalidArgument, err.Error()))
	}
	fullName := svc + "." + meth
	if spec.Metadata != nil {
		_ = metadata.SetIncomingHeaders(spec.Metadata, DecodeMetadata(rh.RequestHeaders()))
	}

	c := &call{
		server:    s,
		carrier:   s.carrier,
		body:      bs,
		method:    fullName,
		md:        spec.Metadata,
		cfg:       s.cfg,
		maxMsg:    s.cfg.MaxMessageSize,
		codec:     s.codec,
		initiator: false,
	}
	return &serverCall{call: c}, nil
}

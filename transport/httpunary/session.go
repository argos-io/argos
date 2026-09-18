package httpunary

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/argos-io/argos/budget"
	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/internal/session"
	"github.com/argos-io/argos/metadata"
	"github.com/argos-io/argos/status"
	"github.com/argos-io/argos/transport"
)

type clientSession struct {
	conn  transport.StreamConn
	cfg   session.Options
	codec string

	router      Router
	contentType func(codecName string) string
	encodeError func(err error) (contentType string, body []byte)

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

func (s *clientSession) OpenCall(ctx context.Context, m descriptor.Method, spec session.CallSpec) (session.Call, error) {
	if m.IsZero() {
		return nil, status.Error(status.InvalidArgument, "httpunary: zero Method")
	}
	if m.Shape() != descriptor.Unary {
		return nil, status.Error(status.Unimplemented, fmt.Sprintf(
			"httpunary: shape %v unsupported (only Unary)", m.Shape()))
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	s.mu.Lock()
	if s.closed || !s.reusable {
		s.mu.Unlock()
		return nil, status.Error(status.Unavailable, "httpunary: session not reusable")
	}
	s.inFlight++
	s.mu.Unlock()

	var outgoing metadata.Metadata
	if spec.Metadata != nil {
		outgoing = spec.Metadata.OutgoingHeaders()
		_ = metadata.FreezeOutgoingHeaders(spec.Metadata)
	}
	preface, err := s.router.BuildPreface(m, s.codec, outgoing)
	if err != nil {
		s.endFlight()
		return nil, err
	}

	car, err := s.conn.OpenStream(ctx, preface)
	if err != nil {
		s.endFlight()
		if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			s.markBad()
		}
		return nil, err
	}
	bs, ok := car.(transport.ByteStreamCarrier)
	if !ok {
		_ = car.Abort()
		s.endFlight()
		return nil, fmt.Errorf("httpunary: OpenStream Carrier is not ByteStreamCarrier")
	}
	if _, ok := car.(transport.SendCloser); !ok {
		_ = car.Abort()
		s.endFlight()
		return nil, fmt.Errorf("httpunary: OpenStream Carrier missing SendCloser")
	}
	if _, ok := car.(transport.ResponseHeaderReader); !ok {
		_ = car.Abort()
		s.endFlight()
		return nil, fmt.Errorf("httpunary: OpenStream Carrier missing ResponseHeaderReader")
	}

	b, _ := budget.FromContext(ctx)
	return &call{
		client:      s,
		carrier:     car,
		body:        bs,
		method:      m.FullName(),
		md:          spec.Metadata,
		cfg:         s.cfg,
		maxMsg:      s.cfg.MaxMessageSize,
		codec:       s.codec,
		contentType: s.contentType,
		encodeError: s.encodeError,
		initiator:   true,
		localCar:    true,
		budget:      b,
	}, nil
}

type serverSession struct {
	conn    transport.CarrierConn
	carrier transport.Carrier
	cfg     session.Options
	codec   string

	router      Router
	contentType func(codecName string) string
	encodeError func(err error) (contentType string, body []byte)

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

func (s *serverSession) AcceptCall(ctx context.Context, spec session.CallSpec) (session.ServerCall, error) {
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
		return nil, fmt.Errorf("httpunary: server Carrier missing RequestHeaderReader")
	}
	bs, ok := s.carrier.(transport.ByteStreamCarrier)
	if !ok {
		return nil, fmt.Errorf("httpunary: server Carrier is not ByteStreamCarrier")
	}
	if _, ok := s.carrier.(transport.UnaryResponseWriter); !ok {
		return nil, fmt.Errorf("httpunary: server Carrier missing UnaryResponseWriter")
	}

	inMD := DecodeMetadata(rh.RequestHeaders())

	newServerCall := func(method string) *serverCall {
		return &serverCall{call: &call{
			server:      s,
			carrier:     s.carrier,
			body:        bs,
			method:      method,
			md:          spec.Metadata,
			cfg:         s.cfg,
			maxMsg:      s.cfg.MaxMessageSize,
			codec:       s.codec,
			contentType: s.contentType,
			encodeError: s.encodeError,
			initiator:   false,
		}}
	}

	fullName, err := s.router.ResolveAccept(rh.RequestMethod(), rh.RequestTarget(), inMD)
	if err != nil {
		if fullName != "" {
			return newServerCall(fullName), err
		}
		return newServerCall(""), err
	}
	if err := checkInboundMeta(s.cfg, inMD); err != nil {
		return newServerCall(fullName), fmt.Errorf("%w: %w", session.ErrCallRejected, err)
	}
	if spec.Metadata != nil {
		_ = metadata.SetIncomingHeaders(spec.Metadata, inMD)
	}

	return newServerCall(fullName), nil
}

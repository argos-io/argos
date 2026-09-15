package grpc

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/argos-io/argos/compressor"
	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/framing"
	"github.com/argos-io/argos/metadata"
	"github.com/argos-io/argos/status"
	"github.com/argos-io/argos/transport"
)

// clientSession is a Concurrent gRPC client session over StreamConn.
type clientSession struct {
	framing     *Framing
	conn        transport.StreamConn
	cfg         framing.Config
	subtype     string
	compressors []compressor.Compressor
	sendName    string

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
		return nil, status.Error(status.InvalidArgument, "framing/grpc: zero Method")
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	s.mu.Lock()
	if s.closed || !s.reusable {
		s.mu.Unlock()
		return nil, status.Error(status.Unavailable, "framing/grpc: session not reusable")
	}
	s.inFlight++
	s.mu.Unlock()

	var timeout time.Duration
	if dl, ok := ctx.Deadline(); ok {
		timeout = time.Until(dl)
		if timeout < 0 {
			timeout = 0
		}
	}
	var outgoing metadata.Metadata
	if spec.Metadata != nil {
		outgoing = spec.Metadata.OutgoingHeaders()
		_ = metadata.FreezeOutgoingHeaders(spec.Metadata)
	}
	sendComp := resolveSendCompressor(s.compressors, s.sendName, nil, false)
	sendEnc := ""
	if sendComp.Name() != compressor.Identity.Name() {
		sendEnc = sendComp.Name()
	}
	preface := BuildRequestPreface(PrefaceOptions{
		Method:            m,
		Outgoing:          outgoing,
		Timeout:           timeout,
		ContentSubtype:    s.subtype,
		SendCompressor:    sendEnc,
		AcceptCompressors: acceptNames(s.compressors),
	})

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
		return nil, fmt.Errorf("framing/grpc: OpenStream Carrier is not ByteStreamCarrier")
	}

	c := &call{
		client:      s,
		carrier:     car,
		body:        bs,
		method:      m.FullName(),
		md:          spec.Metadata,
		shape:       m.Shape(),
		subtype:     s.subtype,
		cfg:         s.cfg,
		maxMsg:      s.cfg.MaxMessageSize,
		localCar:    true,
		initiator:   true,
		compressors: s.compressors,
		sendComp:    sendComp,
	}
	if timeout > 0 {
		c.deadline = time.Now().Add(timeout)
		c.hasDeadline = true
	}
	return c, nil
}

// serverSession wraps one HTTP request CarrierConn. AcceptCall succeeds once.
type serverSession struct {
	framing     *Framing
	conn        transport.CarrierConn
	carrier     transport.Carrier
	cfg         framing.Config
	subtype     string // default from SessionSpec; overridden by request content-type
	compressors []compressor.Compressor
	sendName    string

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
		return nil, fmt.Errorf("framing/grpc: server Carrier missing RequestHeaderReader")
	}
	bs, ok := s.carrier.(transport.ByteStreamCarrier)
	if !ok {
		return nil, fmt.Errorf("framing/grpc: server Carrier is not ByteStreamCarrier")
	}

	info, err := ParseRequestHeaders(rh.RequestTarget(), rh.RequestHeaders())
	if err != nil {
		return nil, fmt.Errorf("%w: %w", framing.ErrCallRejected,
			status.Error(status.InvalidArgument, err.Error()))
	}

	fullName := info.Service + "." + info.Method
	if spec.Metadata != nil {
		_ = metadata.SetIncomingHeaders(spec.Metadata, info.Metadata)
	}

	subtype := info.ContentSubtype
	if subtype == "" {
		subtype = s.subtype
	}

	peerAccept := parseAcceptEncoding(info.AcceptEncoding)
	sendComp := resolveSendCompressor(s.compressors, s.sendName, peerAccept, true)
	peerEnc := info.Encoding
	if peerEnc == compressor.Identity.Name() {
		peerEnc = ""
	}

	c := &call{
		server:       s,
		carrier:      s.carrier,
		body:         bs,
		method:       fullName,
		md:           spec.Metadata,
		subtype:      subtype,
		cfg:          s.cfg,
		maxMsg:       s.cfg.MaxMessageSize,
		initiator:    false,
		compressors:  s.compressors,
		sendComp:     sendComp,
		peerEncoding: peerEnc,
	}
	if info.HasTimeout {
		c.deadline = time.Now().Add(info.Timeout)
		c.hasDeadline = true
	}
	return &serverCall{call: c}, nil
}

// call is one gRPC Call (client or server).
type call struct {
	client *clientSession
	server *serverSession

	carrier transport.Carrier
	body    transport.ByteStreamCarrier
	method  string
	md      metadata.CallMetadata
	shape   descriptor.Shape
	subtype string
	cfg     framing.Config
	maxMsg  int64

	compressors  []compressor.Compressor
	sendComp     compressor.Compressor
	initiator    bool
	localCar     bool
	peerEncoding string // inbound grpc-encoding (empty means identity)
	deadline     time.Time
	hasDeadline  bool

	mu sync.Mutex

	headersSent    bool
	headersSeen    bool
	halfClosed     bool
	finished       bool
	closed         bool
	sawTerminal    bool // initiator read status / EOF after OK
	statusReturned bool
	statusErr      error
	messagesSent   bool // any LPM written (affects trailers-only)
	messagesRecv   bool // any LPM successfully received

	sending atomic.Bool
	recving atomic.Bool
}

func (c *call) Method() string { return c.method }

func (c *call) Deadline() (time.Time, bool) {
	if !c.hasDeadline {
		return time.Time{}, false
	}
	return c.deadline, true
}

func (c *call) markBad() {
	if c.client != nil {
		c.client.markBad()
	}
}

func (c *call) SendHeaders() error {
	if c.initiator {
		return status.Error(status.Unimplemented, "framing/grpc: SendHeaders unsupported for initiator")
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return errCallClosed
	}
	if c.headersSent {
		c.mu.Unlock()
		return metadata.ErrHeadersAlreadySent
	}
	if c.finished {
		c.mu.Unlock()
		return errSendFinished
	}
	c.mu.Unlock()

	rw, ok := c.carrier.(transport.ResponseWriter)
	if !ok {
		return status.Error(status.Unimplemented, "framing/grpc: carrier has no ResponseWriter")
	}
	var md metadata.Metadata
	if c.md != nil {
		md = c.md.OutgoingHeaders()
	}
	if err := rw.WriteHeaders(httpOK, EncodeResponseHeaders(c.subtype, md, c.sendEncodingHeader())); err != nil {
		c.markBad()
		return err
	}
	c.mu.Lock()
	c.headersSent = true
	c.mu.Unlock()
	if c.md != nil {
		_ = metadata.FreezeOutgoingHeaders(c.md)
	}
	return nil
}

const httpOK = 200

func (c *call) maybeAutoHeaders() error {
	if c.initiator {
		return nil
	}
	c.mu.Lock()
	if c.headersSent {
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()
	return c.SendHeaders()
}

func (c *call) Send(payload []byte) error {
	if !c.sending.CompareAndSwap(false, true) {
		return errConcurrentSend
	}
	defer c.sending.Store(false)

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return errCallClosed
	}
	if c.initiator && c.halfClosed {
		c.mu.Unlock()
		return errSendFinished
	}
	if !c.initiator && c.finished {
		c.mu.Unlock()
		return errSendFinished
	}
	c.mu.Unlock()

	if !c.initiator {
		if err := c.maybeAutoHeaders(); err != nil {
			return err
		}
	}

	data := payload
	if data == nil {
		data = []byte{}
	}
	if c.maxMsg > 0 && int64(len(data)) > c.maxMsg {
		return fmt.Errorf("framing/grpc: Send payload %d > max %d", len(data), c.maxMsg)
	}
	// Copy so caller may reuse the buffer after return.
	cp := append([]byte(nil), data...)
	compressed, wire, err := compressMessage(c.sendComp, cp)
	if err != nil {
		c.markBad()
		return err
	}
	if c.cfg.MaxFrameSize > 0 && int64(len(wire)) > c.cfg.MaxFrameSize {
		return fmt.Errorf("framing/grpc: Send wire payload %d > max frame %d", len(wire), c.cfg.MaxFrameSize)
	}
	if err := WriteLPM(c.body, compressed, wire); err != nil {
		c.markBad()
		return err
	}
	c.mu.Lock()
	c.messagesSent = true
	c.mu.Unlock()
	return nil
}

func (c *call) HalfClose() error {
	if !c.initiator {
		return status.Error(status.Unimplemented, "framing/grpc: HalfClose unsupported for responder")
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return errCallClosed
	}
	if c.halfClosed {
		c.mu.Unlock()
		return nil
	}
	c.halfClosed = true
	c.mu.Unlock()

	sc, ok := c.carrier.(transport.SendCloser)
	if !ok {
		return status.Error(status.Unimplemented, "framing/grpc: carrier has no CloseSend")
	}
	if err := sc.CloseSend(); err != nil {
		c.markBad()
		return err
	}
	return nil
}

func (c *call) Finish(err error) error {
	if c.initiator {
		return status.Error(status.Unimplemented, "framing/grpc: Finish unsupported for initiator")
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return errCallClosed
	}
	if c.finished {
		c.mu.Unlock()
		return errSendFinished
	}
	headersSent := c.headersSent
	c.mu.Unlock()

	rw, ok := c.carrier.(transport.ResponseWriter)
	if !ok {
		return status.Error(status.Unimplemented, "framing/grpc: carrier has no ResponseWriter")
	}

	var userTrailers metadata.Metadata
	if c.md != nil {
		userTrailers = c.md.OutgoingTrailers()
		_ = metadata.FreezeOutgoingTrailers(c.md)
		_ = metadata.FreezeOutgoingHeaders(c.md)
	}
	trailers := EncodeStatusTrailers(err, userTrailers)

	var initial transport.Headers
	if !headersSent {
		var hdrMD metadata.Metadata
		if c.md != nil {
			hdrMD = c.md.OutgoingHeaders()
		}
		initial = EncodeResponseHeaders(c.subtype, hdrMD, c.sendEncodingHeader())
	}

	if werr := rw.Finish(httpOK, initial, trailers); werr != nil {
		c.markBad()
		return werr
	}
	c.mu.Lock()
	c.finished = true
	c.headersSent = true
	c.sawTerminal = true
	c.mu.Unlock()
	return nil
}

func (c *call) Recv() (payload []byte, release func(), err error) {
	if !c.recving.CompareAndSwap(false, true) {
		return nil, nil, errConcurrentRecv
	}
	defer c.recving.Store(false)

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, nil, errCallClosed
	}
	if c.statusReturned {
		c.mu.Unlock()
		return nil, nil, io.EOF
	}
	c.mu.Unlock()

	if c.initiator {
		if err := c.ensureResponseHeaders(); err != nil {
			c.markBad()
			return nil, nil, err
		}
	}

	compressed, data, err := ReadLPM(c.body)
	if err != nil {
		if err == io.EOF {
			if c.initiator {
				return c.finishClientRecv()
			}
			c.mu.Lock()
			c.sawTerminal = true
			c.mu.Unlock()
			return nil, nil, io.EOF
		}
		// Non-gRPC HTTP error bodies are not LPM. Before any message, drain and
		// resolve via trailers / HTTP fallback instead of surfacing a parse error.
		if c.initiator {
			c.mu.Lock()
			gotMsg := c.messagesRecv
			c.mu.Unlock()
			if !gotMsg {
				_, _ = io.Copy(io.Discard, c.body)
				return c.finishClientRecv()
			}
		}
		c.markBad()
		return nil, nil, err
	}
	if enc := c.peerEncoding; enc != "" && enc != compressor.Identity.Name() {
		if _, ok := compressor.Find(enc, c.compressors); !ok {
			c.markBad()
			return nil, nil, unsupportedEncoding(enc)
		}
	}
	if compressed {
		enc := c.peerEncoding
		if enc == "" || enc == compressor.Identity.Name() {
			c.markBad()
			return nil, nil, unsupportedEncoding(enc)
		}
		comp, ok := compressor.Find(enc, c.compressors)
		if !ok {
			c.markBad()
			return nil, nil, unsupportedEncoding(enc)
		}
		decoded, derr := decompressMessage(comp, data, c.maxMsg)
		if derr != nil {
			c.markBad()
			return nil, nil, derr
		}
		data = decoded
	} else if c.maxMsg > 0 && int64(len(data)) > c.maxMsg {
		c.markBad()
		return nil, nil, fmt.Errorf("framing/grpc: Recv payload %d > max %d", len(data), c.maxMsg)
	}
	c.mu.Lock()
	c.messagesRecv = true
	c.mu.Unlock()
	return data, func() {}, nil
}

func (c *call) ensureResponseHeaders() error {
	c.mu.Lock()
	if c.headersSeen {
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()

	rh, ok := c.carrier.(transport.ResponseHeaderReader)
	if !ok {
		return nil
	}
	hs, err := rh.ResponseHeaders()
	if err != nil {
		return err
	}
	// Invalid content-type is tolerated here: HTTP fallback (missing
	// grpc-status) may return a non-gRPC response. User metadata is still
	// applied when DecodeResponseHeaders succeeds.
	md, _, encoding, err := DecodeResponseHeaders(hs)
	if err == nil {
		if encoding != "" && encoding != compressor.Identity.Name() {
			c.mu.Lock()
			c.peerEncoding = encoding
			c.mu.Unlock()
		}
		if c.md != nil {
			_ = metadata.SetIncomingHeaders(c.md, md)
		}
	}
	c.mu.Lock()
	c.headersSeen = true
	c.mu.Unlock()
	return nil
}

func (c *call) finishClientRecv() ([]byte, func(), error) {
	_ = c.ensureResponseHeaders()

	httpStatus := httpOK
	if rh, ok := c.carrier.(transport.ResponseHeaderReader); ok {
		if st, err := rh.ResponseStatus(); err == nil {
			httpStatus = st
		}
	}

	var headers, trailers transport.Headers
	if rh, ok := c.carrier.(transport.ResponseHeaderReader); ok {
		if hs, err := rh.ResponseHeaders(); err == nil {
			headers = hs
		}
	}
	if rt, ok := c.carrier.(transport.ResponseTrailerReader); ok {
		if tr, err := rt.ResponseTrailers(); err == nil {
			trailers = tr
		}
	}

	userMD, _ := DecodeMetadata(trailers)
	// Also merge trailers-only user keys that landed in headers (excluding reserved).
	if hdrMD, err := DecodeMetadata(headers); err == nil {
		for k, vs := range hdrMD {
			if _, exists := userMD[k]; !exists {
				userMD[k] = vs
			}
		}
	}
	if c.md != nil {
		_ = metadata.SetIncomingTrailers(c.md, userMD)
	}

	stErr := resolveCallStatus(httpStatus, headers, trailers)
	c.mu.Lock()
	c.sawTerminal = true
	if stErr != nil {
		c.statusErr = stErr
		c.statusReturned = true
		c.mu.Unlock()
		return nil, nil, stErr
	}
	c.mu.Unlock()
	return nil, nil, io.EOF
}

func (c *call) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	terminal := c.sawTerminal || c.finished
	initiator := c.initiator
	c.mu.Unlock()

	c.wakeRead()

	if initiator && !terminal {
		c.markBad()
	}
	if c.localCar {
		_ = c.carrier.Abort()
	}
	if c.client != nil {
		c.client.endFlight()
	}
	return nil
}

func (c *call) wakeRead() {
	type deadliner interface {
		SetReadDeadline(time.Time) error
	}
	if d, ok := c.carrier.(deadliner); ok {
		_ = d.SetReadDeadline(time.Now())
		return
	}
	type both interface {
		SetDeadline(time.Time) error
	}
	if d, ok := c.carrier.(both); ok {
		_ = d.SetDeadline(time.Now())
	}
}


func (c *call) sendEncodingHeader() string {
	if c.sendComp == nil || c.sendComp.Name() == compressor.Identity.Name() {
		return ""
	}
	return c.sendComp.Name()
}

type serverCall struct {
	*call
}

func (c *serverCall) Accept(m descriptor.Method) error {
	if m.IsZero() {
		return status.Error(status.InvalidArgument, "framing/grpc: zero Method")
	}
	// gRPC over HTTP/2 byte-stream carriers support all four shapes.
	_ = m.Shape()
	return nil
}

var (
	errConcurrentSend = fmt.Errorf("framing/grpc: concurrent Send")
	errConcurrentRecv = fmt.Errorf("framing/grpc: concurrent Recv")
	errSendFinished   = fmt.Errorf("framing/grpc: send already finished")
	errCallClosed     = fmt.Errorf("framing/grpc: call closed")
)

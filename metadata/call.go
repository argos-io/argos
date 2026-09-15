package metadata

import (
	"sync"

	"github.com/argos-io/argos/status"
)

// callMD is the concrete CallMetadata created by New.
// Framing helpers (SetIncoming*, Freeze*) type-assert to *callMD.
type callMD struct {
	mu sync.Mutex

	role        Role
	sendHeaders func(Metadata) error

	incomingHeaders  Metadata
	incomingTrailers Metadata
	outgoingHeaders  Metadata
	outgoingTrailers Metadata

	headersFrozen  bool
	headersSent    bool // successful SendHeaders completed
	trailersFrozen bool
}

// New creates a CallMetadata for the given role.
//
// sendHeaders is the wire submit of the current OutgoingHeaders snapshot.
// A nil sendHeaders means the carrier does not support explicit header submit;
// SendHeaders then returns status.Unimplemented and does not freeze.
// If sendHeaders returns an error whose CodeOf is Unimplemented, outgoing
// headers are likewise left unfrozen.
func New(role Role, sendHeaders func(Metadata) error) CallMetadata {
	return &callMD{
		role:             role,
		sendHeaders:      sendHeaders,
		incomingHeaders:  make(Metadata),
		incomingTrailers: make(Metadata),
		outgoingHeaders:  make(Metadata),
		outgoingTrailers: make(Metadata),
	}
}

func (c *callMD) IncomingHeaders() Metadata {
	c.mu.Lock()
	defer c.mu.Unlock()
	return Clone(c.incomingHeaders)
}

func (c *callMD) IncomingTrailers() Metadata {
	c.mu.Lock()
	defer c.mu.Unlock()
	return Clone(c.incomingTrailers)
}

func (c *callMD) OutgoingHeaders() Metadata {
	c.mu.Lock()
	defer c.mu.Unlock()
	return Clone(c.outgoingHeaders)
}

func (c *callMD) OutgoingTrailers() Metadata {
	c.mu.Lock()
	defer c.mu.Unlock()
	return Clone(c.outgoingTrailers)
}

func (c *callMD) AddOutgoingHeader(key string, values ...string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.headersFrozen {
		return ErrOutgoingHeadersFrozen
	}
	add(c.outgoingHeaders, key, values...)
	return nil
}

func (c *callMD) AddOutgoingTrailer(key string, values ...string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.role == RoleInitiator {
		return errTrailersInitiator
	}
	if c.trailersFrozen {
		return ErrOutgoingTrailersFrozen
	}
	add(c.outgoingTrailers, key, values...)
	return nil
}

func (c *callMD) SendHeaders() error {
	c.mu.Lock()
	if c.role != RoleResponder {
		c.mu.Unlock()
		return errSendHeadersUnsupported
	}
	if c.headersSent {
		c.mu.Unlock()
		return ErrHeadersAlreadySent
	}
	if c.headersFrozen {
		// Frozen by FreezeOutgoingHeaders / Send path without an explicit
		// prior SendHeaders — treat as already sent for the submit API.
		c.mu.Unlock()
		return ErrHeadersAlreadySent
	}
	if c.sendHeaders == nil {
		c.mu.Unlock()
		return errSendHeadersUnsupported
	}
	snapshot := Clone(c.outgoingHeaders)
	send := c.sendHeaders
	c.mu.Unlock()

	// Wire I/O outside the lock so concurrent getters/adds can proceed until
	// we re-lock to commit freeze (or leave unfrozen on failure).
	err := send(snapshot)
	if err != nil {
		if status.CodeOf(err) == status.Unimplemented {
			return err
		}
		return err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.headersSent {
		return ErrHeadersAlreadySent
	}
	c.headersFrozen = true
	c.headersSent = true
	return nil
}

func add(md Metadata, key string, values ...string) {
	if len(values) == 0 {
		return
	}
	md[key] = append(md[key], values...)
}

// asCallMD type-asserts md to the concrete type from New.
func asCallMD(md CallMetadata) (*callMD, error) {
	c, ok := md.(*callMD)
	if !ok || c == nil {
		return nil, errNotConcrete
	}
	return c, nil
}

// SetIncomingHeaders replaces the incoming headers snapshot (Framing-facing).
// md must have been created by New. The input is deep-copied.
func SetIncomingHeaders(md CallMetadata, h Metadata) error {
	c, err := asCallMD(md)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if h == nil {
		c.incomingHeaders = make(Metadata)
	} else {
		c.incomingHeaders = Clone(h)
	}
	return nil
}

// SetIncomingTrailers replaces the incoming trailers snapshot (Framing-facing).
// md must have been created by New. The input is deep-copied.
func SetIncomingTrailers(md CallMetadata, t Metadata) error {
	c, err := asCallMD(md)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if t == nil {
		c.incomingTrailers = make(Metadata)
	} else {
		c.incomingTrailers = Clone(t)
	}
	return nil
}

// FreezeOutgoingHeaders freezes outgoing headers so further AddOutgoingHeader
// fails. Used on the first successful Send/Finish path (and after a successful
// SendHeaders). Idempotent. md must have been created by New.
func FreezeOutgoingHeaders(md CallMetadata) error {
	c, err := asCallMD(md)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.headersFrozen = true
	return nil
}

// FreezeOutgoingTrailers freezes outgoing trailers so further AddOutgoingTrailer
// fails. Used on Finish. Idempotent. md must have been created by New.
func FreezeOutgoingTrailers(md CallMetadata) error {
	c, err := asCallMD(md)
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.trailersFrozen = true
	return nil
}

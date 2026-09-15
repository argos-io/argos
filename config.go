package argos

import (
	"fmt"
	"math"
	"time"
)

const (
	kiB = 1024
	miB = 1024 * kiB
	giB = 1024 * miB
)

// Config is an immutable snapshot after New returns.
//
// Prefer reading exported fields. Do not mutate them after New: Option values
// are applied only during New, and later Option instances cannot reach this
// value. Treat the returned *Config as read-only.
//
// Provisional (§6.1 ⚠️) fields: MaxIdleSessions, SessionIdleTimeout,
// MaxSessionLifetime, MaxInboundConns, MaxInboundConnIdle, MaxInboundConnAge.
type Config struct {
	// Call-dimension limits.
	MaxFrameSize        int64
	MaxMessageSize      int64
	MaxMetadataSize     int64
	MaxHeaderBytes      int64
	ReadAheadMessages   int
	MaxConcurrentCalls  int
	MaxBufferedBytes    int64
	OpenTimeout         time.Duration

	// Connection-dimension limits.
	HandshakeTimeout       time.Duration
	MaxDrainBytes          int64
	ConnReadBufferSize     int64
	MaxSessionsPerEndpoint int
	MaxIdleSessions        int           // provisional ⚠️
	SessionIdleTimeout     time.Duration // provisional ⚠️; 0 disables
	MaxSessionLifetime      time.Duration // provisional ⚠️; 0 disables
	MaxInboundConns        int           // provisional ⚠️
	MaxInboundConnIdle     time.Duration // provisional ⚠️; must be > 0
	MaxInboundConnAge      time.Duration // provisional ⚠️; must be > 0

	// Binding is the optional factory stored by WithBinding.
	// Nil is allowed until a Client/Server path requires it.
	Binding BindingFunc
}

// New builds a Config from defaults, applies opts once, validates, and
// returns a pointer that Option cannot mutate afterward.
func New(opts ...Option) (*Config, error) {
	c := defaults()
	for _, opt := range opts {
		if opt == nil {
			return nil, fmt.Errorf("argos: nil Option")
		}
		opt.apply(&c)
	}
	if err := c.validate(); err != nil {
		return nil, err
	}
	out := c
	return &out, nil
}

func defaults() Config {
	return Config{
		MaxFrameSize:           4 * miB,
		MaxMessageSize:         4 * miB,
		MaxMetadataSize:        256 * kiB,
		MaxHeaderBytes:         1 * miB,
		ReadAheadMessages:      1,
		MaxConcurrentCalls:     64,
		MaxBufferedBytes:       1 * giB,
		OpenTimeout:            10 * time.Second,
		HandshakeTimeout:       10 * time.Second,
		MaxDrainBytes:          1 * miB,
		ConnReadBufferSize:     64 * kiB,
		MaxSessionsPerEndpoint: 64,
		// Provisional ⚠️ defaults (§6.1); task 7.5 will confirm.
		MaxIdleSessions:    8,
		SessionIdleTimeout: 50 * time.Second,
		MaxSessionLifetime:  30 * time.Minute,
		MaxInboundConns:    1024,
		MaxInboundConnIdle: 50 * time.Second,
		MaxInboundConnAge:  30 * time.Minute,
	}
}

// PerCall returns the admission reservation size (§2.6):
//
//	MaxFrameSize + (ReadAheadMessages+1)×MaxMessageSize + MaxMessageSize
func (c *Config) PerCall() (int64, error) {
	if c == nil {
		return 0, fmt.Errorf("argos: nil Config")
	}
	return perCall(c.MaxFrameSize, c.MaxMessageSize, c.ReadAheadMessages)
}

func perCall(maxFrame, maxMsg int64, readAhead int) (int64, error) {
	if readAhead < 1 {
		return 0, fmt.Errorf("argos: ReadAheadMessages must be >= 1")
	}
	// (ReadAheadMessages+1)*MaxMessageSize
	aheadPlusOne := int64(readAhead) + 1
	part, ok := mulInt64(aheadPlusOne, maxMsg)
	if !ok {
		return 0, fmt.Errorf("argos: perCall overflow (ReadAheadMessages, MaxMessageSize)")
	}
	sum, ok := addInt64(maxFrame, part)
	if !ok {
		return 0, fmt.Errorf("argos: perCall overflow (MaxFrameSize, MaxMessageSize, ReadAheadMessages)")
	}
	sum, ok = addInt64(sum, maxMsg)
	if !ok {
		return 0, fmt.Errorf("argos: perCall overflow (MaxFrameSize, MaxMessageSize, ReadAheadMessages)")
	}
	return sum, nil
}

func (c *Config) validate() error {
	if c.MaxFrameSize <= 0 {
		return fmt.Errorf("argos: MaxFrameSize must be > 0")
	}
	if c.MaxMessageSize <= 0 {
		return fmt.Errorf("argos: MaxMessageSize must be > 0")
	}
	if c.MaxMetadataSize <= 0 {
		return fmt.Errorf("argos: MaxMetadataSize must be > 0")
	}
	if c.MaxHeaderBytes <= 0 {
		return fmt.Errorf("argos: MaxHeaderBytes must be > 0")
	}
	if c.ReadAheadMessages < 1 {
		return fmt.Errorf("argos: ReadAheadMessages must be >= 1")
	}
	if c.MaxConcurrentCalls < 1 {
		return fmt.Errorf("argos: MaxConcurrentCalls must be >= 1")
	}
	if c.MaxBufferedBytes <= 0 {
		return fmt.Errorf("argos: MaxBufferedBytes must be > 0")
	}
	if c.OpenTimeout <= 0 {
		return fmt.Errorf("argos: OpenTimeout must be > 0")
	}
	if c.HandshakeTimeout <= 0 {
		return fmt.Errorf("argos: HandshakeTimeout must be > 0")
	}
	if c.MaxDrainBytes <= 0 {
		return fmt.Errorf("argos: MaxDrainBytes must be > 0")
	}
	if c.ConnReadBufferSize <= 0 {
		return fmt.Errorf("argos: ConnReadBufferSize must be > 0")
	}
	if c.MaxSessionsPerEndpoint < 1 {
		return fmt.Errorf("argos: MaxSessionsPerEndpoint must be > 0")
	}
	if c.MaxIdleSessions < 0 {
		return fmt.Errorf("argos: MaxIdleSessions must be >= 0")
	}
	if c.SessionIdleTimeout < 0 {
		return fmt.Errorf("argos: SessionIdleTimeout must be >= 0")
	}
	if c.MaxSessionLifetime < 0 {
		return fmt.Errorf("argos: MaxSessionLifetime must be >= 0")
	}
	if c.MaxInboundConns < 1 {
		return fmt.Errorf("argos: MaxInboundConns must be > 0")
	}
	if c.MaxInboundConnIdle <= 0 {
		return fmt.Errorf("argos: MaxInboundConnIdle must be > 0")
	}
	if c.MaxInboundConnAge <= 0 {
		return fmt.Errorf("argos: MaxInboundConnAge must be > 0")
	}

	pc, err := perCall(c.MaxFrameSize, c.MaxMessageSize, c.ReadAheadMessages)
	if err != nil {
		return err
	}
	product, ok := mulInt64(int64(c.MaxConcurrentCalls), pc)
	if !ok || product > c.MaxBufferedBytes {
		return fmt.Errorf("argos: MaxConcurrentCalls × perCall > MaxBufferedBytes (MaxConcurrentCalls, MaxFrameSize, MaxMessageSize, ReadAheadMessages, MaxBufferedBytes)")
	}
	return nil
}

func addInt64(a, b int64) (int64, bool) {
	if b > 0 && a > math.MaxInt64-b {
		return 0, false
	}
	if b < 0 && a < math.MinInt64-b {
		return 0, false
	}
	return a + b, true
}

func mulInt64(a, b int64) (int64, bool) {
	if a == 0 || b == 0 {
		return 0, true
	}
	if a > 0 {
		if b > 0 {
			if a > math.MaxInt64/b {
				return 0, false
			}
		} else if b < math.MinInt64/a {
			return 0, false
		}
	} else {
		if b > 0 {
			if a < math.MinInt64/b {
				return 0, false
			}
		} else if a != 0 && b < math.MaxInt64/a {
			return 0, false
		}
	}
	return a * b, true
}

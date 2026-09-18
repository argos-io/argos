package grpc

import (
	"fmt"
	"math"
	"strconv"
	"time"
)

// timeoutUnit is the trailing unit letter of a grpc-timeout value.
type timeoutUnit byte

const (
	hour        timeoutUnit = 'H'
	minute      timeoutUnit = 'M'
	second      timeoutUnit = 'S'
	millisecond timeoutUnit = 'm'
	microsecond timeoutUnit = 'u'
	nanosecond  timeoutUnit = 'n'
)

// maxTimeoutValue is the largest integer allowed before the unit letter
// (8 decimal digits). Matches grpc-go.
const maxTimeoutValue int64 = 100000000 - 1

func timeoutUnitToDuration(u timeoutUnit) (d time.Duration, ok bool) {
	switch u {
	case hour:
		return time.Hour, true
	case minute:
		return time.Minute, true
	case second:
		return time.Second, true
	case millisecond:
		return time.Millisecond, true
	case microsecond:
		return time.Microsecond, true
	case nanosecond:
		return time.Nanosecond, true
	default:
		return 0, false
	}
}

// div does integer division rounding up (grpc-go compatible).
func div(d, r time.Duration) int64 {
	if d%r > 0 {
		return int64(d/r + 1)
	}
	return int64(d / r)
}

// EncodeTimeout encodes d as a grpc-timeout header value: at most 8 digits
// plus a unit letter (n/u/m/S/M/H). Compatible with grpc-go EncodeDuration.
func EncodeTimeout(d time.Duration) string {
	if d <= 0 {
		return "0n"
	}
	if n := div(d, time.Nanosecond); n <= maxTimeoutValue {
		return strconv.FormatInt(n, 10) + "n"
	}
	if n := div(d, time.Microsecond); n <= maxTimeoutValue {
		return strconv.FormatInt(n, 10) + "u"
	}
	if n := div(d, time.Millisecond); n <= maxTimeoutValue {
		return strconv.FormatInt(n, 10) + "m"
	}
	if n := div(d, time.Second); n <= maxTimeoutValue {
		return strconv.FormatInt(n, 10) + "S"
	}
	if n := div(d, time.Minute); n <= maxTimeoutValue {
		return strconv.FormatInt(n, 10) + "M"
	}
	return strconv.FormatInt(div(d, time.Hour), 10) + "H"
}

// DecodeTimeout parses a grpc-timeout header value.
func DecodeTimeout(s string) (time.Duration, error) {
	size := len(s)
	if size < 2 {
		return 0, fmt.Errorf("grpc: timeout string too short: %q", s)
	}
	if size > 9 {
		return 0, fmt.Errorf("grpc: timeout string too long: %q", s)
	}
	unit := timeoutUnit(s[size-1])
	d, ok := timeoutUnitToDuration(unit)
	if !ok {
		return 0, fmt.Errorf("grpc: timeout unit not recognized: %q", s)
	}
	t, err := strconv.ParseUint(s[:size-1], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("grpc: malformed timeout %q: %w", s, err)
	}
	const maxHours = math.MaxInt64 / uint64(time.Hour)
	if d == time.Hour && t > maxHours {
		return time.Duration(math.MaxInt64), nil
	}
	return d * time.Duration(t), nil
}

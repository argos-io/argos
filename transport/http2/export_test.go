package http2

import "github.com/argos-io/argos/transport"

// TrackedCarrierCount reports how many OpenStream carriers are still tracked
// on a client StreamConn. Used by tests to detect carrier-map leaks.
func TrackedCarrierCount(c transport.Conn) int {
	sc, ok := c.(*streamConn)
	if !ok {
		return -1
	}
	return sc.trackedCarrierCount()
}

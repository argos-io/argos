package http1

import "github.com/argos-io/argos/transport"

// trackedCarrierCount reports how many OpenStream carriers are still tracked
// on a client StreamConn. Used by tests to detect carrier-map leaks.
func trackedCarrierCount(c transport.Conn) int {
	sc, ok := c.(*streamConn)
	if !ok {
		return -1
	}
	return sc.trackedCarrierCount()
}

// trackedStreamConnCount reports how many Dial endpoint handles the Transport
// still tracks. Used by tests to detect tracking-map leaks.
func trackedStreamConnCount(tr transport.Pipe) int {
	t, ok := tr.(*Transport)
	if !ok {
		return -1
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.streamConns)
}

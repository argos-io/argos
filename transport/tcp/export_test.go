package tcp

import "github.com/argos-io/argos/transport"

// TrackedConnCount reports how many connections the Transport still tracks.
// Used by tests to detect tracking-map leaks.
func TrackedConnCount(tr transport.Transport) int {
	t, ok := tr.(*Transport)
	if !ok {
		return -1
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.conns)
}

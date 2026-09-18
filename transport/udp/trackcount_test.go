package udp

import "github.com/argos-io/argos/transport"

// trackedConnCount reports how many connections the Transport still tracks.
// Used by tests to detect tracking-map leaks.
func trackedConnCount(tr transport.Pipe) int {
	t, ok := tr.(*Transport)
	if !ok {
		return -1
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.conns)
}

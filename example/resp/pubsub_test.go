package resp_test

import (
	"sync"
	"testing"
	"time"

	"github.com/argos-io/argos/example/resp"
)

// The gate review found a panic reachable from a real server: Store.Publish
// snapshots the subscriber channels under RLock and sends to them after
// releasing it, while Unsubscribe closes a channel under the write lock. A
// select with a default does not protect against a closed channel — the send
// case is ready, so it is selected and panics ("send on closed channel"). A
// PUBLISH handler holding the snapshot while a returning SUBSCRIBE handler runs
// its deferred Unsubscribe is enough to hit it.

// TestStorePublishWhileUnsubscribing reproduces that interleaving. The window
// is narrow, so it needs a high iteration count rather than one crafted
// schedule: the publisher never stops, and the subscriber churns one
// registration per iteration, so some unsubscribe lands between a snapshot and
// the send it was captured for.
func TestStorePublishWhileUnsubscribing(t *testing.T) {
	const subs = 32
	store := resp.NewStore()
	chs := make([]chan resp.PubMessage, subs)
	for i := range chs {
		chs[i] = store.Subscribe("news")
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			store.Publish("news", "payload")
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			idx := i % subs
			store.Unsubscribe(chs[idx])
			// Re-register on a fresh channel: the old one is left to any
			// publisher still holding it in a snapshot.
			chs[idx] = store.Subscribe("news")
		}
	}()

	time.Sleep(250 * time.Millisecond)
	close(stop)
	wg.Wait()
}

// TestStoreUnsubscribeIsIdempotent covers the second half of the same defect:
// Unsubscribe closed the channel unconditionally, so calling it twice — or
// unsubscribing a channel this Store never handed out — panicked with "close of
// closed channel".
func TestStoreUnsubscribeIsIdempotent(t *testing.T) {
	store := resp.NewStore()
	ch := store.Subscribe("news")

	store.Unsubscribe(ch)
	store.Unsubscribe(ch)         // second call: no-op
	store.Unsubscribe(ch, "news") // partial repeat after full removal: no-op

	foreign := make(chan resp.PubMessage, 1)
	store.Unsubscribe(foreign) // never registered with this Store: no-op

	if n := store.Publish("news", "payload"); n != 0 {
		t.Fatalf("Publish recipients = %d, want 0 after every subscriber left", n)
	}
}

// TestStorePublishAfterUnsubscribeIsSafe pins the delivery contract that makes
// the race above impossible: Unsubscribe does not close the subscriber channel,
// so a publisher that captured the registration in a snapshot can still finish
// its send, and a full channel is dropped by the default arm instead of
// blocking the publisher. Receivers stop through their own ctx (see
// handleSUBSCRIBE), not through a closed channel.
func TestStorePublishAfterUnsubscribeIsSafe(t *testing.T) {
	store := resp.NewStore()
	ch := store.Subscribe("news")
	store.Unsubscribe(ch)

	if n := store.Publish("news", "late"); n != 0 {
		t.Fatalf("Publish recipients = %d, want 0 after Unsubscribe", n)
	}
	sendStale(t, ch)
}

// sendStale performs the send a publisher would make from a snapshot taken just
// before Unsubscribe ran.
func sendStale(t *testing.T, ch chan resp.PubMessage) {
	t.Helper()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("send panicked (%v): Unsubscribe must not close a subscriber channel a publisher may still hold", r)
		}
	}()
	select {
	case ch <- resp.PubMessage{Channel: "news", Payload: "stale snapshot"}:
	default:
		t.Fatal("subscriber channel refused the stale-snapshot send")
	}
}

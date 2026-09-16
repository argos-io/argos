package resp

import (
	"context"
	"fmt"
	"sync"

	"github.com/argos-io/argos/descriptor"
	"github.com/argos-io/argos/filter"
	"github.com/argos-io/argos/server"
	"github.com/argos-io/argos/status"
	"github.com/argos-io/argos/stream"
)

// PubMessage is one Redis pub/sub push delivered to SUBSCRIBE callers.
type PubMessage struct {
	Channel string
	Payload string
}

// subscriber is one pub/sub registration: ch carries pushes and done is closed
// exactly once, by Unsubscribe. ch itself is never closed — a publisher that
// captured the registration in a snapshot may still be sending to it after the
// subscriber leaves, and a send on a closed channel panics even from a select
// with a default arm.
type subscriber struct {
	ch   chan PubMessage
	done chan struct{}
	once sync.Once
}

func (sub *subscriber) stop() { sub.once.Do(func() { close(sub.done) }) }

// Store is an in-process fake Redis key/value + pub/sub map (no redis dependency).
type Store struct {
	mu   sync.RWMutex
	data map[string]string
	subs map[string]map[*subscriber]struct{}
	// byCh resolves the channel handed back to Unsubscribe to its
	// registration; the entry lives as long as the channel has a membership.
	byCh map[chan PubMessage]*subscriber
}

// NewStore returns an empty Store.
func NewStore() *Store {
	return &Store{
		data: make(map[string]string),
		subs: make(map[string]map[*subscriber]struct{}),
		byCh: make(map[chan PubMessage]*subscriber),
	}
}

// Get returns the value and whether the key exists.
func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.data[key]
	return v, ok
}

// Set stores key → value.
func (s *Store) Set(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = value
}

// Subscribe registers for pushes on channels. Caller must Unsubscribe.
// The returned channel is never closed; see Unsubscribe.
func (s *Store) Subscribe(channels ...string) chan PubMessage {
	sub := &subscriber{
		ch:   make(chan PubMessage, 16),
		done: make(chan struct{}),
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.byCh[sub.ch] = sub
	for _, c := range channels {
		m := s.subs[c]
		if m == nil {
			m = make(map[*subscriber]struct{})
			s.subs[c] = m
		}
		m[sub] = struct{}{}
	}
	return sub.ch
}

// Unsubscribe removes ch from the given channels (all channels if none given)
// and, once ch holds no membership, retires the registration. It is
// idempotent: a repeat, a partial repeat, or a channel this Store never handed
// out are all no-ops.
//
// ch itself is not closed. Publish snapshots the subscriber set and delivers
// outside the lock, so a publisher can still be sending to a registration that
// Unsubscribe has just retired, and a publisher must never panic or block;
// subscribers stop through their own cancellation instead (see handleSUBSCRIBE).
func (s *Store) Unsubscribe(ch chan PubMessage, channels ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sub := s.byCh[ch]
	if sub == nil {
		return
	}
	for c, m := range s.subs {
		if _, ok := m[sub]; !ok {
			continue
		}
		drop := len(channels) == 0
		for _, want := range channels {
			if want == c {
				drop = true
				break
			}
		}
		if !drop {
			continue
		}
		delete(m, sub)
		if len(m) == 0 {
			delete(s.subs, c)
		}
	}
	for _, m := range s.subs {
		if _, ok := m[sub]; ok {
			return // still subscribed elsewhere
		}
	}
	delete(s.byCh, ch)
	sub.stop()
}

// Publish delivers payload to all subscribers of channel. Returns recipient
// count. Delivery never blocks and never panics: a subscriber that cannot take
// the message right now — slow reader, or unsubscribed after the snapshot — is
// skipped.
func (s *Store) Publish(channel, payload string) int {
	msg := PubMessage{Channel: channel, Payload: payload}
	s.mu.RLock()
	subs := s.subs[channel]
	targets := make([]*subscriber, 0, len(subs))
	for sub := range subs {
		targets = append(targets, sub)
	}
	s.mu.RUnlock()
	n := 0
	for _, sub := range targets {
		select {
		case <-sub.done:
			// Unsubscribed after the snapshot: leave ch untouched.
			continue
		default:
		}
		select {
		case sub.ch <- msg:
			n++
		default:
			// Drop if subscriber is slow — gate tests use buffered channels.
		}
	}
	return n
}

// Methods for the default redis.Redis service.
var (
	MethodPING      = descriptor.MustMethod(defaultService+".PING", descriptor.Unary)
	MethodGET       = descriptor.MustMethod(defaultService+".GET", descriptor.Unary)
	MethodSET       = descriptor.MustMethod(defaultService+".SET", descriptor.Unary)
	MethodPUBLISH   = descriptor.MustMethod(defaultService+".PUBLISH", descriptor.Unary)
	MethodSUBSCRIBE = descriptor.MustMethod(defaultService+".SUBSCRIBE", descriptor.ServerStreaming)
)

// ServiceDescriptor returns the redis.Redis service with PING/GET/SET/PUBLISH/SUBSCRIBE.
func ServiceDescriptor() descriptor.Service {
	return descriptor.MustService(defaultService,
		MethodPING, MethodGET, MethodSET, MethodPUBLISH, MethodSUBSCRIBE)
}

// Register attaches PING/GET/SET/PUBLISH/SUBSCRIBE handlers backed by store to srv.
func Register(srv *server.Server, store *Store) error {
	if store == nil {
		return fmt.Errorf("resp: nil Store")
	}
	return srv.Register(ServiceDescriptor(), map[string]filter.Handler{
		"PING":      handlePING,
		"GET":       handleGET(store),
		"SET":       handleSET(store),
		"PUBLISH":   handlePUBLISH(store),
		"SUBSCRIBE": handleSUBSCRIBE(store),
	})
}

func drainUnary(st stream.Stream) {
	var extra []byte
	_ = st.Recv(&extra) // expect io.EOF after the single request message
}

func handlePING(_ context.Context, _ descriptor.Method, st stream.Stream) error {
	var args []byte
	if err := st.Recv(&args); err != nil {
		return err
	}
	drainUnary(st)
	return st.Send(EncodeSimple("PONG"))
}

func handleGET(store *Store) filter.Handler {
	return func(_ context.Context, _ descriptor.Method, st stream.Stream) error {
		var raw []byte
		if err := st.Recv(&raw); err != nil {
			return err
		}
		drainUnary(st)
		args, err := DecodeArgs(raw)
		if err != nil {
			return status.Error(status.InvalidArgument, err.Error())
		}
		if len(args) != 1 {
			return status.Error(status.InvalidArgument, "GET requires 1 arg")
		}
		if v, ok := store.Get(args[0]); ok {
			return st.Send(EncodeBulk(v))
		}
		return st.Send(EncodeBulkNull())
	}
}

func handleSET(store *Store) filter.Handler {
	return func(_ context.Context, _ descriptor.Method, st stream.Stream) error {
		var raw []byte
		if err := st.Recv(&raw); err != nil {
			return err
		}
		drainUnary(st)
		args, err := DecodeArgs(raw)
		if err != nil {
			return status.Error(status.InvalidArgument, err.Error())
		}
		if len(args) != 2 {
			return status.Error(status.InvalidArgument, "SET requires 2 args")
		}
		store.Set(args[0], args[1])
		return st.Send(EncodeSimple("OK"))
	}
}

func handlePUBLISH(store *Store) filter.Handler {
	return func(_ context.Context, _ descriptor.Method, st stream.Stream) error {
		var raw []byte
		if err := st.Recv(&raw); err != nil {
			return err
		}
		drainUnary(st)
		args, err := DecodeArgs(raw)
		if err != nil {
			return status.Error(status.InvalidArgument, err.Error())
		}
		if len(args) != 2 {
			return status.Error(status.InvalidArgument, "PUBLISH requires channel and message")
		}
		n := store.Publish(args[0], args[1])
		return st.Send([]byte(fmt.Sprintf(":%d\r\n", n)))
	}
}

func handleSUBSCRIBE(store *Store) filter.Handler {
	return func(ctx context.Context, _ descriptor.Method, st stream.Stream) error {
		var raw []byte
		if err := st.Recv(&raw); err != nil {
			return err
		}
		drainUnary(st)
		args, err := DecodeArgs(raw)
		if err != nil {
			return status.Error(status.InvalidArgument, err.Error())
		}
		if len(args) < 1 {
			return status.Error(status.InvalidArgument, "SUBSCRIBE requires at least 1 channel")
		}
		ch := store.Subscribe(args...)
		defer store.Unsubscribe(ch)
		peerGone := PeerGone()

		for i, channel := range args {
			if err := st.Send(EncodeSubscribeAck(channel, i+1)); err != nil {
				return err
			}
		}
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-peerGone:
				return nil
			case msg := <-ch:
				// ch is never closed (Unsubscribe only retires the
				// registration), so this readiness is always a push.
				if err := st.Send(EncodePushMessage(msg.Channel, msg.Payload)); err != nil {
					return err
				}
			}
		}
	}
}

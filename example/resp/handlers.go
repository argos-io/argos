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

// Store is an in-process fake Redis key/value + pub/sub map (no redis dependency).
type Store struct {
	mu   sync.RWMutex
	data map[string]string
	subs map[string]map[chan PubMessage]struct{}
}

// NewStore returns an empty Store.
func NewStore() *Store {
	return &Store{
		data: make(map[string]string),
		subs: make(map[string]map[chan PubMessage]struct{}),
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
func (s *Store) Subscribe(channels ...string) chan PubMessage {
	ch := make(chan PubMessage, 16)
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range channels {
		m := s.subs[c]
		if m == nil {
			m = make(map[chan PubMessage]struct{})
			s.subs[c] = m
		}
		m[ch] = struct{}{}
	}
	return ch
}

// Unsubscribe removes ch from the given channels (all channels if none given)
// and closes ch when it has no remaining membership.
func (s *Store) Unsubscribe(ch chan PubMessage, channels ...string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for c, m := range s.subs {
		if _, ok := m[ch]; !ok {
			continue
		}
		drop := len(channels) == 0
		if !drop {
			for _, want := range channels {
				if want == c {
					drop = true
					break
				}
			}
		}
		if !drop {
			continue
		}
		delete(m, ch)
		if len(m) == 0 {
			delete(s.subs, c)
		}
	}
	for _, m := range s.subs {
		if _, ok := m[ch]; ok {
			return
		}
	}
	close(ch)
}

// Publish delivers payload to all subscribers of channel. Returns recipient count.
func (s *Store) Publish(channel, payload string) int {
	msg := PubMessage{Channel: channel, Payload: payload}
	s.mu.RLock()
	subs := s.subs[channel]
	targets := make([]chan PubMessage, 0, len(subs))
	for ch := range subs {
		targets = append(targets, ch)
	}
	s.mu.RUnlock()
	n := 0
	for _, ch := range targets {
		select {
		case ch <- msg:
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
			case msg, ok := <-ch:
				if !ok {
					return nil
				}
				if err := st.Send(EncodePushMessage(msg.Channel, msg.Payload)); err != nil {
					return err
				}
			}
		}
	}
}

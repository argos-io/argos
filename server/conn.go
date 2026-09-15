package server

import (
	"context"
	"errors"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/argos-io/argos"
	"github.com/argos-io/argos/budget"
	"github.com/argos-io/argos/filter"
	"github.com/argos-io/argos/framing"
	"github.com/argos-io/argos/metadata"
	"github.com/argos-io/argos/status"
	"github.com/argos-io/argos/stream"
	"github.com/argos-io/argos/transport"
)

func (s *Server) onConn(lb *liveBinding, routes map[string]map[string]routeEntry, filters []filter.Filter, c transport.Conn) {
	cfg := lb.cfg
	maxConns := int64(cfg.MaxInboundConns)
	if n := lb.active.Add(1); maxConns > 0 && n > maxConns {
		lb.active.Add(-1)
		argos.NotifyConnError(cfg, argos.ConnInfo{
			Side:    argos.SideServer,
			Binding: lb.name,
			Phase:   argos.ConnPhaseAdmit,
		}, status.Error(status.ResourceExhausted, "server: MaxInboundConns exceeded"))
		_ = c.Close()
		return
	}
	defer lb.active.Add(-1)

	connCtx, connCancel := context.WithCancelCause(context.Background())
	defer connCancel(nil)

	acceptCtx, acceptCancel := context.WithCancel(connCtx)
	defer acceptCancel()

	id := new(uint64)
	s.mu.Lock()
	s.acceptCancels[id] = acceptCancel
	s.connCancels[id] = connCancel
	closed := s.closed
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.acceptCancels, id)
		delete(s.connCancels, id)
		s.mu.Unlock()
	}()
	if closed {
		_ = c.Close()
		return
	}

	var aged atomic.Bool
	ageTimer := time.AfterFunc(cfg.MaxInboundConnAge, func() {
		aged.Store(true)
		acceptCancel()
	})
	defer ageTimer.Stop()

	hsCtx, hsCancel := context.WithTimeout(connCtx, cfg.HandshakeTimeout)
	sess, err := lb.framing.NewServerSession(hsCtx, c, lb.sessSpec)
	hsCancel()
	if err != nil {
		argos.NotifyConnError(cfg, argos.ConnInfo{
			Side:    argos.SideServer,
			Binding: lb.name,
			Phase:   argos.ConnPhaseHandshake,
		}, err)
		_ = c.Close()
		return
	}
	defer func() { _ = sess.Close() }()

	var callWG sync.WaitGroup
	defer callWG.Wait()

	for {
		var callHolder atomic.Pointer[framing.ServerCall]
		md := metadata.New(metadata.RoleResponder, func(metadata.Metadata) error {
			p := callHolder.Load()
			if p == nil || *p == nil {
				return status.Error(status.Unimplemented, "server: SendHeaders before accept")
			}
			return (*p).SendHeaders()
		})
		spec := framing.CallSpec{Metadata: md}

		waitCtx, waitCancel := context.WithCancel(acceptCtx)
		idleTimer := time.AfterFunc(cfg.MaxInboundConnIdle, waitCancel)

		call, err := sess.AcceptCall(waitCtx, spec)
		idleTimer.Stop()
		waitCancel()

		if err != nil {
			if errors.Is(err, framing.ErrCallRejected) {
				s.handleRejected(call, err)
				continue
			}
			if errors.Is(err, io.EOF) {
				// Normal AcceptCall end: peer closed a Sequential/OneCall
				// session, or a one-shot Concurrent CarrierConn (gRPC×HTTP/2)
				// has no further calls. Do not cancel in-flight Concurrent
				// handlers with ErrPeerGone — that races Finish into
				// Unavailable while the handler is still completing.
				// defer callWG.Wait() drains in-flight; defer connCancel(nil)
				// releases the connection context afterward.
				return
			}
			// acceptCtx / idle / age / shutdown wake — clean loop exit.
			if acceptCtx.Err() != nil || waitCtx.Err() != nil {
				if aged.Load() {
					connCancel(ErrSessionExpired)
				}
				return
			}
			argos.NotifyConnError(cfg, argos.ConnInfo{
				Side:    argos.SideServer,
				Binding: lb.name,
				Phase:   argos.ConnPhaseAccept,
			}, err)
			connCancel(ErrPeerGone)
			return
		}

		callHolder.Store(&call)

		switch lb.reuse {
		case framing.Concurrent:
			callWG.Add(1)
			go func(call framing.ServerCall, md metadata.CallMetadata) {
				defer callWG.Done()
				s.handleCall(connCtx, lb, routes, filters, call, md)
			}(call, md)
		default: // Sequential, OneCallPerConn
			s.handleCall(connCtx, lb, routes, filters, call, md)
		}
	}
}

func (s *Server) handleRejected(call framing.ServerCall, err error) {
	finishErr := status.Error(status.InvalidArgument, err.Error())
	var se *status.StatusError
	if errors.As(err, &se) {
		finishErr = se
	}
	if call != nil {
		_ = call.Finish(finishErr)
		_ = call.Close()
	}
}

func (s *Server) handleCall(
	connCtx context.Context,
	lb *liveBinding,
	routes map[string]map[string]routeEntry,
	filters []filter.Filter,
	call framing.ServerCall,
	md metadata.CallMetadata,
) {
	defer func() { _ = call.Close() }()

	b, release, err := s.admit.tryAdmit()
	if err != nil {
		_ = call.Finish(err)
		argos.NotifyCallError(lb.cfg, argos.CallInfo{
			Method: call.Method(),
			Phase:  argos.PhaseOpen,
		}, err)
		return
	}
	defer release()

	callCtx, callCancel := deriveCallCtx(connCtx, call)
	defer callCancel()

	callCtx = metadata.ContextWith(callCtx, md)
	callCtx = budget.ContextWith(callCtx, b)

	methodName := call.Method()
	entry, ok := lookup(routes, methodName)
	if !ok {
		ferr := status.Error(status.Unimplemented, "server: method not found: "+methodName)
		_ = call.Finish(ferr)
		argos.NotifyCallError(lb.cfg, argos.CallInfo{
			Method: methodName,
			Phase:  argos.PhaseDispatch,
		}, ferr)
		return
	}

	if err := call.Accept(entry.method); err != nil {
		ferr := err
		if status.CodeOf(err) == status.Unknown {
			ferr = status.Error(status.Unimplemented, err.Error())
		}
		_ = call.Finish(ferr)
		argos.NotifyCallError(lb.cfg, argos.CallInfo{
			Service: entry.method.Service(),
			Method:  entry.method.Name(),
			Phase:   argos.PhaseDispatch,
		}, ferr)
		return
	}

	st := stream.Wrap(call, lb.codec)
	chain := filter.Chain(filters, entry.handler)
	herr := chain(callCtx, entry.method, st)
	ferr := finishError(callCtx, herr)
	if err := call.Finish(ferr); err != nil {
		argos.NotifyCallError(lb.cfg, argos.CallInfo{
			Service: entry.method.Service(),
			Method:  entry.method.Name(),
			Phase:   argos.PhaseFinish,
		}, err)
	}
}

func deriveCallCtx(connCtx context.Context, call framing.Call) (context.Context, context.CancelFunc) {
	if dl, ok := call.Deadline(); ok {
		return context.WithDeadline(connCtx, dl)
	}
	return context.WithCancel(connCtx)
}

func lookup(routes map[string]map[string]routeEntry, fullMethod string) (routeEntry, bool) {
	svc, name, ok := splitMethod(fullMethod)
	if !ok {
		return routeEntry{}, false
	}
	methods, ok := routes[svc]
	if !ok {
		return routeEntry{}, false
	}
	e, ok := methods[name]
	return e, ok
}

func splitMethod(full string) (service, name string, ok bool) {
	i := strings.LastIndexByte(full, '.')
	if i <= 0 || i == len(full)-1 {
		return "", "", false
	}
	return full[:i], full[i+1:], true
}

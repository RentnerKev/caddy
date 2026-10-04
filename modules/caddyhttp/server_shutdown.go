// Copyright 2026 The Caddy Authors
// Licensed under the Apache License, Version 2.0.

package caddyhttp

import (
	"context"
	"net"
	"net/http"
	"sync"
	"time"
)

// shutdownListener lets Caddy retire listeners before calling http.Server.Shutdown,
// which closes them again. Module listener wrappers are not necessarily idempotent.
type shutdownListener struct {
	net.Listener
	once sync.Once
	err  error
}

func (ln *shutdownListener) Close() error {
	ln.once.Do(func() { ln.err = ln.Listener.Close() })
	return ln.err
}

type acceptedConnContextKey struct{}

func (s *Server) acceptedConnectionChanged() {
	if s.acceptedChanged != nil {
		close(s.acceptedChanged)
	}
	s.acceptedChanged = make(chan struct{})
}

// StateActive is deliberately excluded: net/http sets it before checking its
// shutdown flag, so it does not prove the first request has entered a handler.
func (s *Server) trackAcceptedConnection(conn net.Conn, state http.ConnState) {
	s.acceptedMu.Lock()
	defer s.acceptedMu.Unlock()
	switch state {
	case http.StateNew:
		if s.acceptedFirst == nil {
			s.acceptedFirst = make(map[net.Conn]time.Time)
		}
		s.acceptedFirst[conn] = time.Now()
		s.acceptedConnectionChanged()
	case http.StateActive:
		// Active precedes net/http's shutdown check; wait for ServeHTTP instead.
		return
	case http.StateIdle, http.StateClosed, http.StateHijacked:
		if _, ok := s.acceptedFirst[conn]; ok {
			delete(s.acceptedFirst, conn)
			s.acceptedConnectionChanged()
		}
	}
}

func (s *Server) admitAcceptedConnection(r *http.Request) {
	conn, ok := r.Context().Value(acceptedConnContextKey{}).(net.Conn)
	if !ok {
		return
	} // HTTP/3 and manually constructed request contexts.
	s.acceptedMu.Lock()
	defer s.acceptedMu.Unlock()
	if _, ok := s.acceptedFirst[conn]; ok {
		delete(s.acceptedFirst, conn)
		s.acceptedConnectionChanged()
	}
}

// awaitAcceptedRequests is called after Serve loops stop adding connections.
// Preserve a socket's first request across reload; headerless sockets must not
// hold an eternal grace period open. The read-header budget starts at acceptance,
// and the shared grace deadline can shorten it. Reusing an already-idle keepalive
// connection still has net/http's ordinary shutdown race.
func (s *Server) awaitAcceptedRequests(ctx context.Context) {
	headerBudget := time.Duration(s.ReadHeaderTimeout)
	if headerBudget <= 0 {
		headerBudget = time.Duration(defaultReadHeaderTimeout)
	}
	for {
		s.acceptedMu.Lock()
		if len(s.acceptedFirst) == 0 {
			s.acceptedMu.Unlock()
			return
		}
		changed := s.acceptedChanged
		var deadline time.Time
		for _, accepted := range s.acceptedFirst {
			expires := accepted.Add(headerBudget)
			if deadline.IsZero() || expires.Before(deadline) {
				deadline = expires
			}
		}
		s.acceptedMu.Unlock()
		timer := time.NewTimer(time.Until(deadline))
		select {
		case <-changed:
			timer.Stop()
		case <-ctx.Done():
			timer.Stop()
			s.closeUnadmittedConnections(time.Time{})
			return
		case <-timer.C:
			s.closeUnadmittedConnections(time.Now().Add(-headerBudget))
		}
	}
}

// A zero cutoff closes every unadmitted socket at grace expiry. Otherwise only
// sockets accepted on or before the cutoff have exhausted their header budget.
func (s *Server) closeUnadmittedConnections(cutoff time.Time) {
	s.acceptedMu.Lock()
	var expired []net.Conn
	for conn, accepted := range s.acceptedFirst {
		if cutoff.IsZero() || !accepted.After(cutoff) {
			expired = append(expired, conn)
			delete(s.acceptedFirst, conn)
		}
	}
	if len(expired) > 0 {
		s.acceptedConnectionChanged()
	}
	s.acceptedMu.Unlock()
	for _, conn := range expired {
		_ = conn.Close()
	}
}

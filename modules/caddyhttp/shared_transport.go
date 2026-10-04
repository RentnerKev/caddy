// Copyright 2026 The Caddy Authors
// Licensed under the Apache License, Version 2.0.

package caddyhttp

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"runtime"
	"slices"
	"strings"
	"sync"

	"go.uber.org/zap"

	"github.com/caddyserver/caddy/v2"
)

// sharedTransportKey describes the complete persistent transport, deliberately
// excluding generation-specific routes, handlers, and server names.
func sharedTransportKey(app *App, srv *Server) (string, bool) {
	if app.GracePeriod != 0 || app.ShutdownDelay != 0 || len(srv.TLSConnPolicies) != 0 ||
		len(srv.connStateFuncs) != 0 || len(srv.connContextFuncs) != 0 ||
		len(srv.listenerWrappers) != 0 || len(srv.packetConnWrappers) != 0 ||
		len(srv.ListenerWrappersRaw) != 0 || len(srv.PacketConnWrappersRaw) != 0 || len(srv.Listen) == 0 {
		return "", false
	}
	canonicalProtocols := func(protocols []string) ([]string, bool) {
		protocols = slices.Clone(protocols)
		slices.Sort(protocols)
		protocols = slices.Compact(protocols)
		// h2 currently also enables net/http's unencrypted HTTP/2 parser.
		// Retain that behavior and its explicit transport identity.
		for _, p := range protocols {
			if p != "h1" && p != "h2" && p != "h3" {
				return nil, false
			}
		}
		return protocols, slices.Contains(protocols, "h1")
	}
	serverProtocols, ok := canonicalProtocols(srv.protocolsWithDefaults())
	if !ok {
		return "", false
	}
	listeners := make([]string, 0, len(srv.Listen))
	for i, listen := range srv.Listen {
		addr, err := caddy.ParseNetworkAddress(listen)
		if err != nil || (addr.Network != "tcp" && addr.Network != "tcp4" && addr.Network != "tcp6") || addr.StartPort == 0 {
			return "", false
		}
		protocols := serverProtocols
		if len(srv.ListenProtocols) != 0 {
			if len(srv.ListenProtocols) != len(srv.Listen) {
				return "", false
			}
			protocols, ok = canonicalProtocols(srv.listenerProtocolsWithDefaults(srv.ListenProtocols[i]))
			if !ok {
				return "", false
			}
		}
		for port := uint(0); port < addr.PortRangeSize(); port++ {
			listeners = append(listeners, addr.Network+"/"+addr.JoinHostPort(port)+"/"+strings.Join(protocols, ","))
		}
	}
	slices.Sort(listeners)
	key := struct {
		Listeners                     []string
		Protocols                     []string
		Read, ReadHeader, Write, Idle caddy.Duration
		Keepalive, KeepaliveIdle      caddy.Duration
		KeepaliveCount, MaxHeader     int
	}{
		listeners, serverProtocols, srv.ReadTimeout, srv.ReadHeaderTimeout, srv.WriteTimeout, srv.IdleTimeout,
		srv.KeepAliveInterval, srv.KeepAliveIdle, srv.KeepAliveCount, srv.MaxHeaderBytes,
	}
	encoded, _ := json.Marshal(key)
	return string(encoded), true
}

var sharedHTTPTransports = struct {
	sync.Mutex
	groups map[string]*sharedHTTPTransport
}{groups: make(map[string]*sharedHTTPTransport)}

type sharedHTTPGeneration struct {
	release      func()
	logger       *zap.Logger
	shutdownOnce sync.Once
	shutdown     sync.WaitGroup
}

// Lock order is registry -> group -> Server.requestMu. The immutable native
// server owns connections; generations own handler resources and cleanup holds.
type sharedHTTPTransport struct {
	key          string
	mu           sync.Mutex
	owners       []*Server
	owner        *Server
	generations  map[*Server]*sharedHTTPGeneration
	server       *http.Server
	listeners    []net.Listener
	addresses    []caddy.NetworkAddress
	serveLoops   sync.WaitGroup
	accepted     acceptedConnectionTracker
	logger       sharedHTTPLogger
	connMu       sync.Mutex
	connections  map[net.Conn]struct{}
	connectionWG sync.WaitGroup
}

// acquireSharedHTTPTransport publishes the generation before returning. Failed
// Start must retire it, restoring the newest surviving owner.
func acquireSharedHTTPTransport(key string, srv *Server, native *http.Server, logger *zap.Logger) (*sharedHTTPTransport, bool) {
	generation := &sharedHTTPGeneration{release: srv.ctx.HoldCleanup(), logger: logger}
	sharedHTTPTransports.Lock()
	defer sharedHTTPTransports.Unlock()
	g, reused := sharedHTTPTransports.groups[key]
	if !reused {
		g = &sharedHTTPTransport{key: key, server: native, generations: make(map[*Server]*sharedHTTPGeneration)}
		g.logger = sharedHTTPLogger{group: g}
		native.Handler = g
		native.ErrorLog = log.New(sharedHTTPLogWriter{group: g}, "", 0)
		// http2.ConfigureServer's callback belongs to the transport, not a route generation.
		baseState := native.ConnState
		native.ConnState = func(conn net.Conn, state http.ConnState) {
			g.trackConnection(conn, state)
			g.accepted.track(conn, state)
			if baseState != nil {
				baseState(conn, state)
			}
		}
		native.ConnContext = func(ctx context.Context, conn net.Conn) context.Context {
			return context.WithValue(ctx, acceptedConnContextKey{}, conn)
		}
		sharedHTTPTransports.groups[key] = g
	}
	g.mu.Lock()
	g.generations[srv] = generation
	g.owners = append(g.owners, srv)
	g.owner = srv
	g.mu.Unlock()
	return g, reused
}

func (g *sharedHTTPTransport) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	g.mu.Lock()
	owner := g.owner
	if owner == nil || !owner.beginRequest() {
		g.mu.Unlock()
		http.Error(w, "server shutting down", http.StatusServiceUnavailable)
		return
	}
	logger := g.generations[owner].logger
	remoteAddr := r.RemoteAddr
	g.mu.Unlock()
	defer owner.requests.Done()
	returned := false
	defer func() {
		value := recover()
		if !returned {
			if value != nil && value != http.ErrAbortHandler {
				stack := make([]byte, 64<<10)
				stack = stack[:runtime.Stack(stack, false)]
				logger.Error(fmt.Sprintf("http: panic serving %v: %v\n%s", remoteAddr, value, stack))
			}
			// Native recovery must still abort with GODEBUG=panicnil=1,
			// where a nil panic recovers as nil. Suppress duplicate logging
			// after this generation's resources have been released.
			panic(http.ErrAbortHandler)
		}
	}()
	g.accepted.admit(r)
	if r.ProtoMajor == 1 {
		w = wrapNativeResponseWriter(w)
	}
	owner.serveAdmittedHTTP(w, r)
	returned = true
}

// retire removes only a live binding. The last generation remains available for
// accepted first requests and logging until its native transport has drained.
func (g *sharedHTTPTransport) retire(srv *Server) bool {
	sharedHTTPTransports.Lock()
	defer sharedHTTPTransports.Unlock()
	g.mu.Lock()
	defer g.mu.Unlock()
	for i, owner := range g.owners {
		if owner == srv {
			g.owners = slices.Delete(g.owners, i, i+1)
			break
		}
	}
	if len(g.owners) != 0 {
		if g.owner == srv {
			g.owner = g.owners[len(g.owners)-1]
		}
		return true
	}
	if sharedHTTPTransports.groups[g.key] == g {
		delete(sharedHTTPTransports.groups, g.key)
	}
	return false
}

func (g *sharedHTTPTransport) seal() {
	g.mu.Lock()
	g.owner = nil
	g.mu.Unlock()
}

func (g *sharedHTTPTransport) beginGenerationShutdown(srv *Server) {
	g.mu.Lock()
	generation := g.generations[srv]
	g.mu.Unlock()
	if generation == nil {
		return
	}
	generation.shutdownOnce.Do(func() {
		generation.shutdown.Add(len(srv.onShutdownFuncs))
		for _, hook := range srv.onShutdownFuncs {
			go func(f func()) { defer generation.shutdown.Done(); f() }(hook)
		}
	})
}

func (g *sharedHTTPTransport) waitGenerationShutdown(srv *Server) {
	// Ensure Add has completed even if a concurrent caller starts shutdown.
	g.beginGenerationShutdown(srv)
	g.mu.Lock()
	generation := g.generations[srv]
	g.mu.Unlock()
	if generation != nil {
		generation.shutdown.Wait()
	}
}

// releaseGeneration follows hook completion and finishRequests in App's finalizer.
func (g *sharedHTTPTransport) releaseGeneration(srv *Server) {
	g.mu.Lock()
	generation := g.generations[srv]
	delete(g.generations, srv)
	g.mu.Unlock()
	if generation != nil {
		generation.release()
	}
}

func (g *sharedHTTPTransport) trackConnection(conn net.Conn, state http.ConnState) {
	g.connMu.Lock()
	defer g.connMu.Unlock()
	switch state {
	case http.StateNew:
		if g.connections == nil {
			g.connections = make(map[net.Conn]struct{})
		}
		if _, exists := g.connections[conn]; !exists {
			g.connections[conn] = struct{}{}
			g.connectionWG.Add(1)
		}
	case http.StateActive, http.StateIdle:
		// Both states still belong to a live connection.
		return
	case http.StateClosed, http.StateHijacked:
		if _, exists := g.connections[conn]; exists {
			delete(g.connections, conn)
			g.connectionWG.Done()
		}
	}
}

// Call only after serveLoops.Wait and Shutdown/Close: no new connections may Add.
func (g *sharedHTTPTransport) waitConnections() { g.connectionWG.Wait() }

func (g *sharedHTTPTransport) leaseLogger() (*zap.Logger, func()) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.owner == nil || !g.owner.beginRequest() {
		return nil, nil
	}
	generation := g.generations[g.owner]
	if generation == nil {
		g.owner.requests.Done()
		return nil, nil
	}
	return generation.logger, g.owner.requests.Done
}

type sharedHTTPLogger struct{ group *sharedHTTPTransport }

func (l sharedHTTPLogger) Debug(msg string, fields ...zap.Field) {
	logger, done := l.group.leaseLogger()
	if done != nil {
		defer done()
		logger.Debug(msg, fields...)
	}
}

func (l sharedHTTPLogger) Warn(msg string, fields ...zap.Field) {
	logger, done := l.group.leaseLogger()
	if done != nil {
		defer done()
		logger.Warn(msg, fields...)
	}
}

type sharedHTTPLogWriter struct{ group *sharedHTTPTransport }

func (w sharedHTTPLogWriter) Write(p []byte) (int, error) {
	logger, done := w.group.leaseLogger()
	if done != nil {
		defer done()
		_, _ = (stdlibLogRouter{logger: logger}).Write(p)
	}
	return len(p), nil
}

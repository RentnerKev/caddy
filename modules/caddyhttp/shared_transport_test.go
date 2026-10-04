// Copyright 2026 The Caddy Authors
// Licensed under the Apache License, Version 2.0.

package caddyhttp

import (
	"context"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/modules/caddytls"
)

func sharedTransportTestServer(t *testing.T) *Server {
	t.Helper()
	ctx, cancel := caddy.NewContext(caddy.Context{Context: context.Background()})
	t.Cleanup(cancel)
	return &Server{ctx: ctx, Listen: []string{":8123"}, Protocols: []string{"h1", "h2"}}
}

func TestSharedTransportIdentity(t *testing.T) {
	app := new(App)
	a := &Server{Listen: []string{":8123", "tcp4/127.0.0.1:8124"}, Protocols: []string{"h1", "h2"}, name: "before"}
	b := &Server{Listen: []string{"tcp4/127.0.0.1:8124", "tcp/:8123"}, Protocols: []string{"h2", "h1", "h1"}, name: "after"}
	key, ok := sharedTransportKey(app, a)
	other, otherOK := sharedTransportKey(app, b)
	if !ok || !otherOK || key != other {
		t.Fatal("reordered listeners/protocols or server rename changed transport identity")
	}
	b.ListenProtocols = [][]string{{"h1"}, {"h1", "h2"}}
	changed, ok := sharedTransportKey(app, b)
	if !ok || changed == key {
		t.Fatal("listener protocol change must have a distinct identity")
	}
	for name, mutate := range map[string]func(*Server){
		"read":            func(s *Server) { s.ReadTimeout = 1 },
		"header":          func(s *Server) { s.ReadHeaderTimeout = 1 },
		"write":           func(s *Server) { s.WriteTimeout = 1 },
		"idle":            func(s *Server) { s.IdleTimeout = 1 },
		"keepalive":       func(s *Server) { s.KeepAliveInterval = 1 },
		"keepalive idle":  func(s *Server) { s.KeepAliveIdle = 1 },
		"keepalive count": func(s *Server) { s.KeepAliveCount = 1 },
		"header limit":    func(s *Server) { s.MaxHeaderBytes = 1 },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := Server{Listen: a.Listen, Protocols: a.Protocols}
			mutate(&candidate)
			got, eligible := sharedTransportKey(app, &candidate)
			if !eligible || got == key {
				t.Fatal("transport setting was omitted from key")
			}
		})
	}
}

func TestSharedTransportEligibility(t *testing.T) {
	cases := map[string]func(*App, *Server){
		"grace":     func(a *App, _ *Server) { a.GracePeriod = 1 },
		"delay":     func(a *App, _ *Server) { a.ShutdownDelay = 1 },
		"ephemeral": func(_ *App, s *Server) { s.Listen = []string{":0"} },
		"unix":      func(_ *App, s *Server) { s.Listen = []string{"unix//tmp/transport.sock"} },
		"tls": func(_ *App, s *Server) {
			s.TLSConnPolicies = caddytls.ConnectionPolicies{new(caddytls.ConnectionPolicy)}
		},
		"h2c":          func(_ *App, s *Server) { s.Protocols = []string{"h1", "h2c"} },
		"listener h2c": func(_ *App, s *Server) { s.ListenProtocols = [][]string{{"h1", "h2c"}} },
		"state hook": func(_ *App, s *Server) {
			s.connStateFuncs = []func(net.Conn, http.ConnState){func(net.Conn, http.ConnState) {}}
		},
		"context hook": func(_ *App, s *Server) {
			s.connContextFuncs = []func(context.Context, net.Conn) context.Context{func(ctx context.Context, _ net.Conn) context.Context { return ctx }}
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			app, srv := new(App), &Server{Listen: []string{":8123"}, Protocols: []string{"h1", "h2"}}
			mutate(app, srv)
			if _, ok := sharedTransportKey(app, srv); ok {
				t.Fatal("unsupported transport was eligible")
			}
		})
	}
}

func TestSharedTransportPublishRollbackAndFinalRetirement(t *testing.T) {
	old, failed := sharedTransportTestServer(t), sharedTransportTestServer(t)

	native := new(http.Server)
	g, reused := acquireSharedHTTPTransport(t.Name(), old, native, zap.NewNop())
	if reused {
		t.Fatal("unexpected reuse")
	}
	next, reused := acquireSharedHTTPTransport(t.Name(), failed, new(http.Server), zap.NewNop())
	if !reused || next != g || g.server != native || g.owner != failed {
		t.Fatal("new generation did not reuse and publish")
	}
	if !g.retire(failed) || g.owner != old {
		t.Fatal("failed Start did not restore old generation")
	}
	g.releaseGeneration(failed)
	if g.retire(old) || g.owner != old {
		t.Fatal("final retirement must retain dispatch owner until drain")
	}
	replacement := sharedTransportTestServer(t)
	fresh, reused := acquireSharedHTTPTransport(t.Name(), replacement, new(http.Server), zap.NewNop())
	if reused || fresh == g {
		t.Fatal("closing transport remained discoverable")
	}
	// Repeated retirement of the old group must not unregister its replacement.
	g.retire(old)
	sharedHTTPTransports.Lock()
	registered := sharedHTTPTransports.groups[t.Name()]
	sharedHTTPTransports.Unlock()
	if registered != fresh {
		t.Fatal("old retirement removed replacement")
	}
	g.seal()
	g.releaseGeneration(old)
	fresh.retire(replacement)
	fresh.seal()
	fresh.releaseGeneration(replacement)
}

func TestSharedTransportShutdownHooksKeepGenerationResources(t *testing.T) {
	srv := sharedTransportTestServer(t)
	ctx, cancel := caddy.NewContext(caddy.Context{Context: context.Background()})
	srv.ctx = ctx
	t.Cleanup(cancel)
	cleaned := make(chan struct{})
	srv.ctx.OnCancel(func() { close(cleaned) })
	started, unblock := make(chan struct{}), make(chan struct{})
	var count atomic.Int32
	srv.onShutdownFuncs = []func(){func() { count.Add(1); close(started); <-unblock }}
	g, _ := acquireSharedHTTPTransport(t.Name(), srv, new(http.Server), zap.NewNop())
	g.beginGenerationShutdown(srv)
	g.beginGenerationShutdown(srv)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("shutdown hook did not start")
	}
	if count.Load() != 1 {
		t.Fatal("shutdown hook ran more than once")
	}
	// Cancellation must defer resource cleanup while the hook still runs.
	cancel()
	select {
	case <-cleaned:
		t.Fatal("resources cleaned before generation release")
	default:
	}
	close(unblock)
	g.waitGenerationShutdown(srv)
	g.retire(srv)
	g.seal()
	g.releaseGeneration(srv)
	select {
	case <-cleaned:
	case <-time.After(time.Second):
		t.Fatal("generation cleanup did not resume")
	}
}

func TestSharedTransportLogsFollowGenerationAndLease(t *testing.T) {
	old, next := sharedTransportTestServer(t), sharedTransportTestServer(t)
	oldCore, oldLogs := observer.New(zapcore.DebugLevel)
	nextCore, nextLogs := observer.New(zapcore.DebugLevel)
	g, _ := acquireSharedHTTPTransport(t.Name(), old, new(http.Server), zap.New(oldCore))
	acquireSharedHTTPTransport(t.Name(), next, new(http.Server), zap.New(nextCore))
	g.server.ErrorLog.Print("http: panic serving test: failed")
	if oldLogs.Len() != 0 || nextLogs.Len() != 1 || nextLogs.All()[0].Level != zapcore.ErrorLevel {
		t.Fatal("panic did not use published generation's logger")
	}
	g.retire(next)
	g.server.ErrorLog.Print("transport debug")
	if oldLogs.Len() != 1 || oldLogs.All()[0].Level != zapcore.DebugLevel {
		t.Fatal("rollback did not restore logging owner")
	}
	logger, release := g.leaseLogger()
	if logger == nil || release == nil {
		t.Fatal("logging lease missing")
	}
	finished := make(chan struct{})
	go func() { old.finishRequests(); close(finished) }()
	select {
	case <-finished:
		t.Fatal("generation finalized during logging lease")
	case <-time.After(20 * time.Millisecond):
	}
	release()
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("released logging lease still blocks finalization")
	}
	g.retire(old)
	g.seal()
	before := oldLogs.Len()
	g.logger.Warn("sealed")
	if oldLogs.Len() != before {
		t.Fatal("sealed transport kept a logger")
	}
	g.releaseGeneration(next)
	g.releaseGeneration(old)
}

func TestSharedTransportOwnsConnectionCallbacks(t *testing.T) {
	srv := sharedTransportTestServer(t)
	var callbacks atomic.Int32
	native := &http.Server{ConnState: func(net.Conn, http.ConnState) { callbacks.Add(1) }}
	g, _ := acquireSharedHTTPTransport(t.Name(), srv, native, zap.NewNop())
	defer func() { g.retire(srv); g.seal(); g.releaseGeneration(srv) }()
	conn, peer := net.Pipe()
	defer conn.Close()
	defer peer.Close()
	ctx := native.ConnContext(context.Background(), conn)
	if got := ctx.Value(acceptedConnContextKey{}); got != conn {
		t.Fatal("transport did not attach accepted connection")
	}
	native.ConnState(conn, http.StateNew)
	native.ConnState(conn, http.StateActive)
	finished := make(chan struct{})
	go func() { g.waitConnections(); close(finished) }()
	select {
	case <-finished:
		t.Fatal("live connection disappeared from transport lifetime")
	case <-time.After(20 * time.Millisecond):
	}
	native.ConnState(conn, http.StateHijacked)
	native.ConnState(conn, http.StateClosed)
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("terminal connection state did not release transport")
	}
	if callbacks.Load() != 4 {
		t.Fatal("native transport callback was not preserved")
	}
}

func TestSharedTransportSuccessfulReloadKeepsNewOwner(t *testing.T) {
	first, second, third := sharedTransportTestServer(t), sharedTransportTestServer(t), sharedTransportTestServer(t)
	g, _ := acquireSharedHTTPTransport(t.Name(), first, new(http.Server), zap.NewNop())
	acquireSharedHTTPTransport(t.Name(), second, new(http.Server), zap.NewNop())
	acquireSharedHTTPTransport(t.Name(), third, new(http.Server), zap.NewNop())
	if !g.retire(first) || g.owner != third {
		t.Fatal("retiring old generation displaced published generation")
	}
	g.releaseGeneration(first)
	if !g.retire(third) || g.owner != second {
		t.Fatal("failed latest generation did not restore surviving owner")
	}
	g.releaseGeneration(third)
	g.retire(second)
	g.seal()
	g.releaseGeneration(second)
}

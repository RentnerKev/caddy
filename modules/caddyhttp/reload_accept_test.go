// Copyright 2026 The Caddy Authors
// Licensed under the Apache License, Version 2.0.

package caddyhttp

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
)

type reloadAcceptTestGate struct {
	accepted       chan struct{}
	releaseNew     chan struct{}
	listenerClosed chan struct{}
	acceptedOnce   sync.Once
	releaseOnce    sync.Once
	closedOnce     sync.Once
}

var reloadAcceptTestCurrent atomic.Pointer[reloadAcceptTestGate]

// The test hook runs in net/http's Accept loop before conn.serve starts.
// Blocking here deterministically separates acceptance from first-request admission.
type reloadAcceptTestWrapper struct{ gate *reloadAcceptTestGate }

func (reloadAcceptTestWrapper) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{ID: "caddy.listeners.reload_accept_test", New: func() caddy.Module { return new(reloadAcceptTestWrapper) }}
}

func (wrapper *reloadAcceptTestWrapper) Provision(ctx caddy.Context) error {
	wrapper.gate = reloadAcceptTestCurrent.Load()
	if wrapper.gate == nil {
		return fmt.Errorf("missing reload acceptance test gate")
	}
	server, ok := ctx.Value(ServerCtxKey).(*Server)
	if !ok {
		return fmt.Errorf("reload acceptance test requires HTTP server context")
	}
	server.RegisterConnState(func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			wrapper.gate.acceptedOnce.Do(func() {
				close(wrapper.gate.accepted)
				<-wrapper.gate.releaseNew
			})
		}
	})
	return nil
}

func (wrapper *reloadAcceptTestWrapper) WrapListener(listener net.Listener) net.Listener {
	return &reloadAcceptTestListener{Listener: listener, gate: wrapper.gate}
}

type reloadAcceptTestListener struct {
	net.Listener
	gate      *reloadAcceptTestGate
	closeOnce sync.Once
	closeErr  error
}

func (listener *reloadAcceptTestListener) Close() error {
	listener.closeOnce.Do(func() {
		listener.closeErr = listener.Listener.Close()
		listener.gate.closedOnce.Do(func() { close(listener.gate.listenerClosed) })
	})
	return listener.closeErr
}

func init() { caddy.RegisterModule(reloadAcceptTestWrapper{}) }

func TestAppReloadAdmitsAcceptedFirstRequest(t *testing.T) {
	// Global Caddy state is intentionally used serially; do not call t.Parallel.
	gate := &reloadAcceptTestGate{accepted: make(chan struct{}), releaseNew: make(chan struct{}), listenerClosed: make(chan struct{})}
	reloadAcceptTestCurrent.Store(gate)
	var conn net.Conn
	t.Cleanup(func() {
		gate.releaseOnce.Do(func() { close(gate.releaseNew) })
		if conn != nil {
			_ = conn.Close()
		}
		if err := caddy.Stop(); err != nil {
			t.Error(err)
		}
		reloadAcceptTestCurrent.Store(nil)
	})
	config := []byte(`{"admin":{"disabled":true,"config":{"persist":false}},"apps":{"http":{"servers":{"accept-test":{"listen":["127.0.0.1:0"],"protocols":["h1"],"automatic_https":{"disable":true},"read_header_timeout":2000000000,"listener_wrappers":[{"wrapper":"reload_accept_test"}],"routes":[{"handle":[{"handler":"static_response","status_code":200,"body":"accepted-first-request"}]}]}}}}}`)
	if err := caddy.Load(config, true); err != nil {
		t.Fatal(err)
	}
	module, err := caddy.ActiveContext().App("http")
	if err != nil {
		t.Fatal(err)
	}
	app := module.(*App)
	server := app.Servers["accept-test"]
	if len(server.listeners) != 1 {
		t.Fatalf("listeners = %d; want one", len(server.listeners))
	}
	conn, err = net.DialTimeout("tcp", server.listeners[0].Addr().String(), 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(conn, "GET / HTTP/1.1\r\nHost: fixture\r\nConnection: close\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-gate.accepted:
	case <-time.After(3 * time.Second):
		t.Fatal("request connection was not accepted")
	}
	stopResult := make(chan error, 1)
	go func() { stopResult <- app.stop(false) }()
	// The request was sent before retirement. The accepted socket cannot begin
	// processing until the old listener is closed. This event works for both the
	// current Shutdown-first path and a future quiet-listeners-then-admit path.
	select {
	case <-gate.listenerClosed:
	case <-time.After(3 * time.Second):
		t.Fatal("reload did not retire the old listener")
	}
	gate.releaseOnce.Do(func() { close(gate.releaseNew) })
	response, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: "GET"})
	if err != nil {
		t.Fatalf("accepted first request lost during reload: %v", err)
	}
	body, err := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if err != nil || response.StatusCode != http.StatusOK || string(body) != "accepted-first-request" {
		t.Fatalf("accepted first response = %d %q, error %v", response.StatusCode, body, err)
	}
	select {
	case err := <-stopResult:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("reload stop did not finish")
	}
}

// Copyright 2026 The Caddy Authors
// Licensed under the Apache License, Version 2.0.

package caddyhttp

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
)

func TestReloadReturnsBeforeAcceptedFirstRequestAdmission(t *testing.T) {
	id, state := newShutdownTestState(t)
	state.accepted = make(chan net.Conn, 1)
	gate := make(chan struct{})
	state.newGate = gate
	releaseGate := sync.OnceFunc(func() { close(gate) })
	t.Cleanup(releaseGate)
	if err := caddy.Load(shutdownTestConfig(id, 0, false, false), true); err != nil {
		t.Fatal(err)
	}
	conn, err := net.Dial("tcp", awaitShutdownTest(t, state.addr))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	awaitShutdownTest(t, state.accepted)
	if _, err := io.WriteString(conn, "GET / HTTP/1.1\r\nHost: fixture\r\nConnection: close\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	reloaded := make(chan error, 1)
	go func() { reloaded <- caddy.Load([]byte(`{"admin":{"disabled":true,"config":{"persist":false}}}`), true) }()
	if err := awaitShutdownTest(t, reloaded); err != nil {
		t.Fatal(err)
	}
	// Reload returned while conn.serve was still prevented from starting.
	releaseGate()
	response, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: "GET"})
	if err != nil {
		t.Fatalf("first accepted request was lost: %v", err)
	}
	defer response.Body.Close()
	awaitShutdownTest(t, state.entered)
	assertShutdownTestRetained(t, state)
	finishShutdownTest(t, state)
	body, err := io.ReadAll(response.Body)
	if err != nil || string(body) != "before\nafter\n" {
		t.Fatalf("first accepted response: %q, %v", body, err)
	}
}

func TestReloadClosesUnadmittedConnections(t *testing.T) {
	for _, grace := range []time.Duration{0, 20 * time.Millisecond} {
		t.Run(grace.String(), func(t *testing.T) {
			id, state := newShutdownTestState(t)
			state.accepted = make(chan net.Conn, 1)
			var config map[string]any
			if err := json.Unmarshal(shutdownTestConfig(id, grace, false, false), &config); err != nil {
				t.Fatal(err)
			}
			server := config["apps"].(map[string]any)["http"].(map[string]any)["servers"].(map[string]any)["test"].(map[string]any)
			headerBudget := 20 * time.Millisecond
			if grace > 0 {
				headerBudget = 3 * time.Second
			}
			server["read_header_timeout"] = int64(headerBudget)
			raw, _ := json.Marshal(config)
			if err := caddy.Load(raw, true); err != nil {
				t.Fatal(err)
			}
			module, err := caddy.ActiveContext().App("http")
			if err != nil {
				t.Fatal(err)
			}
			app := module.(*App)
			conn, err := net.Dial("tcp", awaitShutdownTest(t, state.addr))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { conn.Close() })
			conn.SetDeadline(time.Now().Add(5 * time.Second))
			awaitShutdownTest(t, state.accepted)
			if err := caddy.Load([]byte(`{"admin":{"disabled":true,"config":{"persist":false}}}`), true); err != nil {
				t.Fatal(err)
			}
			var byteRead [1]byte
			if _, err := conn.Read(byteRead[:]); !errors.Is(err, io.EOF) {
				t.Fatalf("unadmitted connection was not closed: %v", err)
			}
			awaitShutdownTest(t, app.shutdownDone)
			hook := awaitShutdownTest(t, state.hook)
			if hook.err != nil || !hook.bounded {
				t.Errorf("hook context: %+v", hook)
			}
			awaitShutdownTest(t, state.cleaned)
			select {
			case <-state.entered:
				t.Fatal("headerless connection entered handler")
			default:
			}
		})
	}
}

func TestAcceptedConnectionWaitsForHandlerAdmission(t *testing.T) {
	conn, peer := net.Pipe()
	defer conn.Close()
	defer peer.Close()
	server := new(Server)
	server.trackAcceptedConnection(conn, http.StateNew)
	server.trackAcceptedConnection(conn, http.StateActive)
	if len(server.acceptedConnections.first) != 1 {
		t.Fatal("StateActive prematurely admitted a connection")
	}
	request := httptest.NewRequest("GET", "/", nil).WithContext(context.WithValue(context.Background(), acceptedConnContextKey{}, conn))
	server.admitAcceptedConnection(request)
	if len(server.acceptedConnections.first) != 0 {
		t.Fatal("handler admission did not release first request wait")
	}
	for _, state := range []http.ConnState{http.StateIdle, http.StateClosed, http.StateHijacked} {
		server.trackAcceptedConnection(conn, http.StateNew)
		server.trackAcceptedConnection(conn, state)
		if len(server.acceptedConnections.first) != 0 {
			t.Fatalf("state %s did not release first request wait", state)
		}
	}
}

type shutdownCountingListener struct {
	net.Listener
	calls atomic.Int32
}

func (ln *shutdownCountingListener) Close() error { ln.calls.Add(1); return ln.Listener.Close() }
func TestShutdownListenerClosesWrappedListenerOnce(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	counted := &shutdownCountingListener{Listener: listener}
	listenerWrapper := &shutdownListener{Listener: counted}
	var calls sync.WaitGroup
	for range 8 {
		calls.Go(func() {
			if err := listenerWrapper.Close(); err != nil {
				t.Error(err)
			}
		})
	}
	calls.Wait()
	if counted.calls.Load() != 1 {
		t.Fatalf("wrapped listener closed %d times", counted.calls.Load())
	}
}

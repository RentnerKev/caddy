// Copyright 2026 The Caddy Authors
// Licensed under the Apache License, Version 2.0.

package caddyhttp

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/http2"

	"github.com/caddyserver/caddy/v2"
)

// These tests use fixed TCP addresses and real sockets. In particular, no
// http.Client retry can conceal a connection closed by an acknowledged reload.
func TestKeepaliveReloadSameHTTP1Connection(t *testing.T) {
	addr := keepaliveReloadAddress(t)
	old := newKeepaliveReloadState(t, "old")
	keepaliveReloadLoad(t, keepaliveReloadConfig(addr, old, nil))
	native := old.server.server
	conn, reader := keepaliveReloadDial(t, addr)
	keepaliveReloadAssertBody(t, keepaliveReloadHTTP1(t, conn, reader, "/"), "old")
	next := newKeepaliveReloadState(t, "new")
	keepaliveReloadLoad(t, keepaliveReloadConfig(addr, next, nil))
	if next.server.server != native {
		t.Error("eligible reload replaced the native HTTP server")
	}
	keepaliveReloadAssertBody(t, keepaliveReloadHTTP1(t, conn, reader, "/"), "new")
}

func TestKeepaliveReloadPendingGeneration(t *testing.T) {
	for _, protocol := range []string{"h1", "h2"} {
		t.Run(protocol, func(t *testing.T) {
			addr := keepaliveReloadAddress(t)
			old := newKeepaliveReloadState(t, "old")
			protocols := []string{"h1"}
			if protocol == "h2" {
				protocols = append(protocols, "h2")
			}
			configure := func(cfg map[string]any) { keepaliveReloadServer(cfg)["protocols"] = protocols }
			keepaliveReloadLoad(t, keepaliveReloadConfig(addr, old, configure))
			conn, reader := keepaliveReloadDial(t, addr)
			var request func(string) *http.Response
			if protocol == "h2" {
				transport := &http2.Transport{AllowHTTP: true}
				client, err := transport.NewClientConn(conn)
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { client.Close() })
				request = func(path string) *http.Response {
					req, err := http.NewRequestWithContext(t.Context(), "GET", "http://"+addr+path, nil)
					if err != nil {
						t.Fatal(err)
					}
					response, err := client.RoundTrip(req)
					if err != nil {
						t.Fatal(err)
					}
					if response.ProtoMajor != 2 {
						t.Fatalf("expected HTTP/2, got %s", response.Proto)
					}
					return response
				}
			} else {
				request = func(path string) *http.Response { return keepaliveReloadHTTP1(t, conn, reader, path) }
			}
			response := request("/block")
			t.Cleanup(func() { response.Body.Close() })
			keepaliveReloadAwait(t, old.entered)
			next := newKeepaliveReloadState(t, "new")
			keepaliveReloadLoad(t, keepaliveReloadConfig(addr, next, configure))
			if protocol == "h2" {
				// A second stream on this exact ClientConn must dispatch to the new generation.
				keepaliveReloadAssertBody(t, request("/"), "new")
			} else {
				other, otherReader := keepaliveReloadDial(t, addr)
				keepaliveReloadAssertBody(t, keepaliveReloadHTTP1(t, other, otherReader, "/"), "new")
			}
			keepaliveReloadNotClosed(t, old.cleaned, "old handler cleaned while its response was active")
			old.unblock()
			keepaliveReloadAssertBody(t, response, "old-start\nold-end")
			keepaliveReloadAwait(t, old.cleaned)
			if old.usedAfterCleanup.Load() {
				t.Fatal("old request used cleaned module resources")
			}
			// HTTP/1 also remains reusable after its old response finishes.
			keepaliveReloadAssertBody(t, request("/"), "new")
		})
	}
}

func TestKeepaliveReloadFailedStartRestoresSameConnection(t *testing.T) {
	addr := keepaliveReloadAddress(t)
	old := newKeepaliveReloadState(t, "old")
	keepaliveReloadLoad(t, keepaliveReloadConfig(addr, old, nil))
	conn, reader := keepaliveReloadDial(t, addr)
	keepaliveReloadAssertBody(t, keepaliveReloadHTTP1(t, conn, reader, "/"), "old")
	failed := newKeepaliveReloadState(t, "failed")
	cfg := keepaliveReloadConfig(addr, failed, func(cfg map[string]any) {
		cfg["admin"].(map[string]any)["config"].(map[string]any)["load"] = map[string]any{"module": "keepalive_reload_failure_test", "id": failed.id}
	})
	result := make(chan error, 1)
	go func() { result <- caddy.Load(cfg, true) }()
	keepaliveReloadAwait(t, failed.loaderEntered)
	// The synchronous config loader runs after app Start, so this proves the
	// rejected generation really was published before the failure occurs.
	keepaliveReloadAssertBody(t, keepaliveReloadHTTP1(t, conn, reader, "/"), "failed")
	failed.failLoad()
	if err := keepaliveReloadAwait(t, result); err == nil {
		t.Fatal("reload unexpectedly succeeded")
	}
	keepaliveReloadAssertBody(t, keepaliveReloadHTTP1(t, conn, reader, "/"), "old")
	keepaliveReloadAwait(t, failed.cleaned)
	keepaliveReloadNotClosed(t, old.cleaned, "rollback cleaned the restored generation")
}

func TestKeepaliveReloadShutdownHookRetainsResources(t *testing.T) {
	addr := keepaliveReloadAddress(t)
	old := newKeepaliveReloadState(t, "old")
	old.hookRelease = make(chan struct{})
	keepaliveReloadLoad(t, keepaliveReloadConfig(addr, old, nil))
	conn, reader := keepaliveReloadDial(t, addr)
	keepaliveReloadAssertBody(t, keepaliveReloadHTTP1(t, conn, reader, "/"), "old")
	next := newKeepaliveReloadState(t, "new")
	keepaliveReloadLoad(t, keepaliveReloadConfig(addr, next, nil))
	keepaliveReloadAwait(t, old.hookEntered)
	keepaliveReloadNotClosed(t, old.cleaned, "module cleaned before asynchronous shutdown hook completed")
	keepaliveReloadAssertBody(t, keepaliveReloadHTTP1(t, conn, reader, "/"), "new")
	old.finishHook()
	keepaliveReloadAwait(t, old.cleaned)
	if old.hookCount.Load() != 1 {
		t.Fatalf("shutdown hook ran %d times", old.hookCount.Load())
	}
	if old.usedAfterCleanup.Load() {
		t.Fatal("shutdown hook used cleaned resources")
	}
}

func TestKeepaliveReloadTransportChangesReplaceNativeServer(t *testing.T) {
	for _, setting := range []string{"finite_grace", "read_header_timeout", "idle_timeout"} {
		t.Run(setting, func(t *testing.T) {
			addr := keepaliveReloadAddress(t)
			old := newKeepaliveReloadState(t, "old")
			keepaliveReloadLoad(t, keepaliveReloadConfig(addr, old, nil))
			native := old.server.server
			next := newKeepaliveReloadState(t, "new")
			budget := 350 * time.Millisecond
			keepaliveReloadLoad(t, keepaliveReloadConfig(addr, next, func(cfg map[string]any) {
				if setting == "finite_grace" {
					cfg["apps"].(map[string]any)["http"].(map[string]any)["grace_period"] = int64(budget)
				} else {
					keepaliveReloadServer(cfg)[setting] = int64(budget)
				}
			}))
			if next.server.server == native {
				t.Fatal("incompatible settings reused the previous native HTTP server")
			}
			if setting == "read_header_timeout" && next.server.server.ReadHeaderTimeout != budget {
				t.Fatal("new read header timeout was not applied")
			}
			if setting == "idle_timeout" && next.server.server.IdleTimeout != budget {
				t.Fatal("new idle timeout was not applied")
			}
			conn, reader := keepaliveReloadDial(t, addr)
			keepaliveReloadAssertBody(t, keepaliveReloadHTTP1(t, conn, reader, "/"), "new")
		})
	}
}

type keepaliveReloadState struct {
	id, body                          string
	server                            *Server
	entered, release, cleaned         chan struct{}
	loaderEntered, loaderRelease      chan struct{}
	hookEntered, hookRelease          chan struct{}
	releaseOnce, loaderOnce, hookOnce sync.Once
	cleanupCount, hookCount           atomic.Int32
	usedAfterCleanup                  atomic.Bool
}

func (s *keepaliveReloadState) unblock()  { s.releaseOnce.Do(func() { close(s.release) }) }
func (s *keepaliveReloadState) failLoad() { s.loaderOnce.Do(func() { close(s.loaderRelease) }) }
func (s *keepaliveReloadState) finishHook() {
	s.hookOnce.Do(func() {
		if s.hookRelease != nil {
			close(s.hookRelease)
		}
	})
}

var (
	keepaliveReloadStates sync.Map
	keepaliveReloadID     atomic.Uint64
)

func newKeepaliveReloadState(t *testing.T, body string) *keepaliveReloadState {
	t.Helper()
	state := &keepaliveReloadState{
		id: fmt.Sprintf("keepalive%d", keepaliveReloadID.Add(1)), body: body,
		entered: make(chan struct{}), release: make(chan struct{}), cleaned: make(chan struct{}),
		loaderEntered: make(chan struct{}), loaderRelease: make(chan struct{}), hookEntered: make(chan struct{}),
	}
	keepaliveReloadStates.Store(state.id, state)
	t.Cleanup(func() {
		// Release every test gate before Stop: a newer state's cleanup runs first.
		keepaliveReloadStates.Range(func(_, value any) bool {
			pending := value.(*keepaliveReloadState)
			pending.unblock()
			pending.failLoad()
			pending.finishHook()
			return true
		})
		if err := caddy.Stop(); err != nil {
			t.Error(err)
		}
		keepaliveReloadStates.Delete(state.id)
	})
	return state
}

type keepaliveReloadHandler struct {
	ID    string `json:"id"`
	state *keepaliveReloadState
}

func (keepaliveReloadHandler) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{ID: "http.handlers.keepalive_reload_test", New: func() caddy.Module { return new(keepaliveReloadHandler) }}
}

func (h *keepaliveReloadHandler) Provision(ctx caddy.Context) error {
	value, ok := keepaliveReloadStates.Load(h.ID)
	if !ok {
		return errors.New("missing keepalive reload test state")
	}
	h.state = value.(*keepaliveReloadState)
	h.state.server = ctx.Value(ServerCtxKey).(*Server)
	if h.state.hookRelease != nil {
		h.state.server.RegisterOnShutdown(func() {
			h.state.hookCount.Add(1)
			close(h.state.hookEntered)
			<-h.state.hookRelease
			if h.state.cleanupCount.Load() != 0 {
				h.state.usedAfterCleanup.Store(true)
			}
		})
	}
	return nil
}

func (h *keepaliveReloadHandler) ServeHTTP(w http.ResponseWriter, r *http.Request, _ Handler) error {
	switch r.URL.Path {
	case "/panic":
		panic("keepalive panic fixture")
	case "/panic-nil":
		panic(nil)
	case "/abort":
		panic(http.ErrAbortHandler)
	case "/hijack-panic":
		conn, _, err := http.NewResponseController(w).Hijack()
		if err != nil {
			return err
		}
		defer conn.Close()
		close(h.state.entered)
		<-h.state.release
		if h.state.cleanupCount.Load() != 0 {
			h.state.usedAfterCleanup.Store(true)
		}
		panic("keepalive hijacked panic fixture")
	}
	if r.URL.Path == "/block" {
		close(h.state.entered)
		fmt.Fprintln(w, h.state.body+"-start")
		if err := http.NewResponseController(w).Flush(); err != nil {
			return err
		}
		<-h.state.release
		if h.state.cleanupCount.Load() != 0 {
			h.state.usedAfterCleanup.Store(true)
		}
		fmt.Fprint(w, h.state.body+"-end")
	} else {
		fmt.Fprint(w, h.state.body)
	}
	return nil
}

func (h *keepaliveReloadHandler) Cleanup() error {
	if h.state != nil && h.state.cleanupCount.Add(1) == 1 {
		close(h.state.cleaned)
	}
	return nil
}

type keepaliveReloadFailureLoader struct {
	ID string `json:"id"`
}

func (keepaliveReloadFailureLoader) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{ID: "caddy.config_loaders.keepalive_reload_failure_test", New: func() caddy.Module { return new(keepaliveReloadFailureLoader) }}
}

func (l *keepaliveReloadFailureLoader) LoadConfig(caddy.Context) ([]byte, error) {
	value, ok := keepaliveReloadStates.Load(l.ID)
	if !ok {
		return nil, errors.New("missing failure loader test state")
	}
	state := value.(*keepaliveReloadState)
	close(state.loaderEntered)
	<-state.loaderRelease
	return nil, errors.New("intentional failure after HTTP app Start")
}

func init() {
	caddy.RegisterModule(keepaliveReloadHandler{})
	caddy.RegisterModule(keepaliveReloadFailureLoader{})
}

func keepaliveReloadAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}

func keepaliveReloadConfig(addr string, state *keepaliveReloadState, edit func(map[string]any)) []byte {
	cfg := map[string]any{
		"admin": map[string]any{"disabled": true, "config": map[string]any{"persist": false}},
		"apps": map[string]any{"http": map[string]any{"servers": map[string]any{"test": map[string]any{
			"listen": []string{addr}, "protocols": []string{"h1"}, "automatic_https": map[string]any{"disable": true},
			"routes": []any{map[string]any{"handle": []any{map[string]any{"handler": "keepalive_reload_test", "id": state.id}}}},
		}}}},
	}
	if edit != nil {
		edit(cfg)
	}
	raw, err := json.Marshal(cfg)
	if err != nil {
		panic(err)
	}
	return raw
}

func keepaliveReloadServer(cfg map[string]any) map[string]any {
	return cfg["apps"].(map[string]any)["http"].(map[string]any)["servers"].(map[string]any)["test"].(map[string]any)
}

func keepaliveReloadLoad(t *testing.T, config []byte) {
	t.Helper()
	if err := caddy.Load(config, true); err != nil {
		t.Fatal(err)
	}
}

func keepaliveReloadDial(t *testing.T, addr string) (net.Conn, *bufio.Reader) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	return conn, bufio.NewReader(conn)
}

func keepaliveReloadHTTP1(t *testing.T, conn net.Conn, reader *bufio.Reader, path string) *http.Response {
	t.Helper()
	if _, err := fmt.Fprintf(conn, "GET %s HTTP/1.1\r\nHost: fixture\r\n\r\n", path); err != nil {
		t.Fatalf("write on original socket: %v", err)
	}
	response, err := http.ReadResponse(reader, &http.Request{Method: http.MethodGet})
	if err != nil {
		t.Fatalf("read on original socket: %v", err)
	}
	return response
}

func keepaliveReloadAssertBody(t *testing.T, response *http.Response, want string) {
	t.Helper()
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK || string(body) != want {
		t.Fatalf("response %d %q; want 200 %q", response.StatusCode, body, want)
	}
}

func keepaliveReloadAwait[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for keepalive reload event")
		var zero T
		return zero
	}
}

func keepaliveReloadNotClosed(t *testing.T, ch <-chan struct{}, message string) {
	t.Helper()
	select {
	case <-ch:
		t.Fatal(message)
	default:
	}
}

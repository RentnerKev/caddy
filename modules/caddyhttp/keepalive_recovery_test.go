// Copyright 2026 The Caddy Authors
// Licensed under the Apache License, Version 2.0.

package caddyhttp

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/http2"

	"github.com/caddyserver/caddy/v2"
	_ "github.com/caddyserver/caddy/v2/modules/logging"
)

func TestKeepaliveRecoveryLogsPanicOnce(t *testing.T) {
	for _, protocol := range []string{"h1", "h2"} {
		for _, tc := range []struct {
			name, path, panicnil, fixture string
			logs                          int
		}{
			{name: "panic", path: "/panic", fixture: "keepalive panic fixture", logs: 1},
			{name: "abort", path: "/abort"},
			{name: "nil-panic", path: "/panic-nil", panicnil: "0", fixture: "panic called with nil argument", logs: 1},
			{name: "legacy-nil-panic", path: "/panic-nil", panicnil: "1"},
		} {
			t.Run(protocol+"/"+tc.name, func(t *testing.T) {
				path := tc.path
				if tc.panicnil != "" {
					t.Setenv("GODEBUG", os.Getenv("GODEBUG")+",panicnil="+tc.panicnil)
				}
				addr := keepaliveReloadAddress(t)
				state := newKeepaliveReloadState(t, "old")
				logfile := filepath.Join(t.TempDir(), "panic.jsonl")
				cfg := keepaliveReloadConfig(addr, state, func(cfg map[string]any) {
					keepaliveRecoveryLogging(cfg, logfile)
					if protocol == "h2" {
						keepaliveReloadServer(cfg)["protocols"] = []string{"h1", "h2"}
					}
				})
				keepaliveReloadLoad(t, cfg)
				app := keepaliveRecoveryApp(t)
				conn, _ := keepaliveReloadDial(t, addr)
				if protocol == "h1" {
					if _, err := fmt.Fprintf(conn, "GET %s HTTP/1.1\r\nHost: fixture\r\n\r\n", path); err != nil {
						t.Fatal(err)
					}
					var first [1]byte
					if n, err := conn.Read(first[:]); n != 0 || !errors.Is(err, io.EOF) {
						t.Fatalf("panic must abort HTTP/1 socket: read %d bytes, %v", n, err)
					}
				} else {
					transport := &http2.Transport{AllowHTTP: true}
					client, err := transport.NewClientConn(conn)
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() { client.Close() })
					req, err := http.NewRequestWithContext(t.Context(), "GET", "http://"+addr+path, nil)
					if err != nil {
						t.Fatal(err)
					}
					response, err := client.RoundTrip(req)
					if response != nil {
						response.Body.Close()
						t.Fatalf("panic returned HTTP response %s", response.Status)
					}
					var streamError http2.StreamError
					if !errors.As(err, &streamError) || streamError.Code != http2.ErrCodeInternal {
						t.Fatalf("panic must reset HTTP/2 stream: %v", err)
					}
					// Native stream recovery leaves the connection usable.
					req, err = http.NewRequestWithContext(t.Context(), "GET", "http://"+addr+"/", nil)
					if err != nil {
						t.Fatal(err)
					}
					response, err = client.RoundTrip(req)
					if err != nil {
						t.Fatal(err)
					}
					keepaliveReloadAssertBody(t, response, "old")
					client.Close()
				}
				if err := caddy.Stop(); err != nil {
					t.Fatal(err)
				}
				keepaliveReloadAwait(t, app.shutdownDone)
				keepaliveReloadAwait(t, state.cleaned)
				keepaliveRecoveryAssertPanicLog(t, logfile, tc.fixture, tc.logs)
			})
		}
	}
}

func TestKeepaliveRecoveryHijackedPanicAfterTransportSealed(t *testing.T) {
	addr := keepaliveReloadAddress(t)
	state := newKeepaliveReloadState(t, "old")
	logfile := filepath.Join(t.TempDir(), "hijacked.jsonl")
	keepaliveReloadLoad(t, keepaliveReloadConfig(addr, state, func(cfg map[string]any) { keepaliveRecoveryLogging(cfg, logfile) }))
	app := keepaliveRecoveryApp(t)
	group := state.server.transport
	if group == nil {
		t.Fatal("fixture did not acquire shared transport")
	}
	conn, _ := keepaliveReloadDial(t, addr)
	if _, err := io.WriteString(conn, "GET /hijack-panic HTTP/1.1\r\nHost: fixture\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	keepaliveReloadAwait(t, state.entered)
	keepaliveRecoveryRemoveConfig(t)
	keepaliveRecoveryUntil(t, func() bool {
		group.mu.Lock()
		defer group.mu.Unlock()
		return group.owner == nil
	}, "hijacked connection did not allow transport to seal")
	// ConnStateHijacked removes this socket from connection tracking, but the
	// handler's request lease must still retain its logger and module resources.
	keepaliveReloadNotClosed(t, state.cleaned, "hijacked handler cleaned before its panic")
	state.unblock()
	var first [1]byte
	if n, err := conn.Read(first[:]); n != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("hijacked socket was not closed: %d, %v", n, err)
	}
	keepaliveReloadAwait(t, app.shutdownDone)
	keepaliveReloadAwait(t, state.cleaned)
	if state.usedAfterCleanup.Load() {
		t.Fatal("hijacked handler used cleaned resources")
	}
	keepaliveRecoveryAssertPanicLog(t, logfile, "keepalive hijacked panic fixture", 1)
}

func TestKeepaliveFinalRetirementPreservesAcceptedFirstRequest(t *testing.T) {
	addr := keepaliveReloadAddress(t)
	state := newKeepaliveReloadState(t, "last")
	keepaliveReloadLoad(t, keepaliveReloadConfig(addr, state, nil))
	app := keepaliveRecoveryApp(t)
	group := state.server.transport
	if group == nil {
		t.Fatal("fixture did not acquire shared transport")
	}
	conn, reader := keepaliveReloadDial(t, addr)
	keepaliveRecoveryUntil(t, func() bool {
		group.accepted.mu.Lock()
		defer group.accepted.mu.Unlock()
		return len(group.accepted.first) == 1
	}, "socket was not recorded as accepted before its first request")
	keepaliveRecoveryRemoveConfig(t)
	loopsDone := make(chan struct{})
	go func() { group.serveLoops.Wait(); close(loopsDone) }()
	keepaliveReloadAwait(t, loopsDone)
	keepaliveReloadNotClosed(t, state.cleaned, "accepted socket lost its generation before admission")
	if _, err := io.WriteString(conn, "GET / HTTP/1.1\r\nHost: fixture\r\nConnection: close\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	response, err := http.ReadResponse(reader, &http.Request{Method: http.MethodGet})
	if err != nil {
		t.Fatalf("accepted first request was dropped after final retirement: %v", err)
	}
	keepaliveReloadAssertBody(t, response, "last")
	keepaliveReloadAwait(t, app.shutdownDone)
	keepaliveReloadAwait(t, state.cleaned)
}

func keepaliveRecoveryApp(t *testing.T) *App {
	t.Helper()
	module, err := caddy.ActiveContext().App("http")
	if err != nil {
		t.Fatal(err)
	}
	return module.(*App)
}

func keepaliveRecoveryRemoveConfig(t *testing.T) {
	t.Helper()
	keepaliveReloadLoad(t, []byte(`{"admin":{"disabled":true,"config":{"persist":false}}}`))
}

func keepaliveRecoveryLogging(cfg map[string]any, filename string) {
	cfg["logging"] = map[string]any{"logs": map[string]any{"default": map[string]any{
		"level": "ERROR", "writer": map[string]any{"output": "file", "filename": filename, "roll": false},
		"encoder": map[string]any{"format": "json"},
	}}}
}

func keepaliveRecoveryAssertPanicLog(t *testing.T, filename, fixture string, want int) {
	t.Helper()
	contents, err := os.ReadFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, line := range bytes.Split(contents, []byte{'\n'}) {
		if len(line) == 0 {
			continue
		}
		var entry struct {
			Level   string `json:"level"`
			Message string `json:"msg"`
		}
		if err := json.Unmarshal(line, &entry); err != nil {
			t.Fatalf("invalid log line: %v", err)
		}
		if !strings.HasPrefix(entry.Message, "http: panic serving ") {
			continue
		}
		count++
		if entry.Level != "error" {
			t.Errorf("panic level = %q, want error", entry.Level)
		}
		if !strings.Contains(entry.Message, fixture+"\n") || !strings.Contains(entry.Message, "goroutine ") {
			t.Errorf("panic log omitted fixture or native stack: %q", entry.Message)
		}
	}
	if count != want {
		t.Fatalf("panic log count = %d, want %d; logs: %s", count, want, contents)
	}
}

func keepaliveRecoveryUntil(t *testing.T, ready func() bool, message string) {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for !ready() {
		select {
		case <-ticker.C:
		case <-timer.C:
			t.Fatal(message)
		}
	}
}

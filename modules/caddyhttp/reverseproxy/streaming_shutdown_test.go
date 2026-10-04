package reverseproxy

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	_ "github.com/caddyserver/caddy/v2/modules/logging"
)

type tunnelShutdownState struct {
	addr     chan string
	cleaned  chan struct{}
	shutdown chan struct{}
}

var (
	tunnelShutdownStates sync.Map
	tunnelShutdownID     atomic.Uint64
)

type tunnelShutdownObserver struct {
	ID    string `json:"id"`
	state *tunnelShutdownState
}

func (tunnelShutdownObserver) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{ID: "http.handlers.tunnel_shutdown_test", New: func() caddy.Module { return new(tunnelShutdownObserver) }}
}

func (h *tunnelShutdownObserver) Provision(ctx caddy.Context) error {
	value, ok := tunnelShutdownStates.Load(h.ID)
	if !ok {
		return fmt.Errorf("missing tunnel test state")
	}
	h.state = value.(*tunnelShutdownState)
	ctx.OnCancel(func() { close(h.state.cleaned) })
	ctx.Value(caddyhttp.ServerCtxKey).(*caddyhttp.Server).RegisterOnShutdown(func() { close(h.state.shutdown) })
	return nil
}

func (h *tunnelShutdownObserver) ServeHTTP(w http.ResponseWriter, r *http.Request, next caddyhttp.Handler) error {
	return next.ServeHTTP(w, r)
}

func init() {
	caddy.RegisterModule(tunnelShutdownObserver{})
	caddy.RegisterNetwork("tunnel-shutdown-test", func(_ context.Context, _, host, _ string, _ uint, _ net.ListenConfig) (any, error) {
		value, ok := tunnelShutdownStates.Load(host)
		if !ok {
			return nil, fmt.Errorf("missing tunnel listener state")
		}
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err == nil {
			value.(*tunnelShutdownState).addr <- ln.Addr().String()
		}
		return ln, err
	})
}

func newTunnelShutdownState(t *testing.T) (string, *tunnelShutdownState) {
	t.Helper()
	id := fmt.Sprintf("tunnel%d", tunnelShutdownID.Add(1))
	state := &tunnelShutdownState{make(chan string, 1), make(chan struct{}), make(chan struct{})}
	tunnelShutdownStates.Store(id, state)
	t.Cleanup(func() {
		if err := caddy.Stop(); err != nil {
			t.Error(err)
		}
		tunnelShutdownStates.Delete(id)
	})
	return id, state
}

func tunnelShutdownConfig(id, backend string, grace, delay time.Duration, detached bool) []byte {
	proxy := map[string]any{"handler": "reverse_proxy", "upstreams": []any{map[string]any{"dial": strings.TrimPrefix(backend, "http://")}}, "stream_detached": detached, "stream_close_delay": int64(delay)}
	config := map[string]any{"admin": map[string]any{"disabled": true}, "apps": map[string]any{"http": map[string]any{"grace_period": int64(grace), "servers": map[string]any{"test": map[string]any{"listen": []string{"tunnel-shutdown-test/" + id + ":1"}, "protocols": []string{"h1"}, "automatic_https": map[string]any{"disable": true}, "routes": []any{map[string]any{"handle": []any{map[string]any{"handler": "tunnel_shutdown_test", "id": id}, proxy}}}}}}}}
	raw, _ := json.Marshal(config)
	return raw
}

func awaitTunnelShutdown[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for tunnel lifecycle")
		var zero T
		return zero
	}
}

func openShutdownTunnel(t *testing.T, addr string) (net.Conn, *bufio.Reader) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := fmt.Fprintf(conn, "GET / HTTP/1.1\r\nHost: localhost\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(conn)
	response, err := http.ReadResponse(reader, &http.Request{Method: "GET"})
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != 101 {
		t.Fatalf("upgrade status=%d", response.StatusCode)
	}
	return conn, reader
}

func TestAttachedWebsocketReloadCompletesCleanup(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, _, err := http.NewResponseController(w).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.Close()
		fmt.Fprint(conn, "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n\r\n")
		io.Copy(io.Discard, conn)
	}))
	defer backend.Close()
	for _, tc := range []struct {
		name         string
		grace, delay time.Duration
	}{{"unlimited", 0, 0}, {"finite", 20 * time.Millisecond, 0}, {"delayed", 20 * time.Millisecond, 80 * time.Millisecond}} {
		t.Run(tc.name, func(t *testing.T) {
			id, state := newTunnelShutdownState(t)
			if err := caddy.Load(tunnelShutdownConfig(id, backend.URL, tc.grace, tc.delay, false), true); err != nil {
				t.Fatal(err)
			}
			_, reader := openShutdownTunnel(t, awaitTunnelShutdown(t, state.addr))
			retired := time.Now()
			if err := caddy.Load([]byte(`{"admin":{"disabled":true}}`), true); err != nil {
				t.Fatal(err)
			}
			awaitTunnelShutdown(t, state.shutdown)
			closeFrame := make([]byte, 4)
			n, err := io.ReadFull(reader, closeFrame)
			if err != nil && err != io.EOF {
				t.Fatal(err)
			}
			if elapsed := time.Since(retired); elapsed < tc.delay {
				t.Fatalf("stream closed after %s, before stream_close_delay %s", elapsed, tc.delay)
			}
			if n != 0 && string(closeFrame) != string([]byte{0x88, 2, 3, 0xe9}) {
				t.Fatalf("unexpected WebSocket close frame: %x", closeFrame)
			}
			if _, err := reader.ReadByte(); err != io.EOF {
				t.Fatalf("stream did not close: %v", err)
			}
			awaitTunnelShutdown(t, state.cleaned)
		})
	}
}

func TestTunnelRetirementLateRegistrationAndDelay(t *testing.T) {
	ts := newTunnelTracker(caddy.Log(), time.Hour)
	registerDetachedTunnelTrackers(ts)
	t.Cleanup(func() { unregisterDetachedTunnelTrackers(ts) })
	if err := ts.stopAttachedConnections(); err != nil {
		t.Fatal(err)
	}
	timer := ts.closeTimer
	if err := ts.stopAttachedConnections(); err != nil {
		t.Fatal(err)
	}
	if ts.closeTimer != timer {
		t.Fatal("repeated retirement reset close delay")
	}
	attached := newTrackingReadWriteCloser()
	del := ts.registerConnection(attached, nil, false, "upstream")
	defer del()
	detached := newTrackingReadWriteCloser()
	deleteDetached := ts.registerConnection(detached, nil, true, "upstream")
	defer deleteDetached()
	if attached.isClosed() {
		t.Fatal("late attached connection bypassed delay")
	}
	if err := ts.closeAttachedConnections(); err != nil {
		t.Fatal(err)
	}
	if !attached.isClosed() || detached.isClosed() {
		t.Fatal("retirement did not selectively close attached connections")
	}
	late := newTrackingReadWriteCloser()
	ts.registerConnection(late, nil, false, "upstream")()
	if !late.isClosed() {
		t.Fatal("attached connection admitted after closure remains open")
	}
	if err := ts.cleanupAttachedConnections(); err != nil {
		t.Fatal(err)
	}
	if ts.closeTimer != timer {
		t.Fatal("cleanup reset retirement timer")
	}
	if detached.isClosed() {
		t.Fatal("cleanup closed detached connection with retained upstream")
	}
}

// Detached streams must survive retirement when the new configuration retains
// their upstream, then close once that upstream is actually removed.
func TestDetachedWebsocketReloadRetainsUpstream(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, _, err := http.NewResponseController(w).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.Close()
		fmt.Fprint(conn, "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n\r\n")
		// Read a masked client ping and answer with an unmasked pong. This proves
		// the detached stream remains usable after its configuration is cleaned.
		frame := make([]byte, 7)
		if _, err := io.ReadFull(conn, frame); err != nil {
			return
		}
		conn.Write([]byte{0x8a, 1, frame[6] ^ frame[2]})
		io.Copy(io.Discard, conn)
	}))
	defer backend.Close()
	id, state := newTunnelShutdownState(t)
	if err := caddy.Load(tunnelShutdownConfig(id, backend.URL, 20*time.Millisecond, 0, true), true); err != nil {
		t.Fatal(err)
	}
	conn, reader := openShutdownTunnel(t, awaitTunnelShutdown(t, state.addr))
	nextID, next := newTunnelShutdownState(t)
	if err := caddy.Load(tunnelShutdownConfig(nextID, backend.URL, 20*time.Millisecond, 0, true), true); err != nil {
		t.Fatal(err)
	}
	awaitTunnelShutdown(t, next.addr)
	awaitTunnelShutdown(t, state.cleaned)
	if _, err := conn.Write([]byte{0x89, 0x81, 1, 2, 3, 4, 'x' ^ 1}); err != nil {
		t.Fatal(err)
	}
	pong := make([]byte, 3)
	if _, err := io.ReadFull(reader, pong); err != nil {
		t.Fatal(err)
	}
	if string(pong) != string([]byte{0x8a, 1, 'x'}) {
		t.Fatalf("unexpected pong %x", pong)
	}
	if err := caddy.Load([]byte(`{"admin":{"disabled":true}}`), true); err != nil {
		t.Fatal(err)
	}
	awaitTunnelShutdown(t, next.cleaned)
	// WebSocket close control is best-effort: closing the backend side may
	// make the tunnel copier close the frontend before its control is written.
	closeFrame := make([]byte, 4)
	n, err := io.ReadFull(reader, closeFrame)
	if err != nil && err != io.EOF {
		t.Fatal(err)
	}
	if n != 0 && string(closeFrame) != string([]byte{0x88, 2, 3, 0xe9}) {
		t.Fatalf("unexpected close frame %x", closeFrame)
	}
	if _, err := reader.ReadByte(); err != io.EOF {
		t.Fatalf("removed upstream stream remained open: %v", err)
	}
}

func TestTunnelCleanupJoinsActiveClosure(t *testing.T) {
	ts := newTunnelTracker(caddy.Log(), 0)
	entered := make(chan struct{})
	release := make(chan struct{})
	conn := newTrackingReadWriteCloser()
	del := ts.registerConnection(conn, func() error { close(entered); <-release; return nil }, false, "upstream")
	defer del()
	retired := make(chan struct{})
	go func() { _ = ts.stopAttachedConnections(); close(retired) }()
	awaitTunnelShutdown(t, entered)
	cleaned := make(chan struct{})
	go func() { _ = ts.cleanupAttachedConnections(); close(cleaned) }()
	// Cleanup cannot finish while an actual closure callback is using resources.
	select {
	case <-cleaned:
		close(release)
		t.Fatal("cleanup returned before active closure completed")
	case <-time.After(20 * time.Millisecond):
	}
	close(release)
	awaitTunnelShutdown(t, retired)
	awaitTunnelShutdown(t, cleaned)
	if !conn.isClosed() {
		t.Fatal("cleanup completed without closing the connection")
	}
}

func TestTunnelEmptyCleanupRejectsLateAttachedConnection(t *testing.T) {
	ts := newTunnelTracker(caddy.Log(), time.Hour)
	if err := ts.cleanupAttachedConnections(); err != nil {
		t.Fatal(err)
	}
	conn := newTrackingReadWriteCloser()
	ts.registerConnection(conn, nil, false, "upstream")()
	if !conn.isClosed() {
		t.Fatal("empty cleanup canceled timer but allowed late attached tunnel")
	}
}

func tunnelShutdownFileLoggingConfig(id, backend, filename string) []byte {
	raw := tunnelShutdownConfig(id, backend, 20*time.Millisecond, 0, true)
	var config map[string]any
	if err := json.Unmarshal(raw, &config); err != nil {
		panic(err)
	}
	config["logging"] = map[string]any{"logs": map[string]any{"default": map[string]any{"level": "DEBUG", "writer": map[string]any{"output": "file", "filename": filename, "roll": false}}}}
	return mustMarshalTunnelConfig(config)
}

func mustMarshalTunnelConfig(config map[string]any) []byte {
	raw, err := json.Marshal(config)
	if err != nil {
		panic(err)
	}
	return raw
}

func TestDetachedWebsocketReloadUsesCurrentLogWriter(t *testing.T) {
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, _, err := http.NewResponseController(w).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.Close()
		fmt.Fprint(conn, "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n\r\n")
		io.Copy(io.Discard, conn)
	}))
	defer backend.Close()
	// Identify the retiring tracker so its final deletion provides a barrier
	// after the tunnel completion log, without polling files or sleeping.
	previous := make(map[*tunnelTracker]bool)
	detachedTunnelTrackersMu.Lock()
	for ts := range detachedTunnelTrackers {
		previous[ts] = true
	}
	detachedTunnelTrackersMu.Unlock()
	oldFile := filepath.Join(t.TempDir(), "old.log")
	newFile := filepath.Join(t.TempDir(), "new.log")
	id, state := newTunnelShutdownState(t)
	if err := caddy.Load(tunnelShutdownFileLoggingConfig(id, backend.URL, oldFile), true); err != nil {
		t.Fatal(err)
	}
	conn, _ := openShutdownTunnel(t, awaitTunnelShutdown(t, state.addr))
	var retiring *tunnelTracker
	var added int
	detachedTunnelTrackersMu.Lock()
	for ts := range detachedTunnelTrackers {
		if !previous[ts] {
			added++
			retiring = ts
		}
	}
	detachedTunnelTrackersMu.Unlock()
	if added > 1 {
		t.Fatal("multiple new detached trackers")
	}
	if retiring == nil {
		t.Fatal("missing detached tracker")
	}
	nextID, next := newTunnelShutdownState(t)
	if err := caddy.Load(tunnelShutdownFileLoggingConfig(nextID, backend.URL, newFile), true); err != nil {
		t.Fatal(err)
	}
	awaitTunnelShutdown(t, next.addr)
	awaitTunnelShutdown(t, state.cleaned)
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		detachedTunnelTrackersMu.Lock()
		_, registered := detachedTunnelTrackers[retiring]
		detachedTunnelTrackersMu.Unlock()
		if !registered {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("detached tunnel did not finish")
		}
		runtime.Gosched()
	}
	oldLog, err := os.ReadFile(oldFile)
	if err != nil {
		t.Fatal(err)
	}
	newLog, err := os.ReadFile(newFile)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(oldLog), `"msg":"connection closed"`) {
		t.Fatal("detached completion was written to retired writer")
	}
	if !strings.Contains(string(newLog), `"logger":"http.handlers.reverse_proxy.stream","msg":"connection closed"`) {
		t.Fatalf("completion missing from current writer: %s", newLog)
	}
}

type tunnelCloseErrorConn struct {
	io.ReadWriteCloser
	err error
}

func (c tunnelCloseErrorConn) Close() error { return c.err }
func TestCloseTunnelConnectionIgnoresOnlyClosedErrors(t *testing.T) {
	genuine := errors.New("genuine close failure")
	for _, tc := range []struct {
		name                  string
		graceful, close, want error
	}{
		{"already closed", net.ErrClosed, net.ErrClosed, nil},
		{"closed pipe", fmt.Errorf("wrapped: %w", io.ErrClosedPipe), io.ErrClosedPipe, nil},
		{"genuine graceful", genuine, net.ErrClosed, genuine},
		{"genuine close", io.ErrClosedPipe, genuine, genuine},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn := tunnelCloseErrorConn{newTrackingReadWriteCloser(), tc.close}
			err := closeTunnelConnection(openConnection{conn: conn, gracefulClose: func() error { return tc.graceful }})
			if !errors.Is(err, tc.want) {
				t.Fatalf("close error=%v, want %v", err, tc.want)
			}
		})
	}
}

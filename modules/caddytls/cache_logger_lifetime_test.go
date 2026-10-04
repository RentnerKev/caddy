// Copyright 2015 Matthew Holt and The Caddy Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package caddytls

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/caddyserver/certmagic"

	"github.com/caddyserver/caddy/v2"
)

var (
	cacheLifetimeWriters  sync.Map
	cacheLifetimeSequence atomic.Uint64
)

type cacheLifetimeWriter struct {
	ID string `json:"id"`
}

func (cacheLifetimeWriter) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{ID: "caddy.logging.writers.cache_lifetime_probe", New: func() caddy.Module { return new(cacheLifetimeWriter) }}
}
func (w cacheLifetimeWriter) String() string    { return w.ID }
func (w cacheLifetimeWriter) WriterKey() string { return "cache-lifetime:" + w.ID }
func (w cacheLifetimeWriter) OpenWriter() (io.WriteCloser, error) {
	value, ok := cacheLifetimeWriters.Load(w.ID)
	if !ok {
		return nil, fmt.Errorf("unknown writer %s", w.ID)
	}
	return value.(*cacheLifetimeDestination), nil
}

type cacheLifetimeDestination struct {
	mu                 sync.Mutex
	closed             bool
	closeCount         int
	entries            []string
	afterClose         []string
	maintenanceStarted chan struct{}
	startedOnce        sync.Once
}

func (w *cacheLifetimeDestination) Write(data []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	entry := string(data)
	w.entries = append(w.entries, entry)
	if strings.Contains(entry, `"msg":"started background certificate maintenance"`) {
		w.startedOnce.Do(func() { close(w.maintenanceStarted) })
	}
	if w.closed {
		w.afterClose = append(w.afterClose, entry)
		return 0, os.ErrClosed
	}
	return len(data), nil
}

func (w *cacheLifetimeDestination) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closed = true
	w.closeCount++
	return nil
}

func (w *cacheLifetimeDestination) snapshot() (int, []string, []string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.closeCount, append([]string(nil), w.entries...), append([]string(nil), w.afterClose...)
}

func init() { caddy.RegisterModule(cacheLifetimeWriter{}) }

func cacheLifetimeConfig(id string) []byte {
	config := map[string]any{
		"admin": map[string]any{"disabled": true, "config": map[string]any{"persist": false}},
		"logging": map[string]any{"logs": map[string]any{"default": map[string]any{
			"level": "DEBUG", "writer": map[string]any{"output": "cache_lifetime_probe", "id": id},
		}}},
		"apps": map[string]any{"tls": map[string]any{"disable_storage_clean": true}},
	}
	raw, err := json.Marshal(config)
	if err != nil {
		panic(err)
	}
	return raw
}

func newCacheLifetimeDestination(t *testing.T) (string, *cacheLifetimeDestination) {
	t.Helper()
	id := fmt.Sprintf("probe-%d", cacheLifetimeSequence.Add(1))
	writer := &cacheLifetimeDestination{maintenanceStarted: make(chan struct{})}
	cacheLifetimeWriters.Store(id, writer)
	t.Cleanup(func() { cacheLifetimeWriters.Delete(id) })
	return id, writer
}

func awaitCacheMaintenance(t *testing.T, writer *cacheLifetimeDestination) {
	t.Helper()
	select {
	case <-writer.maintenanceStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("initial cache maintenance did not start")
	}
}

func checkCacheLifetimeDestination(t *testing.T, writer *cacheLifetimeDestination, wantCloses int) {
	t.Helper()
	closes, _, afterClose := writer.snapshot()
	if closes != wantCloses || len(afterClose) != 0 {
		t.Fatalf("writer closes=%d, want=%d; writes after Close=%q", closes, wantCloses, afterClose)
	}
}

// No listener, HTTP server, request, or WebSocket is needed. The cache keeps
// its original logger across reloads, so only its original writers need a lease.
func TestCacheLoggerLifetimeAcrossReload(t *testing.T) {
	for _, tc := range []struct {
		name    string
		reloads int
		shared  bool
	}{
		{"one_reload", 1, false}, {"repeated_reload", 3, false}, {"shared_writer", 3, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id, old := newCacheLifetimeDestination(t)
			t.Cleanup(func() {
				if err := caddy.Stop(); err != nil {
					t.Error(err)
				}
			})
			if err := caddy.Load(cacheLifetimeConfig(id), true); err != nil {
				t.Fatal(err)
			}
			awaitCacheMaintenance(t, old)
			previous := old
			for range tc.reloads {
				nextID, next := id, old
				if !tc.shared {
					nextID, next = newCacheLifetimeDestination(t)
				}
				if err := caddy.Load(cacheLifetimeConfig(nextID), true); err != nil {
					t.Fatal(err)
				}
				checkCacheLifetimeDestination(t, old, 0)
				if previous != old {
					checkCacheLifetimeDestination(t, previous, 1)
				}
				previous = next
			}
			if err := caddy.Stop(); err != nil {
				t.Fatal(err)
			}
			checkCacheLifetimeDestination(t, old, 1)
			checkCacheLifetimeDestination(t, previous, 1)
			_, entries, _ := old.snapshot()
			stops := 0
			for _, entry := range entries {
				if strings.Contains(entry, `"msg":"stopped background certificate maintenance"`) {
					stops++
				}
			}
			if stops != 1 {
				t.Fatalf("maintenance stop events=%d, want exactly one", stops)
			}
			certCacheMu.RLock()
			cacheGone := certCache == nil && certCacheLifecycle == nil
			certCacheMu.RUnlock()
			if !cacheGone {
				t.Fatal("cache or logwriter ownership leaked after Stop")
			}
		})
	}
}

func TestCacheLoggerLifetimeValidation(t *testing.T) {
	for _, tc := range []struct {
		name      string
		tls       map[string]any
		wantError bool
	}{
		{"valid", map[string]any{"disable_storage_clean": true}, false},
		{"failed_validation", map[string]any{"disable_storage_clean": true, "cache": map[string]any{"capacity": -1}}, true},
		{"failed_provision", map[string]any{"disable_storage_clean": true, "certificates": map[string]any{"unknown_cache_lifetime_test_loader": map[string]any{}}}, true},
		{"before_cache_creation", map[string]any{"dns": map[string]any{"name": "unknown_cache_lifetime_test_dns"}}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id, writer := newCacheLifetimeDestination(t)
			var raw map[string]any
			if err := json.Unmarshal(cacheLifetimeConfig(id), &raw); err != nil {
				t.Fatal(err)
			}
			raw["apps"] = map[string]any{"tls": tc.tls}
			encoded, err := json.Marshal(raw)
			if err != nil {
				t.Fatal(err)
			}
			var cfg caddy.Config
			if err := json.Unmarshal(encoded, &cfg); err != nil {
				t.Fatal(err)
			}
			err = caddy.Validate(&cfg)
			if (err != nil) != tc.wantError {
				t.Fatalf("validation error=%v, wantError=%v", err, tc.wantError)
			}
			checkCacheLifetimeDestination(t, writer, 1)
			certCacheMu.RLock()
			cacheGone := certCache == nil && certCacheLifecycle == nil
			certCacheMu.RUnlock()
			if !cacheGone {
				t.Fatal("validation retained cache or logwriter ownership")
			}
		})
	}
}

func TestCacheLoggerLifetimeValidationKeepsActiveCache(t *testing.T) {
	id, active := newCacheLifetimeDestination(t)
	t.Cleanup(func() {
		if err := caddy.Stop(); err != nil {
			t.Error(err)
		}
	})
	if err := caddy.Load(cacheLifetimeConfig(id), true); err != nil {
		t.Fatal(err)
	}
	awaitCacheMaintenance(t, active)
	validationID, validation := newCacheLifetimeDestination(t)
	var cfg caddy.Config
	if err := json.Unmarshal(cacheLifetimeConfig(validationID), &cfg); err != nil {
		t.Fatal(err)
	}
	if err := caddy.Validate(&cfg); err != nil {
		t.Fatal(err)
	}
	checkCacheLifetimeDestination(t, validation, 1)
	checkCacheLifetimeDestination(t, active, 0)
	certCacheMu.RLock()
	generation := certCacheLifecycle
	certCacheMu.RUnlock()
	if generation == nil {
		t.Fatal("validation stopped the active cache")
	}
	// Exercise the same getter CertMagic maintenance uses, then emit through
	// the returned automation config's logger after validation's writer closed.
	magic, err := generation.getConfig(certmagic.Certificate{Names: []string{"cache-lifetime.example"}})
	if err != nil {
		t.Fatal(err)
	}
	magic.Logger.Info("active cache getter after validation")
	_, entries, _ := active.snapshot()
	if !strings.Contains(strings.Join(entries, ""), "active cache getter after validation") {
		t.Fatal("cache getter did not return the active configuration's logger")
	}
	checkCacheLifetimeDestination(t, validation, 1)
	if err := caddy.Stop(); err != nil {
		t.Fatal(err)
	}
	checkCacheLifetimeDestination(t, active, 1)
}

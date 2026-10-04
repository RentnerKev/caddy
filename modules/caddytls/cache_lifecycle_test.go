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
	"context"
	"errors"
	"testing"

	"github.com/caddyserver/certmagic"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"

	"github.com/caddyserver/caddy/v2"
)

func cacheLifecycleTestOwner(t *testing.T, logger *zap.Logger) (*certificateCacheOwner, *certmagic.Config) {
	t.Helper()
	ctx, cancel := caddy.NewContext(caddy.Context{Context: context.Background()})
	t.Cleanup(cancel)
	magic := &certmagic.Config{Logger: logger}
	tlsApp := &TLS{ctx: ctx}
	owner, err := acquireCertificateCache(tlsApp, certmagic.CacheOptions{
		Logger:           logger,
		GetConfigForCert: func(certmagic.Certificate) (*certmagic.Config, error) { return magic, nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(owner.release)
	return owner, magic
}

func TestCacheLifecycleOwnerSurvivesRetiringSnapshot(t *testing.T) {
	core, entries := observer.New(zapcore.DebugLevel)
	first, _ := cacheLifecycleTestOwner(t, zap.New(core))
	first.start()
	generation := first.generation
	// This is A's stale no-TLS cleanup decision. C acquires that same cache
	// before A releases it, so the decision cannot authorize stopping C's cache.
	if _, err := caddy.ActiveContext().AppIfConfigured("tls"); err == nil {
		t.Fatal("test requires the active configuration to have no TLS app")
	}
	current, expected := cacheLifecycleTestOwner(t, zap.NewNop())
	current.start()
	if current.generation != generation {
		t.Fatal("cache was not shared")
	}
	first.release()
	first.release()
	certCacheMu.RLock()
	stillCurrent := certCacheLifecycle == generation && certCache == generation.cache
	certCacheMu.RUnlock()
	if !stillCurrent {
		t.Fatal("retiring owner detached the current cache")
	}
	actual, err := generation.getConfig(certmagic.Certificate{})
	if err != nil || actual != expected {
		t.Fatalf("current getter=%p err=%v, want %p", actual, err, expected)
	}
	if count := entries.FilterMessage("stopped background certificate maintenance").Len(); count != 0 {
		t.Fatalf("retiring owner stopped shared cache %d times", count)
	}
	current.release()
	current.release()
	if count := entries.FilterMessage("stopped background certificate maintenance").Len(); count != 1 {
		t.Fatalf("final release stop events=%d, want 1", count)
	}
	// An idempotent release of G must never detach a newly created generation H.
	next, _ := cacheLifecycleTestOwner(t, zap.NewNop())
	next.start()
	first.release()
	certCacheMu.RLock()
	stillNext := certCacheLifecycle == next.generation && certCache == next.generation.cache
	certCacheMu.RUnlock()
	if !stillNext || next.generation == generation {
		t.Fatal("stale release affected another generation")
	}
}

func TestCacheLifecycleGetterRestoresStartedOwner(t *testing.T) {
	first, original := cacheLifecycleTestOwner(t, zap.NewNop())
	first.start()
	validation, invalid := cacheLifecycleTestOwner(t, zap.NewNop())
	actual, err := first.generation.getConfig(certmagic.Certificate{})
	if err != nil || actual != original || actual == invalid {
		t.Fatalf("unstarted owner replaced live getter: cfg=%p err=%v", actual, err)
	}
	validation.release()
	next, replacement := cacheLifecycleTestOwner(t, zap.NewNop())
	next.start()
	actual, err = first.generation.getConfig(certmagic.Certificate{})
	if err != nil || actual != replacement {
		t.Fatal("Start did not publish replacement getter")
	}
	// Simulate a failed Start: its cleanup restores the still-owned predecessor.
	next.release()
	actual, err = first.generation.getConfig(certmagic.Certificate{})
	if err != nil || actual != original {
		t.Fatal("failed replacement did not restore predecessor")
	}
}

func TestCacheLifecycleReManageHonorsCanceledContext(t *testing.T) {
	// No policies or issuer are initialized: observing the canceled context
	// must return before attempting to access them or starting issuance.
	tlsApp := new(TLS)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := tlsApp.manage(ctx, map[string]struct{}{"cache-lifetime.example": {}}, false); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled synchronous management error=%v", err)
	}
}

func TestCacheLifecycleStopRestoresGetterBeforeHeldCleanup(t *testing.T) {
	for _, failTLSStart := range []bool{false, true} {
		name := "later_app_start_failed"
		if failTLSStart {
			name = "tls_start_failed"
		}
		t.Run(name, func(t *testing.T) {
			first, original := cacheLifecycleTestOwner(t, zap.NewNop())
			first.start()
			next, replacement := cacheLifecycleTestOwner(t, zap.NewNop())
			ctx, cancel := caddy.NewContext(caddy.Context{Context: context.Background()})
			defer cancel()
			release := ctx.HoldCleanup()
			defer release()
			cleaned := make(chan struct{})
			ctx.OnCancel(func() { close(cleaned) })
			app := &TLS{ctx: ctx, cacheOwner: next, Automation: &AutomationConfig{}}
			if failTLSStart {
				cancel()
				if err := app.Start(); err == nil {
					t.Fatal("Start succeeded with a canceled context")
				}
			} else {
				next.start()
				actual, err := first.generation.getConfig(certmagic.Certificate{})
				if err != nil || actual != replacement {
					t.Fatal("replacement did not become selected")
				}
			}
			if err := app.Stop(); err != nil {
				t.Fatal(err)
			}
			cancel()
			select {
			case <-cleaned:
				t.Fatal("test configuration was cleaned despite its HTTP resource hold")
			default:
			}
			actual, err := first.generation.getConfig(certmagic.Certificate{})
			if err != nil || actual != original {
				t.Fatal("failed configuration remained selected until deferred cleanup")
			}
			certCacheMu.RLock()
			retained := !next.released && certCacheLifecycle == first.generation
			certCacheMu.RUnlock()
			if !retained {
				t.Fatal("Stop released generation resources before Cleanup")
			}
			release()
			select {
			case <-cleaned:
			default:
				t.Fatal("resource hold did not release cleanup")
			}
		})
	}
}

func TestCacheLifecycleRetiringStopKeepsReplacement(t *testing.T) {
	first, _ := cacheLifecycleTestOwner(t, zap.NewNop())
	first.start()
	next, replacement := cacheLifecycleTestOwner(t, zap.NewNop())
	next.start()
	if err := (&TLS{cacheOwner: first}).Stop(); err != nil {
		t.Fatal(err)
	}
	actual, err := first.generation.getConfig(certmagic.Certificate{})
	if err != nil || actual != replacement {
		t.Fatal("stopping predecessor withdrew the running replacement")
	}
}

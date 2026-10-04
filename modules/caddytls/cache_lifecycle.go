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
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/caddyserver/certmagic"

	"github.com/caddyserver/caddy/v2"
)

// Each TLS app owns its particular cache generation, including during
// provisioning. Retiring an older app cannot stop a cache acquired by a newer
// app, even when the active configuration changes during asynchronous cleanup.
// certCacheMu protects owners and selection; reconcileMu serializes changes to
// the cache's configuration with certificate reconciliation, without holding
// certCacheMu while performing certificate operations.
type certificateCacheGeneration struct {
	cache          *certmagic.Cache
	owners         []*certificateCacheOwner
	selected       *certificateCacheOwner
	releaseLogging func()
	reconcileMu    sync.Mutex
}

type certificateCacheOwner struct {
	generation *certificateCacheGeneration
	options    certmagic.CacheOptions
	started    bool
	cleaning   bool
	released   bool
}

var certCacheLifecycle *certificateCacheGeneration

func acquireCertificateCache(t *TLS, options certmagic.CacheOptions) (*certificateCacheOwner, error) {
	for {
		certCacheMu.Lock()
		generation := certCacheLifecycle
		if generation == nil {
			release, err := t.ctx.HoldLogging()
			if err != nil {
				certCacheMu.Unlock()
				return nil, err
			}
			generation = &certificateCacheGeneration{releaseLogging: release}
			owner := &certificateCacheOwner{generation: generation, options: options}
			generation.owners = append(generation.owners, owner)
			generation.selected = owner
			options.GetConfigForCert = generation.getConfig
			generation.cache = certmagic.NewCache(options)
			certCacheLifecycle = generation
			certCache = generation.cache
			certCacheMu.Unlock()
			return owner, nil
		}
		certCacheMu.Unlock()

		generation.reconcileMu.Lock()
		certCacheMu.Lock()
		if certCacheLifecycle != generation {
			certCacheMu.Unlock()
			generation.reconcileMu.Unlock()
			continue
		}
		owner := &certificateCacheOwner{generation: generation, options: options}
		generation.owners = append(generation.owners, owner)
		// Provisioning and validation must not replace a running cache's getter.
		// Start publishes the fully provisioned automation configuration.
		certCacheMu.Unlock()
		generation.reconcileMu.Unlock()
		return owner, nil
	}
}

func (generation *certificateCacheGeneration) getConfig(cert certmagic.Certificate) (*certmagic.Config, error) {
	certCacheMu.RLock()
	selected := generation.selected
	var getter certmagic.ConfigGetter
	if selected != nil && selected.started && !selected.cleaning {
		getter = selected.options.GetConfigForCert
	}
	certCacheMu.RUnlock()
	if getter == nil {
		return nil, fmt.Errorf("certificate cache has no configuration owner")
	}
	return getter(cert)
}

func (generation *certificateCacheGeneration) selectLocked(owner *certificateCacheOwner) {
	generation.selected = owner
	options := owner.options
	options.GetConfigForCert = generation.getConfig
	generation.cache.SetOptions(options)
}

func (owner *certificateCacheOwner) start() {
	generation := owner.generation
	generation.reconcileMu.Lock()
	defer generation.reconcileMu.Unlock()
	certCacheMu.Lock()
	defer certCacheMu.Unlock()
	if !owner.released {
		owner.started = true
		generation.selectLocked(owner)
	}
}

// stop withdraws a running configuration immediately, independently of its
// deferred cleanup. A failed replacement can retain admitted HTTP handlers, but
// its certificate configuration must no longer displace the running predecessor.
func (owner *certificateCacheOwner) stop() {
	generation := owner.generation
	generation.reconcileMu.Lock()
	defer generation.reconcileMu.Unlock()
	certCacheMu.Lock()
	defer certCacheMu.Unlock()
	owner.started = false
	if generation.selected != owner || owner.released {
		return
	}
	generation.selected = nil
	for _, candidate := range generation.owners {
		if candidate.started && !candidate.cleaning && !candidate.released {
			generation.selectLocked(candidate)
		}
	}
}

func (owner *certificateCacheOwner) release() {
	generation := owner.generation
	generation.reconcileMu.Lock()
	certCacheMu.Lock()
	if owner.released {
		certCacheMu.Unlock()
		generation.reconcileMu.Unlock()
		return
	}
	owner.released = true
	for i, candidate := range generation.owners {
		if candidate == owner {
			generation.owners = slices.Delete(generation.owners, i, i+1)
			break
		}
	}
	last := len(generation.owners) == 0
	if last {
		generation.selected = nil
		if certCacheLifecycle == generation {
			certCacheLifecycle = nil
			certCache = nil
		}
	} else if generation.selected == owner {
		// Prefer the newest started owner. An unstarted validation owner is
		// only a fallback when no started owner remains; its getter stays unavailable.
		next := generation.owners[0]
		for _, candidate := range generation.owners {
			if candidate.started {
				next = candidate
			}
		}
		generation.selectLocked(next)
	}
	certCacheMu.Unlock()
	generation.reconcileMu.Unlock()
	if last {
		generation.cache.Stop()
		generation.releaseLogging()
	}
}

func (t *TLS) reconcileCachedCertificates() error {
	owner := t.cacheOwner
	generation := owner.generation
	var releaseNext func()
	generation.reconcileMu.Lock()
	defer func() {
		generation.reconcileMu.Unlock()
		if releaseNext != nil {
			releaseNext()
		}
	}()

	nextContext := caddy.ActiveContext()
	nextApp, err := nextContext.AppIfConfigured("tls")
	if err != nil || nextApp == nil || nextApp == t {
		return nil
	}
	next := nextApp.(*TLS)
	if next.cacheOwner == nil || next.cacheOwner.generation != generation {
		return nil
	}
	// The active snapshot can retire immediately. Keep its module resources
	// alive through synchronous reconciliation, and skip an already-retired one.
	releaseNext = next.ctx.HoldCleanup()
	if next.ctx.Err() != nil {
		return nil
	}
	certCacheMu.RLock()
	selected := generation.selected == next.cacheOwner && !next.cacheOwner.released
	certCacheMu.RUnlock()
	if !selected {
		return nil
	}

	var removed []certmagic.SubjectIssuer
	var unloaded []string
	reManage := make(map[string]struct{})
	for subject, issuer := range t.managing {
		if nextIssuer, ok := next.managing[subject]; !ok || nextIssuer != issuer {
			removed = append(removed, certmagic.SubjectIssuer{Subject: subject, IssuerKey: issuer})
			if ok {
				reManage[subject] = struct{}{}
			}
		}
	}
	for hash := range t.loaded {
		if _, ok := next.loaded[hash]; !ok {
			unloaded = append(unloaded, hash)
		}
	}
	generation.cache.RemoveManaged(removed)
	generation.cache.Remove(unloaded)
	// Never launch work using another configuration after releasing its hold.
	// A canceled replacement stops this work; the finite budget bounds cleanup
	// if certificate issuance cannot finish before the next configuration reload.
	ctx, cancel := context.WithTimeout(next.ctx.Context, 30*time.Second)
	defer cancel()
	if err := next.manage(ctx, reManage, false); err != nil {
		return fmt.Errorf("re-managing unloaded certificates with new config: %w", err)
	}
	return nil
}

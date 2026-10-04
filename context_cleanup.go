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

package caddy

import (
	"context"
	"log"
	"slices"
	"sync"
	"sync/atomic"
)

// contextCleanup is shared by all value copies of a Context. It separates
// cancellation from resource cleanup, which may need to wait for retiring apps.
// A hold is also used while loading a module so parent cancellation cannot race
// its provisioning or its registration in modules.
type contextCleanup struct {
	mu               sync.Mutex
	ctx              context.Context
	modules          map[string][]Module
	moduleOrder      []cleanupModule
	callbacks        []func()
	callbacksStarted bool
	holds            int
	retired          bool
	cleaning         bool
	done             chan struct{}
	stopParent       func() bool
}

// Parents finish loading after their guests, so reverse registration order
// lets a draining app finish using its guests before they are cleaned up.
type cleanupModule struct {
	name     string
	instance Module
}

func newContextCleanup(ctx context.Context, modules map[string][]Module) *contextCleanup {
	return &contextCleanup{ctx: ctx, modules: modules, done: make(chan struct{})}
}

func (cc *contextCleanup) watchParent(stop func() bool) {
	cc.mu.Lock()
	if cc.retired {
		cc.mu.Unlock()
		stop()
		return
	}
	cc.stopParent = stop
	cc.mu.Unlock()
}

func (cc *contextCleanup) acquire() (func(), bool) {
	cc.mu.Lock()
	if cc.retired {
		cc.mu.Unlock()
		return nil, false
	}
	cc.holds++
	if cc.holds == 1 {
		// Track holders before retirement: parent cancellation closes Done
		// synchronously, but its AfterFunc retirement may not have run yet.
		cc.trackLocked()
	}
	cc.mu.Unlock()

	var released atomic.Bool
	return func() {
		if !released.CompareAndSwap(false, true) {
			return
		}
		cc.mu.Lock()
		cc.holds--
		var stopParent func() bool
		if cc.ctx.Err() != nil {
			stopParent = cc.retireLocked()
		}
		start := cc.startCleanupLocked()
		if cc.holds == 0 && !cc.retired && len(cc.modules) == 0 && len(cc.callbacks) == 0 {
			cc.untrackLocked()
		}
		cc.mu.Unlock()
		if stopParent != nil {
			stopParent()
		}
		if start {
			cc.cleanUp()
		}
	}, true
}

func (cc *contextCleanup) onCancel(f func()) {
	cc.mu.Lock()
	if cc.callbacksStarted {
		cc.mu.Unlock()
		f()
		return
	}
	cc.callbacks = append(cc.callbacks, f)
	// Resource-owning contexts remain visible even without a hold: parent
	// cancellation can close Done before the retirement watcher is scheduled.
	cc.trackLocked()
	cc.mu.Unlock()
}

func (cc *contextCleanup) retire() {
	cc.retireWithCleanup(false)
}

func (cc *contextCleanup) retireWithCleanup(async bool) {
	cc.mu.Lock()
	stopParent := cc.retireLocked()
	start := cc.startCleanupLocked()
	cc.mu.Unlock()
	if stopParent != nil {
		stopParent()
	}
	if start {
		if async {
			go cc.cleanUp()
		} else {
			cc.cleanUp()
		}
	}
}

func (cc *contextCleanup) retireLocked() func() bool {
	if cc.retired {
		return nil
	}
	cc.retired = true
	stopParent := cc.stopParent
	cc.stopParent = nil
	cc.trackLocked()
	return stopParent
}

// Registry mutations follow the state lock so the final live release or actual
// cleanup completion cannot race insertion and leave a retained lifecycle.
func (cc *contextCleanup) trackLocked() {
	pendingCleanupsMu.Lock()
	pendingCleanups[cc] = struct{}{}
	pendingCleanupsMu.Unlock()
}

func (cc *contextCleanup) untrackLocked() {
	pendingCleanupsMu.Lock()
	delete(pendingCleanups, cc)
	pendingCleanupsMu.Unlock()
}

// startCleanupLocked reserves cleanup exactly once. The caller must perform
// the actual work after unlocking, because module callbacks can reenter Caddy.
func (cc *contextCleanup) startCleanupLocked() bool {
	if !cc.retired || cc.holds != 0 || cc.cleaning {
		return false
	}
	cc.cleaning = true
	return true
}

func (cc *contextCleanup) cleanUp() {
	cc.mu.Lock()
	modules := cc.modules
	order := cc.moduleOrder
	cc.moduleOrder = nil
	cc.mu.Unlock()

	defer func() {
		cc.mu.Lock()
		close(cc.done)
		cc.untrackLocked()
		cc.mu.Unlock()
	}()

	clean := func(modName string, inst Module) {
		if cu, ok := inst.(CleanerUpper); ok {
			if err := cu.Cleanup(); err != nil {
				log.Printf("[ERROR] %s (%p): cleanup: %v", modName, inst, err)
			}
		}
	}
	if len(order) > 0 {
		for _, module := range slices.Backward(order) {
			clean(module.name, module.instance)
		}
	} else {
		// Internal callers and tests may construct a Context's module map
		// directly, without going through successful module registration.
		for modName, modInstances := range modules {
			for _, inst := range modInstances {
				clean(modName, inst)
			}
		}
	}

	// Resource callbacks run last so modules can still use their log writers
	// and other provisioned resources while cleaning up. Registrations made
	// during module cleanup join this phase rather than running prematurely.
	cc.mu.Lock()
	callbacks := cc.callbacks
	cc.callbacks = nil
	cc.callbacksStarted = true
	cc.mu.Unlock()
	for _, f := range callbacks {
		f()
	}
}

// waitForCleanup waits for canceled resource-owning contexts and ongoing cleanup, including
// callbacks and module cleanup. It also retires canceled contexts whose parent
// cancellation watcher has not run yet. Still-live holders are ignored.
// The caller supplies the process-exit deadline; a canceled module context must
// not be used for this wait.
func waitForCleanup(ctx context.Context) error {
	for {
		pendingCleanupsMu.Lock()
		pending := make([]*contextCleanup, 0, len(pendingCleanups))
		for cc := range pendingCleanups {
			pending = append(pending, cc)
		}
		pendingCleanupsMu.Unlock()
		waited := false
		for _, cc := range pending {
			if cc.ctx.Err() != nil {
				// Mark retirement synchronously, but never run callbacks here:
				// a blocked cleanup must not bypass the caller's finite deadline.
				cc.retireWithCleanup(true)
			}
			cc.mu.Lock()
			retired := cc.retired
			cc.mu.Unlock()
			if !retired {
				continue
			}
			waited = true
			select {
			case <-cc.done:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if !waited {
			return nil
		}
	}
}

var (
	pendingCleanupsMu sync.Mutex
	pendingCleanups   = make(map[*contextCleanup]struct{})
)

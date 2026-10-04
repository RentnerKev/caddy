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
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestContextCleanupHolds(t *testing.T) {
	ctx, cancel := NewContextWithCause(Context{Context: context.Background()})
	t.Cleanup(func() { cancel(nil) })
	var callbacks, modules atomic.Int32
	ctx.OnCancel(func() { callbacks.Add(1) })
	ctx.moduleInstances["test"] = []Module{&cleanupTestModule{cleanup: func() error {
		modules.Add(1)
		return nil
	}}}

	release1, release2 := ctx.HoldCleanup(), ctx.HoldCleanup()
	t.Cleanup(release1)
	t.Cleanup(release2)
	cause := errors.New("retiring old configuration")
	cancel(cause)
	lateRelease := ctx.HoldCleanup()
	defer lateRelease()
	select {
	case <-ctx.Done():
	default:
		t.Fatal("cleanup holds must not delay cancellation")
	}
	if got := context.Cause(ctx); got != cause {
		t.Fatalf("cause = %v; want %v", got, cause)
	}
	release1()
	release1()
	if callbacks.Load() != 0 || modules.Load() != 0 {
		t.Fatal("resources were cleaned before the final release")
	}
	release2()
	release2()
	cancel(errors.New("later cause"))
	if callbacks.Load() != 1 || modules.Load() != 1 {
		t.Fatalf("cleanup counts: callbacks=%d modules=%d; want 1 each", callbacks.Load(), modules.Load())
	}
	if context.Cause(ctx) != cause {
		t.Fatal("later cancellation replaced the original cause")
	}
}

func TestContextCleanupCopiesAndReentrantCallbacks(t *testing.T) {
	ctx, cancel := NewContext(Context{Context: context.Background()})
	t.Cleanup(cancel)
	type valueKey struct{}
	copy := ctx
	derived := ctx.WithValue(valueKey{}, "value")
	var callbacks atomic.Int32
	copy.OnCancel(func() { callbacks.Add(1) })
	derived.OnCancel(func() { callbacks.Add(1) })
	release := derived.HoldCleanup()
	t.Cleanup(release)
	ctx.OnCancel(func() {
		// A release or cancel called from cleanup must not deadlock.
		release()
		cancel()
		ctx.HoldCleanup()()
		ctx.OnCancel(func() { callbacks.Add(1) })
	})
	cancel()
	if callbacks.Load() != 0 {
		t.Fatal("a copied context lost its cleanup hold")
	}
	release()
	if callbacks.Load() != 3 {
		t.Fatalf("callbacks = %d; want 3", callbacks.Load())
	}
	ctx.OnCancel(func() { callbacks.Add(1) })
	if callbacks.Load() != 4 {
		t.Fatal("callback registered after cleanup must run immediately")
	}
}

func TestContextCleanupConcurrentCancelAndRelease(t *testing.T) {
	ctx, cancel := NewContextWithCause(Context{Context: context.Background()})
	var callbacks atomic.Int32
	ctx.OnCancel(func() { callbacks.Add(1) })
	const holders = 16
	releases := make([]func(), holders)
	for i := range releases {
		releases[i] = ctx.HoldCleanup()
		t.Cleanup(releases[i])
	}
	cause := errors.New("first cause")
	cancel(cause)
	var wg sync.WaitGroup
	for _, release := range releases {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 20 {
				cancel(errors.New("subsequent cause"))
				release()
			}
		}()
	}
	wg.Wait()
	if callbacks.Load() != 1 {
		t.Fatalf("callbacks = %d; want 1", callbacks.Load())
	}
	if context.Cause(ctx) != cause {
		t.Fatal("concurrent cancellation changed the first cause")
	}
}

func TestContextCleanupLateHold(t *testing.T) {
	ctx, cancel := NewContext(Context{Context: context.Background()})
	var callbacks atomic.Int32
	ctx.OnCancel(func() { callbacks.Add(1) })
	cancel()
	ctx.HoldCleanup()()
	if callbacks.Load() != 1 {
		t.Fatal("late hold changed completed cleanup")
	}
	if _, err := ctx.LoadModuleByID("unknown", nil); err == nil {
		t.Fatal("a retired context accepted a module load")
	}
}

func TestContextCleanupParentCancellation(t *testing.T) {
	parent, cancelParent := context.WithCancelCause(context.Background())
	ctx, cancel := NewContextWithCause(Context{Context: parent})
	t.Cleanup(func() { cancel(nil) })
	var callbacks atomic.Int32
	cleaned := make(chan struct{})
	ctx.OnCancel(func() {
		callbacks.Add(1)
		close(cleaned)
	})
	release := ctx.HoldCleanup()
	t.Cleanup(release)
	cause := errors.New("parent cause")
	cancelParent(cause)
	if ctx.Err() != context.Canceled || context.Cause(ctx) != cause {
		t.Fatal("child did not preserve standard parent cancellation")
	}
	if callbacks.Load() != 0 {
		t.Fatal("parent cancellation bypassed the child hold")
	}
	release()
	// Parent-triggered retirement can run before or after the release. Both
	// orderings must clean up without explicitly canceling the child.
	waitCleanupTestChannel(t, cleaned)
	if callbacks.Load() != 1 {
		t.Fatal("parent cancellation did not request child cleanup")
	}
	cancel(errors.New("explicit child cause"))
	if context.Cause(ctx) != cause {
		t.Fatal("explicit child cancellation replaced the parent cause")
	}
}

func TestContextCleanupChildDoesNotCleanParent(t *testing.T) {
	parent, cancelParent := NewContext(Context{Context: context.Background()})
	t.Cleanup(cancelParent)
	var parentCallbacks, childCallbacks atomic.Int32
	parent.OnCancel(func() { parentCallbacks.Add(1) })
	child, cancelChild := NewContext(parent)
	child.OnCancel(func() { childCallbacks.Add(1) })
	cancelChild()
	child.cleanup.mu.Lock()
	watcher := child.cleanup.stopParent
	child.cleanup.mu.Unlock()
	if watcher != nil {
		t.Fatal("explicit cancellation retained the parent watcher")
	}
	if parentCallbacks.Load() != 0 || childCallbacks.Load() != 1 {
		t.Fatal("canceling a child ran callbacks belonging to its parent")
	}
	cancelParent()
	if parentCallbacks.Load() != 1 || childCallbacks.Load() != 1 {
		t.Fatal("parent cancellation repeated child cleanup or lost parent cleanup")
	}
}

func TestContextCleanupParentWithoutHold(t *testing.T) {
	parent, cancelParent := context.WithCancelCause(context.Background())
	ctx, cancel := NewContextWithCause(Context{Context: parent})
	t.Cleanup(func() { cancel(nil) })
	cleaned := make(chan struct{})
	ctx.OnCancel(func() { close(cleaned) })
	cancelParent(errors.New("parent ended"))
	waitCleanupTestChannel(t, cleaned)
	ctx.cleanup.mu.Lock()
	watcher := ctx.cleanup.stopParent
	ctx.cleanup.mu.Unlock()
	if watcher != nil {
		t.Fatal("retirement retained the parent watcher")
	}
}

func TestContextCleanupWaitIncludesCallbacks(t *testing.T) {
	ctx, cancel := NewContext(Context{Context: context.Background()})
	release := ctx.HoldCleanup()
	started, finish, released := make(chan struct{}), make(chan struct{}), make(chan struct{})
	ctx.OnCancel(func() {
		close(started)
		<-finish
	})
	cancel()
	go func() {
		release()
		close(released)
	}()
	waitCleanupTestChannel(t, started)
	deadlineCtx, deadlineCancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer deadlineCancel()
	if err := waitForCleanup(deadlineCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waitForCleanup error = %v; want deadline exceeded during callback", err)
	}
	close(finish)
	waitCleanupTestChannel(t, released)
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer waitCancel()
	if err := waitForCleanup(waitCtx); err != nil {
		t.Fatalf("waitForCleanup after cleanup = %v", err)
	}
}

func TestContextCleanupWaitDeadlineWithOutstandingHold(t *testing.T) {
	ctx, cancel := NewContext(Context{Context: context.Background()})
	release := ctx.HoldCleanup()
	t.Cleanup(release)
	cancel()
	deadlineCtx, deadlineCancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer deadlineCancel()
	if err := waitForCleanup(deadlineCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waitForCleanup error = %v; want deadline exceeded", err)
	}
	release()
}

func TestContextCleanupWhileLoading(t *testing.T) {
	ctx, cancel := NewContextWithCause(Context{Context: context.Background()})
	started, finish, loaded := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	var cleanups, callbacks atomic.Int32
	registerCleanupTestModule(t, "test.cleanup_loading", func() Module {
		return &cleanupTestModule{provision: func(ctx Context) error {
			close(started)
			<-finish
			ctx.OnCancel(func() { callbacks.Add(1) })
			return nil
		}, cleanup: func() error { cleanups.Add(1); return nil }}
	})
	go func() {
		_, err := ctx.LoadModuleByID("test.cleanup_loading", nil)
		loaded <- err
	}()
	waitCleanupTestChannel(t, started)
	cancel(errors.New("cancel during provisioning"))
	if cleanups.Load() != 0 {
		t.Fatal("cleanup raced active provisioning")
	}
	close(finish)
	select {
	case err := <-loaded:
		if err != nil {
			t.Fatalf("load already in progress failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("module load did not finish")
	}
	if cleanups.Load() != 1 || callbacks.Load() != 1 {
		t.Fatalf("cleanup counts: modules=%d callbacks=%d; want 1 each", cleanups.Load(), callbacks.Load())
	}
}

func TestContextCleanupFailedLoading(t *testing.T) {
	for _, phase := range []string{"provision", "validate"} {
		t.Run(phase, func(t *testing.T) {
			ctx, cancel := NewContext(Context{Context: context.Background()})
			started, finish := make(chan struct{}), make(chan struct{})
			loaded := make(chan error, 1)
			var cleanups atomic.Int32
			failure := errors.New("failed during " + phase)
			registerCleanupTestModule(t, "test.cleanup_loading_failure", func() Module {
				mod := &cleanupTestModule{cleanup: func() error { cleanups.Add(1); return nil }}
				fail := func() error { close(started); <-finish; return failure }
				if phase == "provision" {
					mod.provision = func(Context) error { return fail() }
				} else {
					mod.validate = fail
				}
				return mod
			})
			go func() {
				_, err := ctx.LoadModuleByID("test.cleanup_loading_failure", nil)
				loaded <- err
			}()
			waitCleanupTestChannel(t, started)
			cancel()
			close(finish)
			select {
			case err := <-loaded:
				if err == nil {
					t.Fatal("failed module load returned nil error")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("failed module load did not finish")
			}
			if cleanups.Load() != 1 {
				t.Fatalf("module cleanup = %d; want once after load failure", cleanups.Load())
			}
			waitCleanupTestChannel(t, ctx.cleanup.done)
		})
	}
}

func TestContextCleanupWaitRetiresCanceledParentBeforeWatcher(t *testing.T) {
	parent, cancelParent := context.WithCancelCause(context.Background())
	ctx, cancel := NewContextWithCause(Context{Context: parent})
	t.Cleanup(func() { cancel(nil) })
	var callbacks atomic.Int32
	ctx.OnCancel(func() { callbacks.Add(1) })
	release := ctx.HoldCleanup()
	t.Cleanup(release)
	// Suppress the asynchronous watcher to model it not being scheduled yet.
	// Done must still close synchronously through ordinary parent cancellation.
	ctx.cleanup.mu.Lock()
	stopWatcher := ctx.cleanup.stopParent
	ctx.cleanup.mu.Unlock()
	if stopWatcher == nil || !stopWatcher() {
		t.Fatal("could not delay the parent retirement watcher")
	}
	cause := errors.New("parent retired before watcher ran")
	cancelParent(cause)
	if ctx.Err() != context.Canceled || context.Cause(ctx) != cause {
		t.Fatal("parent did not synchronously cancel its child")
	}
	deadlineCtx, deadlineCancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer deadlineCancel()
	if err := waitForCleanup(deadlineCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("wait ignored a canceled held child before its watcher ran: %v", err)
	}
	if callbacks.Load() != 0 {
		t.Fatal("waiting bypassed the outstanding cleanup hold")
	}
	release()
	if callbacks.Load() != 1 || context.Cause(ctx) != cause {
		t.Fatal("wait-triggered retirement lost cleanup or its original cause")
	}
	pendingCleanupsMu.Lock()
	_, retained := pendingCleanups[ctx.cleanup]
	pendingCleanupsMu.Unlock()
	if retained {
		t.Fatal("completed child cleanup leaked in the global registry")
	}
}

func TestContextCleanupLiveHoldersDoNotBlockOrLeak(t *testing.T) {
	ctx, cancel := NewContext(Context{Context: context.Background()})
	t.Cleanup(cancel)
	release1, release2 := ctx.HoldCleanup(), ctx.HoldCleanup()
	t.Cleanup(release1)
	t.Cleanup(release2)
	deadlineCtx, deadlineCancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer deadlineCancel()
	if err := waitForCleanup(deadlineCtx); err != nil {
		t.Fatalf("still-live holders blocked cleanup waiting: %v", err)
	}
	release1()
	pendingCleanupsMu.Lock()
	_, retained := pendingCleanups[ctx.cleanup]
	pendingCleanupsMu.Unlock()
	if !retained {
		t.Fatal("first release removed another live holder from tracking")
	}
	release2()
	pendingCleanupsMu.Lock()
	_, retained = pendingCleanups[ctx.cleanup]
	pendingCleanupsMu.Unlock()
	if retained {
		t.Fatal("final live release retained the context in the global registry")
	}
}

func TestContextCleanupFinalReleaseRetiresCanceledParent(t *testing.T) {
	parent, cancelParent := context.WithCancelCause(context.Background())
	ctx, cancel := NewContextWithCause(Context{Context: parent})
	t.Cleanup(func() { cancel(nil) })
	var callbacks atomic.Int32
	ctx.OnCancel(func() { callbacks.Add(1) })
	release := ctx.HoldCleanup()
	t.Cleanup(release)
	ctx.cleanup.mu.Lock()
	stopWatcher := ctx.cleanup.stopParent
	ctx.cleanup.mu.Unlock()
	if stopWatcher == nil || !stopWatcher() {
		t.Fatal("could not delay the parent retirement watcher")
	}
	cancelParent(errors.New("parent canceled before final release"))
	release()
	if callbacks.Load() != 1 {
		t.Fatal("final release dropped a canceled context before cleanup was requested")
	}
	pendingCleanupsMu.Lock()
	_, retained := pendingCleanups[ctx.cleanup]
	pendingCleanupsMu.Unlock()
	if retained {
		t.Fatal("final release leaked a canceled context after cleanup")
	}
}

func TestContextCleanupWaitRetiresCanceledOwnerWithoutHold(t *testing.T) {
	for _, resource := range []string{"callback", "module"} {
		t.Run(resource, func(t *testing.T) {
			parent, cancelParent := context.WithCancelCause(context.Background())
			ctx, cancel := NewContextWithCause(Context{Context: parent})
			t.Cleanup(func() { cancel(nil) })
			var cleaned atomic.Int32
			if resource == "callback" {
				ctx.OnCancel(func() { cleaned.Add(1) })
			} else {
				registerCleanupTestModule(t, "test.cleanup_unheld_owner", func() Module {
					return &cleanupTestModule{cleanup: func() error { cleaned.Add(1); return nil }}
				})
				if _, err := ctx.LoadModuleByID("test.cleanup_unheld_owner", nil); err != nil {
					t.Fatal(err)
				}
			}
			if !cleanupTestTracked(ctx) {
				t.Fatal("resource-owning context was not tracked without a hold")
			}
			stopCleanupTestParentWatcher(t, ctx)
			cause := errors.New("resource owner parent ended before watcher ran")
			cancelParent(cause)
			waitCtx, waitCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer waitCancel()
			if err := waitForCleanup(waitCtx); err != nil {
				t.Fatalf("waiting for canceled resource owner: %v", err)
			}
			if cleaned.Load() != 1 || context.Cause(ctx) != cause {
				t.Fatal("wait missed resource cleanup or changed the parent's cause")
			}
			if cleanupTestTracked(ctx) {
				t.Fatal("completed resource owner leaked in the registry")
			}
		})
	}
}

func TestContextCleanupWaitUnheldOwnerRespectsDeadline(t *testing.T) {
	parent, cancelParent := context.WithCancelCause(context.Background())
	ctx, cancel := NewContextWithCause(Context{Context: parent})
	t.Cleanup(func() { cancel(nil) })
	started, finish := make(chan struct{}), make(chan struct{})
	var finishOnce sync.Once
	unblock := func() { finishOnce.Do(func() { close(finish) }) }
	t.Cleanup(unblock)
	ctx.OnCancel(func() {
		close(started)
		<-finish
	})
	stopCleanupTestParentWatcher(t, ctx)
	cancelParent(errors.New("unheld owner parent retired"))
	deadlineCtx, deadlineCancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer deadlineCancel()
	result := make(chan error, 1)
	go func() { result <- waitForCleanup(deadlineCtx) }()
	select {
	case err := <-result:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("wait ignored canceled unheld resources: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("callback running in waitForCleanup bypassed the caller's deadline")
	}
	waitCleanupTestChannel(t, started)
	if !cleanupTestTracked(ctx) {
		t.Fatal("blocked resource callback disappeared from exit tracking")
	}
	unblock()
	waitCleanupTestChannel(t, ctx.cleanup.done)
	// Done closes just before registry deletion under the same state lock.
	ctx.cleanup.mu.Lock()
	retained := cleanupTestTracked(ctx)
	ctx.cleanup.mu.Unlock()
	if retained {
		t.Fatal("finished callback owner leaked in the registry")
	}
}

func TestContextCleanupLiveResourceOwnersDoNotBlock(t *testing.T) {
	for _, resource := range []string{"callback", "module"} {
		t.Run(resource, func(t *testing.T) {
			ctx, cancel := NewContext(Context{Context: context.Background()})
			t.Cleanup(cancel)
			var cleaned atomic.Int32
			if resource == "callback" {
				ctx.OnCancel(func() { cleaned.Add(1) })
			} else {
				registerCleanupTestModule(t, "test.cleanup_live_owner", func() Module {
					return &cleanupTestModule{cleanup: func() error { cleaned.Add(1); return nil }}
				})
				if _, err := ctx.LoadModuleByID("test.cleanup_live_owner", nil); err != nil {
					t.Fatal(err)
				}
			}
			deadlineCtx, deadlineCancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
			defer deadlineCancel()
			if err := waitForCleanup(deadlineCtx); err != nil {
				t.Fatalf("live resource owner blocked waiting: %v", err)
			}
			if cleaned.Load() != 0 || ctx.Err() != nil {
				t.Fatal("exit waiting retired a still-live resource owner")
			}
			cancel()
			if cleaned.Load() != 1 || cleanupTestTracked(ctx) {
				t.Fatal("normal owner cancellation lost cleanup or leaked registry state")
			}
		})
	}
}

func stopCleanupTestParentWatcher(t *testing.T, ctx Context) {
	t.Helper()
	ctx.cleanup.mu.Lock()
	stopWatcher := ctx.cleanup.stopParent
	ctx.cleanup.mu.Unlock()
	if stopWatcher == nil || !stopWatcher() {
		t.Fatal("could not delay the parent retirement watcher")
	}
}

func cleanupTestTracked(ctx Context) bool {
	pendingCleanupsMu.Lock()
	_, retained := pendingCleanups[ctx.cleanup]
	pendingCleanupsMu.Unlock()
	return retained
}

func TestContextCleanupParentsBeforeChildren(t *testing.T) {
	ctx, cancel := NewContextWithCause(Context{Context: context.Background()})
	t.Cleanup(func() { cancel(nil) })
	var childAlive atomic.Bool
	var parents, children atomic.Int32
	childAlive.Store(true)
	registerCleanupTestModule(t, "test.cleanup_child_dependency", func() Module {
		return &cleanupTestModule{cleanup: func() error {
			if parents.Load() != 1 {
				t.Error("child cleanup ran before its parent finished using it")
			}
			childAlive.Store(false)
			children.Add(1)
			return nil
		}}
	})
	registerCleanupTestModule(t, "test.cleanup_parent_dependency", func() Module {
		return &cleanupTestModule{
			provision: func(ctx Context) error {
				_, err := ctx.LoadModuleByID("test.cleanup_child_dependency", nil)
				return err
			},
			cleanup: func() error {
				if !childAlive.Load() {
					t.Error("parent cleanup could not use its child resources")
				}
				parents.Add(1)
				return nil
			},
		}
	})
	ctx.OnCancel(func() {
		if parents.Load() != 1 || children.Load() != 1 || childAlive.Load() {
			t.Error("resource callback ran before parent and child cleanup completed")
		}
	})
	if _, err := ctx.LoadModuleByID("test.cleanup_parent_dependency", nil); err != nil {
		t.Fatal(err)
	}
	cause := errors.New("dependency configuration retired")
	cancel(cause)
	cancel(nil)
	if parents.Load() != 1 || children.Load() != 1 {
		t.Fatalf("duplicate or missing module cleanup: parents=%d children=%d", parents.Load(), children.Load())
	}
	if context.Cause(ctx) != cause {
		t.Fatal("cleanup order changed the cancellation cause")
	}
}

func TestContextCleanupKeepsLogWriterAlive(t *testing.T) {
	ctx, cancel := NewContext(Context{Context: context.Background()})
	t.Cleanup(cancel)
	writer := new(cleanupTestLogWriter)
	logger := zap.New(zapcore.NewCore(
		zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
		zapcore.AddSync(writer), zapcore.InfoLevel,
	))
	var modules, lateCallbacks atomic.Int32
	// Logging registers its resource callback before provisioning modules.
	ctx.OnCancel(func() { writer.closed.Store(true) })
	registerCleanupTestModule(t, "test.cleanup_logging", func() Module {
		return &cleanupTestModule{cleanup: func() error {
			logger.Info("module cleanup diagnostic")
			ctx.OnCancel(func() {
				if modules.Load() != 1 {
					t.Error("callback registered during module cleanup ran prematurely")
				}
				lateCallbacks.Add(1)
			})
			modules.Add(1)
			return nil
		}}
	})
	if _, err := ctx.LoadModuleByID("test.cleanup_logging", nil); err != nil {
		t.Fatal(err)
	}
	cancel()
	if writer.writes.Load() != 1 {
		t.Fatal("module cleanup could not write its diagnostic before its log writer closed")
	}
	if !writer.closed.Load() || modules.Load() != 1 || lateCallbacks.Load() != 1 {
		t.Fatal("module or resource callbacks did not finish exactly once")
	}
}

type cleanupTestLogWriter struct {
	closed atomic.Bool
	writes atomic.Int32
}

func (w *cleanupTestLogWriter) Write(p []byte) (int, error) {
	if w.closed.Load() {
		return 0, errors.New("cleanup log writer already closed")
	}
	w.writes.Add(1)
	return len(p), nil
}

func waitCleanupTestChannel(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for cleanup synchronization")
	}
}

func registerCleanupTestModule(t *testing.T, id string, constructor func() Module) {
	t.Helper()
	modulesMu.Lock()
	previous, existed := modules[id]
	modules[id] = ModuleInfo{ID: ModuleID(id), New: constructor}
	modulesMu.Unlock()
	t.Cleanup(func() {
		modulesMu.Lock()
		if existed {
			modules[id] = previous
		} else {
			delete(modules, id)
		}
		modulesMu.Unlock()
	})
}

type cleanupTestModule struct {
	provision func(Context) error
	validate  func() error
	cleanup   func() error
}

func (*cleanupTestModule) CaddyModule() ModuleInfo {
	return ModuleInfo{ID: "test.cleanup", New: func() Module { return new(cleanupTestModule) }}
}

func (m *cleanupTestModule) Provision(ctx Context) error {
	if m.provision != nil {
		return m.provision(ctx)
	}
	return nil
}

func (m *cleanupTestModule) Validate() error {
	if m.validate != nil {
		return m.validate()
	}
	return nil
}

func (m *cleanupTestModule) Cleanup() error {
	if m.cleanup != nil {
		return m.cleanup()
	}
	return nil
}

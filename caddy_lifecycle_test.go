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
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

type lifecycleTestState struct {
	startErr error
	stopErr  error
	started  atomic.Int32
	stopped  atomic.Int32
	cleaned  atomic.Int32
	release  func()
}

var (
	lifecycleTestStates sync.Map
	lifecycleTestID     atomic.Uint64
)

type lifecycleTestApp struct {
	ID    string `json:"id"`
	ctx   Context
	state *lifecycleTestState
}

func (lifecycleTestApp) CaddyModule() ModuleInfo {
	return ModuleInfo{ID: "lifecycle_test", New: func() Module { return new(lifecycleTestApp) }}
}

func (app *lifecycleTestApp) Provision(ctx Context) error {
	value, ok := lifecycleTestStates.Load(app.ID)
	if !ok {
		return errors.New("missing lifecycle test state")
	}
	app.ctx = ctx
	app.state = value.(*lifecycleTestState)
	return nil
}

func (app *lifecycleTestApp) Start() error {
	app.state.started.Add(1)
	return app.state.startErr
}

func (app *lifecycleTestApp) Stop() error {
	app.state.stopped.Add(1)
	app.state.release = app.ctx.HoldCleanup()
	return app.state.stopErr
}

func (app *lifecycleTestApp) Cleanup() error {
	app.state.cleaned.Add(1)
	return nil
}

func init() { RegisterModule(lifecycleTestApp{}) }

func lifecycleTestConfig(t *testing.T, state *lifecycleTestState, failAfterStart bool) *Config {
	t.Helper()
	id := fmt.Sprintf("lifecycle%d", lifecycleTestID.Add(1))
	lifecycleTestStates.Store(id, state)
	t.Cleanup(func() {
		if state.release != nil {
			state.release()
		}
		lifecycleTestStates.Delete(id)
	})
	config := map[string]any{
		"admin": map[string]any{"disabled": true},
		"apps":  map[string]any{"lifecycle_test": map[string]any{"id": id}},
	}
	if failAfterStart {
		config["admin"].(map[string]any)["config"] = map[string]any{
			"load": map[string]any{"module": "missing_lifecycle_test_loader"},
		}
	}
	raw, err := json.Marshal(config)
	if err != nil {
		t.Fatal(err)
	}
	var cfg Config
	if err := json.Unmarshal(raw, &cfg); err != nil {
		t.Fatal(err)
	}
	return &cfg
}

func TestRunFailureStopsAppsBeforeCleanup(t *testing.T) {
	for _, failAfterStart := range []bool{false, true} {
		t.Run(fmt.Sprintf("finish_setup_failure=%t", failAfterStart), func(t *testing.T) {
			startErr := errors.New("intentional lifecycle start failure")
			stopErr := errors.New("intentional lifecycle stop failure")
			state := &lifecycleTestState{stopErr: stopErr}
			if !failAfterStart {
				state.startErr = startErr
			}
			ctx, err := run(lifecycleTestConfig(t, state, failAfterStart), true)
			if err == nil {
				t.Fatal("expected configuration startup failure")
			}
			if !errors.Is(err, stopErr) || (!failAfterStart && !errors.Is(err, startErr)) {
				t.Fatalf("startup error lost the original causes: %v", err)
			}
			if ctx.Err() != context.Canceled || !errors.Is(context.Cause(ctx), stopErr) {
				t.Fatalf("retired context lost the failure cause: %v", context.Cause(ctx))
			}
			if state.started.Load() != 1 || state.stopped.Load() != 1 {
				t.Fatalf("expected one start and stop, got %d and %d", state.started.Load(), state.stopped.Load())
			}
			if state.cleaned.Load() != 0 {
				t.Fatal("cleanup ran before the partially started app released its resources")
			}
			if state.release == nil {
				t.Fatal("partially started app was not allowed to hold cleanup")
			}
			state.release()
			ctx.cfg.cancelFunc(errors.New("later cancellation"))
			if state.cleaned.Load() != 1 {
				t.Fatalf("expected exactly one cleanup, got %d", state.cleaned.Load())
			}
		})
	}
}

func TestValidationCleansUnstartedModulesOnce(t *testing.T) {
	state := new(lifecycleTestState)
	cfg := lifecycleTestConfig(t, state, false)
	if err := Validate(cfg); err != nil {
		t.Fatal(err)
	}
	cfg.cancelFunc(errors.New("later validation cancellation"))
	if state.started.Load() != 0 || state.stopped.Load() != 0 || state.cleaned.Load() != 1 {
		t.Fatalf("unexpected validation lifecycle: start=%d stop=%d cleanup=%d",
			state.started.Load(), state.stopped.Load(), state.cleaned.Load())
	}
}

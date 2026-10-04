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
	"bytes"
	"slices"

	"go.uber.org/zap"
)

// Logging generations can finish draining out of order. Retiring an older
// generation must not undo the newer generation's process-global loggers, and
// a failed replacement must restore a generation whose writers are still open.
// defaultLoggerMu protects this list as well as both global logger selections.
var activeLogConfigs []*Logging

// The standard logger keeps one output writer for the process. Replacing its
// output during cleanup can block behind a network sink's Write and prevent
// the very Close that would unblock it. Only the selected Zap logger changes.
type standardLogWriter struct{}

func (standardLogWriter) Write(data []byte) (int, error) {
	defaultLoggerMu.RLock()
	logger := standardLogLogger
	defaultLoggerMu.RUnlock()
	logger.Info(string(bytes.TrimSpace(data)))
	return len(data), nil
}

func (logging *Logging) activateDefault(newDefault *defaultCustomLog) *defaultCustomLog {
	defaultLoggerMu.Lock()
	defer defaultLoggerMu.Unlock()
	previous := defaultLogger
	logging.defaultLog = newDefault
	activeLogConfigs = append(activeLogConfigs, logging)
	logging.selectDefaultLocked()
	return previous
}

func (logging *Logging) selectDefaultLocked() {
	defaultLogger = logging.defaultLog
	sink := defaultLogger.logger
	if logging.Sink != nil {
		sink = logging.Sink.logger
	}
	standardLogLogger = sink.WithOptions(zap.AddCallerSkip(3))
}

func (logging *Logging) deactivateDefault() {
	defaultLoggerMu.Lock()
	defer defaultLoggerMu.Unlock()
	index := slices.Index(activeLogConfigs, logging)
	if index < 0 {
		return
	}
	activeLogConfigs = slices.Delete(activeLogConfigs, index, index+1)
	if index != len(activeLogConfigs) {
		return // a newer generation still owns the global loggers
	}
	if len(activeLogConfigs) > 0 {
		activeLogConfigs[len(activeLogConfigs)-1].selectDefaultLocked()
		return
	}
	defaultLogger = productionDefaultLogger
	standardLogLogger = defaultLogger.logger.WithOptions(zap.AddCallerSkip(3))
}

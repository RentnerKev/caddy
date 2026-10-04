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
	"fmt"
	"log"
	"sync/atomic"
)

// holdWriters transfers additional references to the caller without keeping the
// configuration or its modules alive. The lock prevents closeLogs from releasing
// the configuration's references while these references are acquired.
func (logging *Logging) holdWriters() (func(), error) {
	logging.writerMu.Lock()
	if logging.writersClosed {
		logging.writerMu.Unlock()
		return nil, fmt.Errorf("retaining log writers: logging configuration already closed")
	}
	keys := make([]string, 0, len(logging.writerKeys))
	for _, key := range logging.writerKeys {
		_, _, err := writers.LoadOrNew(key, func() (Destructor, error) {
			return nil, fmt.Errorf("retaining log writer %s: writer unavailable", key)
		})
		if err != nil {
			logging.writerMu.Unlock()
			for _, retained := range keys {
				_, _ = writers.Delete(retained)
			}
			return nil, err
		}
		keys = append(keys, key)
	}
	logging.writerMu.Unlock()
	var released atomic.Bool
	return func() {
		if !released.CompareAndSwap(false, true) {
			return
		}
		for _, key := range keys {
			if _, err := writers.Delete(key); err != nil {
				log.Printf("[ERROR] Closing retained log writer %v: %v", key, err)
			}
		}
	}, nil
}

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
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

// sharedTCPListenerPool owns native TCP sockets on Unix with matching effective
// Multipath TCP settings. Custom Control, port-zero, and file-descriptor listeners
// keep their separate-bind behavior. It cannot contain the nil values used in
// listenerPool for Unix usage counting.
var sharedTCPListenerPool = NewUsagePool()

// Socket-level settings must not be inherited from an incompatible generation.
// Per-connection keepalive settings are instead applied by each owner's Accept.
type sharedTCPListenerKey struct {
	addressKey   string
	multipathTCP bool
}

// listenReusableSharedStream gives each owner a separately closable reference
// to one listening socket. Non-Unix platforms also use it for their existing
// shared stream and file-descriptor networks.
func listenReusableSharedStream(ctx context.Context, lnKey, network, address string, config net.ListenConfig, socketFile *os.File, pool *UsagePool, countUsage bool) (any, error) {
	nativeTCP := network == "tcp" || network == "tcp4" || network == "tcp6"
	var poolKey any = lnKey
	if countUsage {
		multipathTCP := config.MultipathTCP()
		poolKey = sharedTCPListenerKey{addressKey: lnKey, multipathTCP: multipathTCP}
		// Capture the effective default for this generation even if GODEBUG changes.
		config.SetMultipathTCP(multipathTCP)
	}
	sharedLn, _, err := pool.LoadOrNew(poolKey, func() (Destructor, error) {
		var ln net.Listener
		var err error
		if socketFile != nil {
			ln, err = net.FileListener(socketFile)
		} else {
			listenConfig := config
			if nativeTCP {
				// Each owner applies its own settings after Accept. The first
				// generation must not preconfigure later generations' connections.
				listenConfig.KeepAlive = -1
				listenConfig.KeepAliveConfig = net.KeepAliveConfig{}
			}
			ln, err = listenConfig.Listen(ctx, network, address)
		}
		if err != nil {
			return nil, err
		}
		return &sharedListener{Listener: ln, key: lnKey, acceptToken: make(chan struct{}, 1)}, nil
	})
	if err != nil {
		return nil, err
	}
	if countUsage {
		listenerPool.LoadOrStore(lnKey, nil)
	}
	keepAliveConfig := config.KeepAliveConfig
	if nativeTCP && !keepAliveConfig.Enable && config.KeepAlive >= 0 {
		// Match net.newTCPConn's legacy KeepAlive/default precedence.
		keepAliveConfig = net.KeepAliveConfig{Enable: true, Idle: config.KeepAlive}
	}
	return &fakeCloseListener{sharedListener: sharedLn.(*sharedListener), keepAliveConfig: keepAliveConfig, applyKeepAlive: nativeTCP, closing: make(chan struct{}), pool: pool, poolKey: poolKey, countUsage: countUsage}, nil
}

// fakeCloseListener closes only its own reference until the last owner leaves.
// This type is atomic and values must not be copied.
type fakeCloseListener struct {
	closed atomic.Bool
	*sharedListener
	keepAliveConfig net.KeepAliveConfig
	applyKeepAlive  bool
	closing         chan struct{}
	pool            *UsagePool
	poolKey         any
	countUsage      bool
}

type canSetKeepAliveConfig interface {
	SetKeepAliveConfig(net.KeepAliveConfig) error
}

func (fcl *fakeCloseListener) Accept() (net.Conn, error) {
	if fcl.closed.Load() {
		return nil, fakeClosedErr(fcl)
	}
	// Only the token owner can enter the underlying Accept or clear its deadline.
	// A closed waiter exits without waiting for another owner's future connection.
	select {
	case fcl.acceptToken <- struct{}{}:
	case <-fcl.closing:
		return nil, fakeClosedErr(fcl)
	}
	defer func() { <-fcl.acceptToken }()
	if err := fcl.clearDeadline(); err != nil {
		if fcl.closed.Load() {
			return nil, fakeClosedErr(fcl)
		}
		return nil, err
	}
	// Check after clearing, so a concurrent Close cannot have its wakeup cleared
	// immediately before this owner blocks in Accept.
	if fcl.closed.Load() {
		return nil, fakeClosedErr(fcl)
	}
	conn, err := fcl.Listener.Accept()
	if err == nil {
		// A successful Accept owns a connection even when Close raced it. Hand it
		// to the caller for admission/draining; never silently discard it.
		if tconn, ok := conn.(canSetKeepAliveConfig); ok && (fcl.applyKeepAlive || fcl.keepAliveConfig.Enable) {
			if err := tconn.SetKeepAliveConfig(fcl.keepAliveConfig); err != nil {
				Log().With(zap.String("server", fcl.key)).Warn("unable to set keepalive for new connection:", zap.Error(err))
			}
		}
		return conn, nil
	}
	if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
		_ = fcl.clearDeadline()
		if fcl.closed.Load() {
			return nil, fakeClosedErr(fcl)
		}
	}
	return nil, err
}

func (fcl *fakeCloseListener) Close() error {
	if fcl.closed.CompareAndSwap(false, true) {
		close(fcl.closing)
		_ = fcl.setDeadline()
		if fcl.countUsage {
			_, _ = listenerPool.Delete(fcl.key)
		}
		_, _ = fcl.pool.Delete(fcl.poolKey)
	}
	return nil
}

// sharedListener serializes deadline changes separately from its Accept token.
type sharedListener struct {
	net.Listener
	key          string
	acceptToken  chan struct{}
	deadline     bool
	deadlineMu   sync.Mutex
	destructOnce sync.Once
	destructErr  error
}

func (sl *sharedListener) clearDeadline() error {
	sl.deadlineMu.Lock()
	defer sl.deadlineMu.Unlock()
	if !sl.deadline {
		return nil
	}
	var err error
	if ln, ok := sl.Listener.(*net.TCPListener); ok {
		err = ln.SetDeadline(time.Time{})
	}
	sl.deadline = false
	return err
}

func (sl *sharedListener) setDeadline() error {
	sl.deadlineMu.Lock()
	defer sl.deadlineMu.Unlock()
	if sl.deadline {
		return nil
	}
	var err error
	if ln, ok := sl.Listener.(*net.TCPListener); ok {
		err = ln.SetDeadline(time.Now().Add(-time.Minute))
	}
	if err == nil {
		sl.deadline = true
	}
	return err
}

func (sl *sharedListener) Destruct() error {
	sl.destructOnce.Do(func() { sl.destructErr = sl.Listener.Close() })
	return sl.destructErr
}

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

//go:build unix && !solaris

package caddy

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func sharedTCPTestCustomOwner(t *testing.T, address string, calls *atomic.Int32) deleteListener {
	t.Helper()
	config := net.ListenConfig{Control: func(string, string, syscall.RawConn) error {
		calls.Add(1)
		return nil
	}}
	listener, err := listenReusable(context.Background(), listenerKey("tcp", address), "tcp", address, config)
	if err != nil {
		t.Fatal(err)
	}
	owner, ok := listener.(deleteListener)
	if !ok {
		_ = listener.(net.Listener).Close()
		t.Fatalf("custom-Control listener = %T, want separate bind", listener)
	}
	// Unlike the shared wrapper, this pre-existing reference-count wrapper is
	// not idempotent. Tests release each custom bind exactly once.
	t.Cleanup(func() { _ = owner.Close() })
	return owner
}

func TestSharedTCPListenerCustomControlRemainsPerBind(t *testing.T) {
	address := sharedTCPTestAddress(t)
	var calls atomic.Int32
	first := sharedTCPTestCustomOwner(t, address, &calls)
	second := sharedTCPTestCustomOwner(t, address, &calls)
	if first.Listener == second.Listener {
		t.Fatal("custom-Control calls unexpectedly shared one socket")
	}
	if got := calls.Load(); got != 2 {
		t.Fatalf("Control calls = %d, want one per bind (2)", got)
	}
	if count, exists := listenerPool.References(first.lnKey); !exists || count != 2 {
		t.Fatalf("custom owner count = %d, exists = %v", count, exists)
	}
	if sharedTCPTestPooledAddressExists(first.lnKey) {
		t.Fatal("custom-Control owner inserted into the shared TCP pool")
	}
}

func TestSharedTCPListenerMixedControlOwnersReleaseSocket(t *testing.T) {
	for _, customFirst := range []bool{true, false} {
		name := "pooled-first"
		if customFirst {
			name = "custom-first"
		}
		t.Run(name, func(t *testing.T) {
			address := sharedTCPTestAddress(t)
			var calls atomic.Int32
			var custom deleteListener
			var pooled *fakeCloseListener
			if customFirst {
				custom = sharedTCPTestCustomOwner(t, address, &calls)
				pooled = sharedTCPTestOwner(t, address, net.ListenConfig{})
			} else {
				pooled = sharedTCPTestOwner(t, address, net.ListenConfig{})
				custom = sharedTCPTestCustomOwner(t, address, &calls)
			}
			if count, exists := listenerPool.References(pooled.key); !exists || count != 2 {
				t.Fatalf("mixed owner count = %d, exists = %v", count, exists)
			}
			_ = pooled.Close()
			_ = pooled.Close()
			if count, exists := listenerPool.References(pooled.key); !exists || count != 1 {
				t.Fatalf("remaining custom owner count = %d, exists = %v", count, exists)
			}
			if _, exists := sharedTCPListenerPool.References(pooled.poolKey); exists {
				t.Fatal("unowned shared TCP socket remains pooled")
			}
			if conn, err := pooled.Listener.Accept(); !errors.Is(err, net.ErrClosed) {
				if conn != nil {
					_ = conn.Close()
				}
				t.Fatalf("unowned socket not closed while custom owner remains: %v", err)
			}
			if got := calls.Load(); got != 1 {
				t.Fatalf("custom Control calls = %d, want 1", got)
			}
			_ = custom.Listener.(*net.TCPListener).SetDeadline(time.Now().Add(sharedTCPTestTimeout))
			client := sharedTCPTestQueuedClient(t, address)
			sharedTCPTestFirstResponse(t, custom, client)
		})
	}
}

func TestSharedTCPListenerPortZeroKeepsFreshBind(t *testing.T) {
	key := listenerKey("tcp", "127.0.0.1:0")
	var listeners []net.Listener
	for range 2 {
		listener, err := listenReusable(context.Background(), key, "tcp", "127.0.0.1:0", net.ListenConfig{})
		if err != nil {
			t.Fatal(err)
		}
		owner, ok := listener.(deleteListener)
		if !ok {
			_ = listener.(net.Listener).Close()
			t.Fatalf("port-zero listener = %T, want fresh bind", listener)
		}
		listeners = append(listeners, owner)
		t.Cleanup(func() { _ = owner.Close() })
	}
	if listeners[0].Addr().String() == listeners[1].Addr().String() {
		t.Fatal("port zero reused an ephemeral address")
	}
}

func TestSharedTCPListenerKeepAliveDoesNotInheritFirstOwner(t *testing.T) {
	address := sharedTCPTestAddress(t)
	first := sharedTCPTestOwner(t, address, net.ListenConfig{KeepAlive: time.Second})
	current := sharedTCPTestOwner(t, address, net.ListenConfig{KeepAlive: -1})
	_ = first.Close()
	_ = sharedTCPTestQueuedClient(t, address)
	result := sharedTCPTestResult(t, sharedTCPTestAccept(t, current))
	if result.err != nil {
		t.Fatal(result.err)
	}
	conn := result.conn
	defer conn.Close()
	raw, err := conn.(*net.TCPConn).SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var enabled int
	var socketErr error
	err = raw.Control(func(fd uintptr) {
		enabled, socketErr = unix.GetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_KEEPALIVE)
	})
	if err != nil {
		t.Fatal(err)
	}
	if socketErr != nil {
		t.Fatal(socketErr)
	}
	if enabled != 0 {
		t.Fatal("new owner's disabled keepalive inherited the first owner's setting")
	}
}

func sharedTCPTestPooledAddressExists(addressKey string) bool {
	found := false
	sharedTCPListenerPool.Range(func(key, _ any) bool {
		if key.(sharedTCPListenerKey).addressKey == addressKey {
			found = true
			return false
		}
		return true
	})
	return found
}

func TestSharedTCPListenerMultipathTCPSettings(t *testing.T) {
	address := sharedTCPTestAddress(t)
	config := net.ListenConfig{}
	first := sharedTCPTestOwner(t, address, config)
	// Compare effective settings; newer Go versions enable MPTCP by default.
	config.SetMultipathTCP(config.MultipathTCP())
	equivalent := sharedTCPTestOwner(t, address, config)
	if first.sharedListener != equivalent.sharedListener {
		t.Fatal("same effective MPTCP setting replaced the listening socket")
	}
	config.SetMultipathTCP(!config.MultipathTCP())
	opposite := sharedTCPTestOwner(t, address, config)
	if first.sharedListener == opposite.sharedListener {
		t.Fatal("changed MPTCP setting inherited the first owner's socket")
	}
	oppositeEquivalent := sharedTCPTestOwner(t, address, config)
	if opposite.sharedListener != oppositeEquivalent.sharedListener {
		t.Fatal("same opposite MPTCP setting replaced its listening socket")
	}
	if opposite.poolKey == first.poolKey {
		t.Fatal("different MPTCP settings used the same pool key")
	}
	if count, exists := listenerPool.References(first.key); !exists || count != 4 {
		t.Fatalf("common usage = %d, exists = %v, want 4", count, exists)
	}
	for _, owner := range []*fakeCloseListener{first, opposite} {
		if count, exists := owner.pool.References(owner.poolKey); !exists || count != 2 {
			t.Fatalf("actual socket usage = %d, exists = %v, want 2", count, exists)
		}
	}
	_ = first.Close()
	_ = equivalent.Close()
	if _, exists := sharedTCPListenerPool.References(first.poolKey); exists {
		t.Fatal("unowned MPTCP socket remains pooled")
	}
	if count, exists := listenerPool.References(first.key); !exists || count != 2 {
		t.Fatalf("opposite socket common usage = %d, exists = %v, want 2", count, exists)
	}
	_ = opposite.Close()
	_ = oppositeEquivalent.Close()
	if sharedTCPTestPooledAddressExists(first.key) {
		t.Fatal("final MPTCP owner left an actual socket pooled")
	}
}

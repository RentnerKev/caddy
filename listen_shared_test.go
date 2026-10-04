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
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"
)

const sharedTCPTestTimeout = 3 * time.Second

// A fixed port is necessary to test handoff rather than an ephemeral bind.
// Reserve it dynamically, release it, and bind it exactly once without retries.
func sharedTCPTestAddress(t *testing.T) string {
	t.Helper()
	reservation, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := reservation.Addr().String()
	if err := reservation.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

func sharedTCPTestOwner(t *testing.T, address string, config net.ListenConfig) *fakeCloseListener {
	t.Helper()
	listener, err := listenReusable(context.Background(), listenerKey("tcp", address), "tcp", address, config)
	if err != nil {
		t.Fatal(err)
	}
	owner, ok := listener.(*fakeCloseListener)
	if !ok {
		_ = listener.(net.Listener).Close()
		t.Fatalf("native fixed TCP listener is %T, want shared owner", listener)
	}
	t.Cleanup(func() { _ = owner.Close() })
	return owner
}

func sharedTCPTestRefs(t *testing.T, owner *fakeCloseListener, want int) {
	t.Helper()
	for _, entry := range []struct {
		pool *UsagePool
		key  any
	}{{listenerPool, owner.key}, {owner.pool, owner.poolKey}} {
		got, exists := entry.pool.References(entry.key)
		if got != want || exists != (want > 0) {
			t.Fatalf("pool references = %d, exists = %v; want %d", got, exists, want)
		}
	}
}

func sharedTCPTestQueuedClient(t *testing.T, address string) net.Conn {
	t.Helper()
	conn, err := net.DialTimeout("tcp", address, sharedTCPTestTimeout)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := conn.SetDeadline(time.Now().Add(sharedTCPTestTimeout)); err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(conn, "GET / HTTP/1.1\r\nHost: listener.test\r\nConnection: close\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	return conn
}

// There is one Accept and one client request. No client or server retries hide
// a dropped handshake, queued connection, or first request.
func sharedTCPTestFirstResponse(t *testing.T, owner net.Listener, client net.Conn) {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		conn, err := owner.Accept()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(sharedTCPTestTimeout))
		request, err := http.ReadRequest(bufio.NewReader(conn))
		if err != nil {
			done <- err
			return
		}
		_ = request.Body.Close()
		_, err = io.WriteString(conn, "HTTP/1.1 200 OK\r\nContent-Length: 6\r\nConnection: close\r\n\r\nqueued")
		done <- err
	}()
	response, err := http.ReadResponse(bufio.NewReader(client), nil)
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(response.Body)
	_ = response.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK || string(body) != "queued" {
		t.Fatalf("response = %d %q, want 200 queued", response.StatusCode, body)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(sharedTCPTestTimeout):
		t.Fatal("server did not finish the first request")
	}
}

func TestSharedTCPListenerPreservesQueuedFirstRequest(t *testing.T) {
	address := sharedTCPTestAddress(t)
	key := listenerKey("tcp", address)
	// Use only the net.Listener contract until the actual response. On Unix
	// without shared sockets, closing old loses its already-queued connection;
	// the regression must fail on that request, not on an implementation type.
	listen := func() (net.Listener, func()) {
		value, err := listenReusable(context.Background(), key, "tcp", address, net.ListenConfig{})
		if err != nil {
			t.Fatal(err)
		}
		listener := value.(net.Listener)
		var once sync.Once
		closeListener := func() { once.Do(func() { _ = listener.Close() }) }
		t.Cleanup(closeListener)
		return listener, closeListener
	}
	_, closeOld := listen()
	client := sharedTCPTestQueuedClient(t, address)
	current, closeCurrent := listen()
	closeOld()
	sharedTCPTestFirstResponse(t, current, client)
	closeCurrent()
	if count, exists := listenerPool.References(key); exists || count != 0 {
		t.Fatalf("final listener count = %d, exists = %v; want no owners", count, exists)
	}
}

func TestSharedTCPListenerRollbackKeepsOriginal(t *testing.T) {
	address := sharedTCPTestAddress(t)
	original := sharedTCPTestOwner(t, address, net.ListenConfig{})
	provisional := sharedTCPTestOwner(t, address, net.ListenConfig{})
	client := sharedTCPTestQueuedClient(t, address)
	_ = provisional.Close()
	_ = provisional.Close()
	sharedTCPTestRefs(t, original, 1)
	next := sharedTCPTestOwner(t, address, net.ListenConfig{})
	if next.sharedListener != original.sharedListener {
		t.Fatal("rollback lost the original listening socket")
	}
	_ = next.Close()
	sharedTCPTestRefs(t, original, 1)
	sharedTCPTestFirstResponse(t, original, client)
}

// This gate represents Accept already holding a valid connection when Close
// retires its caller. Returning an error here would orphan that connection.
type sharedTCPTestGate struct {
	net.Listener
	conn    net.Conn
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (gate *sharedTCPTestGate) Accept() (net.Conn, error) {
	close(gate.entered)
	<-gate.release
	return gate.conn, nil
}

func (gate *sharedTCPTestGate) unblock() { gate.once.Do(func() { close(gate.release) }) }

func sharedTCPTestInstallGate(t *testing.T, owner *fakeCloseListener) *sharedTCPTestGate {
	t.Helper()
	conn, peer := net.Pipe()
	t.Cleanup(func() { _ = conn.Close(); _ = peer.Close() })
	gate := &sharedTCPTestGate{Listener: owner.Listener, conn: conn, entered: make(chan struct{}), release: make(chan struct{})}
	owner.sharedListener.Listener = gate
	t.Cleanup(gate.unblock)
	return gate
}

type sharedTCPTestAcceptResult struct {
	conn net.Conn
	err  error
}

func sharedTCPTestAccept(t *testing.T, owner *fakeCloseListener) <-chan sharedTCPTestAcceptResult {
	t.Helper()
	done := make(chan sharedTCPTestAcceptResult, 1)
	go func() {
		conn, err := owner.Accept()
		done <- sharedTCPTestAcceptResult{conn, err}
	}()
	return done
}

func sharedTCPTestResult(t *testing.T, done <-chan sharedTCPTestAcceptResult) sharedTCPTestAcceptResult {
	t.Helper()
	select {
	case result := <-done:
		return result
	case <-time.After(sharedTCPTestTimeout):
		t.Fatal("Accept did not return")
		return sharedTCPTestAcceptResult{}
	}
}

func TestSharedTCPListenerAcceptSurvivesCloseRace(t *testing.T) {
	address := sharedTCPTestAddress(t)
	old := sharedTCPTestOwner(t, address, net.ListenConfig{})
	survivor := sharedTCPTestOwner(t, address, net.ListenConfig{})
	gate := sharedTCPTestInstallGate(t, old)
	done := sharedTCPTestAccept(t, old)
	select {
	case <-gate.entered:
	case <-time.After(sharedTCPTestTimeout):
		t.Fatal("Accept did not enter its underlying listener")
	}
	_ = old.Close()
	gate.unblock()
	result := sharedTCPTestResult(t, done)
	if result.conn != gate.conn || result.err != nil {
		t.Fatalf("accepted connection discarded during Close: %v", result.err)
	}
	if conn, err := old.Accept(); conn != nil || !errors.Is(err, errFakeClosed) {
		t.Fatalf("subsequent Accept = %v, %v; want closed", conn, err)
	}
	sharedTCPTestRefs(t, survivor, 1)
}

func TestSharedTCPListenerClosedWaiterUnblocks(t *testing.T) {
	address := sharedTCPTestAddress(t)
	live := sharedTCPTestOwner(t, address, net.ListenConfig{})
	retiring := sharedTCPTestOwner(t, address, net.ListenConfig{})
	gate := sharedTCPTestInstallGate(t, live)
	liveDone := sharedTCPTestAccept(t, live)
	select {
	case <-gate.entered:
	case <-time.After(sharedTCPTestTimeout):
		t.Fatal("live Accept did not enter its underlying listener")
	}
	retiringDone := sharedTCPTestAccept(t, retiring)
	_ = retiring.Close()
	result := sharedTCPTestResult(t, retiringDone)
	if result.conn != nil || !errors.Is(result.err, errFakeClosed) {
		t.Fatalf("closed waiter = %v, %v; want closed", result.conn, result.err)
	}
	// The live owner remains blocked until its own connection is available.
	gate.unblock()
	if result := sharedTCPTestResult(t, liveDone); result.conn != gate.conn || result.err != nil {
		t.Fatalf("live Accept = %v, %v", result.conn, result.err)
	}
}

type sharedTCPTestKeepAliveConn struct {
	net.Conn
	config net.KeepAliveConfig
	calls  int
}

func (conn *sharedTCPTestKeepAliveConn) SetKeepAliveConfig(config net.KeepAliveConfig) error {
	conn.config = config
	conn.calls++
	return nil
}

type sharedTCPTestImmediateListener struct {
	net.Listener
	conn net.Conn
}

func (listener sharedTCPTestImmediateListener) Accept() (net.Conn, error) { return listener.conn, nil }

func TestSharedTCPListenerKeepAlivePerOwner(t *testing.T) {
	for _, test := range []struct {
		name   string
		config net.ListenConfig
		want   net.KeepAliveConfig
	}{
		{"explicit", net.ListenConfig{KeepAliveConfig: net.KeepAliveConfig{Enable: true, Idle: 9 * time.Second, Interval: 3 * time.Second, Count: 2}}, net.KeepAliveConfig{Enable: true, Idle: 9 * time.Second, Interval: 3 * time.Second, Count: 2}},
		{"legacy", net.ListenConfig{KeepAlive: 6 * time.Second}, net.KeepAliveConfig{Enable: true, Idle: 6 * time.Second}},
		{"default", net.ListenConfig{}, net.KeepAliveConfig{Enable: true}},
		{"disabled", net.ListenConfig{KeepAlive: -1}, net.KeepAliveConfig{}},
	} {
		t.Run(test.name, func(t *testing.T) {
			address := sharedTCPTestAddress(t)
			first := sharedTCPTestOwner(t, address, net.ListenConfig{KeepAlive: time.Second})
			current := sharedTCPTestOwner(t, address, test.config)
			conn, peer := net.Pipe()
			t.Cleanup(func() { _ = conn.Close(); _ = peer.Close() })
			record := &sharedTCPTestKeepAliveConn{Conn: conn}
			first.sharedListener.Listener = sharedTCPTestImmediateListener{Listener: first.Listener, conn: record}
			got, err := current.Accept()
			if err != nil || got != record {
				t.Fatalf("Accept = %v, %v", got, err)
			}
			if record.calls != 1 || record.config != test.want {
				t.Fatal(fmt.Sprintf("keepalive = %#v (%d calls), want %#v", record.config, record.calls, test.want))
			}
		})
	}
}

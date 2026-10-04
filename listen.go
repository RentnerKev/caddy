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

//go:build !unix || solaris

package caddy

import (
	"context"
	"fmt"
	"net"
	"os"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

func listenReusable(ctx context.Context, lnKey string, network, address string, config net.ListenConfig) (any, error) {
	var socketFile *os.File

	fd := slices.Contains([]string{"fd", "fdgram"}, network)
	if fd {
		socketFd, err := strconv.ParseUint(address, 0, strconv.IntSize)
		if err != nil {
			return nil, fmt.Errorf("invalid file descriptor: %v", err)
		}

		func() {
			socketFilesMu.Lock()
			defer socketFilesMu.Unlock()

			socketFdWide := uintptr(socketFd)
			var ok bool

			socketFile, ok = socketFiles[socketFdWide]

			if !ok {
				socketFile = os.NewFile(socketFdWide, lnKey)
				if socketFile != nil {
					socketFiles[socketFdWide] = socketFile
				}
			}
		}()

		if socketFile == nil {
			return nil, fmt.Errorf("invalid socket file descriptor: %d", socketFd)
		}
	}

	datagram := slices.Contains([]string{"udp", "udp4", "udp6", "unixgram", "fdgram"}, network)
	if datagram {
		sharedPc, _, err := listenerPool.LoadOrNew(lnKey, func() (Destructor, error) {
			var (
				pc  net.PacketConn
				err error
			)
			if fd {
				pc, err = net.FilePacketConn(socketFile)
			} else {
				pc, err = config.ListenPacket(ctx, network, address)
			}
			if err != nil {
				return nil, err
			}
			return &sharedPacketConn{PacketConn: pc, key: lnKey}, nil
		})
		if err != nil {
			return nil, err
		}
		return &fakeClosePacketConn{sharedPacketConn: sharedPc.(*sharedPacketConn)}, nil
	}

	return listenReusableSharedStream(ctx, lnKey, network, address, config, socketFile, listenerPool, false)
}

// fakeClosePacketConn is like fakeCloseListener, but for PacketConns,
// or more specifically, *net.UDPConn
type fakeClosePacketConn struct {
	closed            atomic.Bool
	*sharedPacketConn // embedded, so we also become a net.PacketConn; its key is used in Close
}

func (fcpc *fakeClosePacketConn) ReadFrom(p []byte) (n int, addr net.Addr, err error) {
	// if the listener is already "closed", return error
	if fcpc.closed.Load() {
		return 0, nil, &net.OpError{
			Op:   "readfrom",
			Net:  fcpc.LocalAddr().Network(),
			Addr: fcpc.LocalAddr(),
			Err:  errFakeClosed,
		}
	}

	// call underlying readfrom
	n, addr, err = fcpc.sharedPacketConn.ReadFrom(p)
	if err != nil {
		// this server was stopped, so clear the deadline and let
		// any new server continue reading; but we will exit
		if fcpc.closed.Load() {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				if err = fcpc.SetReadDeadline(time.Time{}); err != nil {
					return n, addr, err
				}
			}
		}
		return n, addr, err
	}

	return n, addr, err
}

// Close won't close the underlying socket unless there is no more reference, then listenerPool will close it.
func (fcpc *fakeClosePacketConn) Close() error {
	if fcpc.closed.CompareAndSwap(false, true) {
		_ = fcpc.SetReadDeadline(time.Now()) // unblock ReadFrom() calls to kick old servers out of their loops
		_, _ = listenerPool.Delete(fcpc.sharedPacketConn.key)
	}
	return nil
}

func (fcpc *fakeClosePacketConn) Unwrap() net.PacketConn {
	return fcpc.sharedPacketConn.PacketConn
}

// sharedPacketConn is like sharedListener, but for net.PacketConns.
type sharedPacketConn struct {
	net.PacketConn
	key string
}

// Destruct closes the underlying socket.
func (spc *sharedPacketConn) Destruct() error {
	return spc.PacketConn.Close()
}

// Unwrap returns the underlying socket
func (spc *sharedPacketConn) Unwrap() net.PacketConn {
	return spc.PacketConn
}

// Interface guards (see https://github.com/caddyserver/caddy/issues/3998)
var (
	_ (interface {
		Unwrap() net.PacketConn
	}) = (*fakeClosePacketConn)(nil)
)

// socketFiles is a fd -> *os.File map used to make a FileListener/FilePacketConn from a socket file descriptor.
var socketFiles = map[uintptr]*os.File{}

// socketFilesMu synchronizes socketFiles insertions
var socketFilesMu sync.Mutex

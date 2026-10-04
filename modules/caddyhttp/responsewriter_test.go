package caddyhttp

import (
	"bufio"
	"bytes"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

type responseWriterSpy interface {
	http.ResponseWriter
	Written() string
	CalledReadFrom() bool
}

var (
	_ responseWriterSpy = (*baseRespWriter)(nil)
	_ responseWriterSpy = (*readFromRespWriter)(nil)
)

// a barebones http.ResponseWriter mock
type baseRespWriter []byte

func (brw *baseRespWriter) Write(d []byte) (int, error) {
	*brw = append(*brw, d...)
	return len(d), nil
}
func (brw *baseRespWriter) Header() http.Header        { return nil }
func (brw *baseRespWriter) WriteHeader(statusCode int) {}
func (brw *baseRespWriter) Written() string            { return string(*brw) }
func (brw *baseRespWriter) CalledReadFrom() bool       { return false }

// an http.ResponseWriter mock that supports ReadFrom
type readFromRespWriter struct {
	baseRespWriter
	called bool
}

func (rf *readFromRespWriter) ReadFrom(r io.Reader) (int64, error) {
	rf.called = true
	return io.Copy(&rf.baseRespWriter, r)
}

func (rf *readFromRespWriter) CalledReadFrom() bool { return rf.called }

type hijackRespWriter struct {
	baseRespWriter
	header http.Header
	status int
	conn   net.Conn
}

func newHijackRespWriter() *hijackRespWriter {
	return &hijackRespWriter{
		header: make(http.Header),
		conn:   stubConn{},
	}
}

func (hrw *hijackRespWriter) Header() http.Header {
	return hrw.header
}

func (hrw *hijackRespWriter) WriteHeader(statusCode int) {
	hrw.status = statusCode
}

func (hrw *hijackRespWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	br := bufio.NewReader(hrw.conn)
	bw := bufio.NewWriter(hrw.conn)
	return hrw.conn, bufio.NewReadWriter(br, bw), nil
}

type stubConn struct{}

func (stubConn) Read(_ []byte) (int, error)       { return 0, io.EOF }
func (stubConn) Write(p []byte) (int, error)      { return len(p), nil }
func (stubConn) Close() error                     { return nil }
func (stubConn) LocalAddr() net.Addr              { return stubAddr("local") }
func (stubConn) RemoteAddr() net.Addr             { return stubAddr("remote") }
func (stubConn) SetDeadline(time.Time) error      { return nil }
func (stubConn) SetReadDeadline(time.Time) error  { return nil }
func (stubConn) SetWriteDeadline(time.Time) error { return nil }

type stubAddr string

func (a stubAddr) Network() string { return "tcp" }
func (a stubAddr) String() string  { return string(a) }

func TestResponseWriterWrapperReadFrom(t *testing.T) {
	tests := map[string]struct {
		responseWriter responseWriterSpy
		wantReadFrom   bool
	}{
		"no ReadFrom": {
			responseWriter: &baseRespWriter{},
			wantReadFrom:   false,
		},
		"has ReadFrom": {
			responseWriter: &readFromRespWriter{},
			wantReadFrom:   true,
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			// what we expect middlewares to do:
			type myWrapper struct {
				*ResponseWriterWrapper
			}

			wrapped := myWrapper{
				ResponseWriterWrapper: &ResponseWriterWrapper{ResponseWriter: tt.responseWriter},
			}

			const srcData = "boo!"
			// hides everything but Read, since strings.Reader implements WriteTo it would
			// take precedence over our ReadFrom.
			src := struct{ io.Reader }{strings.NewReader(srcData)}

			if _, err := io.Copy(wrapped, src); err != nil {
				t.Errorf("%s: Copy() err = %v", name, err)
			}

			if got := tt.responseWriter.Written(); got != srcData {
				t.Errorf("%s: data = %q, want %q", name, got, srcData)
			}

			if tt.responseWriter.CalledReadFrom() != tt.wantReadFrom {
				if tt.wantReadFrom {
					t.Errorf("%s: ReadFrom() should have been called", name)
				} else {
					t.Errorf("%s: ReadFrom() should not have been called", name)
				}
			}
		})
	}
}

func TestResponseWriterWrapperUnwrap(t *testing.T) {
	w := &ResponseWriterWrapper{&baseRespWriter{}}

	if _, ok := w.Unwrap().(*baseRespWriter); !ok {
		t.Errorf("Unwrap() doesn't return the underlying ResponseWriter")
	}
}

func TestResponseRecorderReadFrom(t *testing.T) {
	tests := map[string]struct {
		responseWriter responseWriterSpy
		shouldBuffer   bool
		wantReadFrom   bool
	}{
		"buffered plain": {
			responseWriter: &baseRespWriter{},
			shouldBuffer:   true,
			wantReadFrom:   false,
		},
		"streamed plain": {
			responseWriter: &baseRespWriter{},
			shouldBuffer:   false,
			wantReadFrom:   false,
		},
		"buffered ReadFrom": {
			responseWriter: &readFromRespWriter{},
			shouldBuffer:   true,
			wantReadFrom:   false,
		},
		"streamed ReadFrom": {
			responseWriter: &readFromRespWriter{},
			shouldBuffer:   false,
			wantReadFrom:   true,
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer

			rr := NewResponseRecorder(tt.responseWriter, &buf, func(status int, header http.Header) bool {
				return tt.shouldBuffer
			})

			const srcData = "boo!"
			// hides everything but Read, since strings.Reader implements WriteTo it would
			// take precedence over our ReadFrom.
			src := struct{ io.Reader }{strings.NewReader(srcData)}

			if _, err := io.Copy(rr, src); err != nil {
				t.Errorf("Copy() err = %v", err)
			}

			wantStreamed := srcData
			wantBuffered := ""
			if tt.shouldBuffer {
				wantStreamed = ""
				wantBuffered = srcData
			}

			if got := tt.responseWriter.Written(); got != wantStreamed {
				t.Errorf("streamed data = %q, want %q", got, wantStreamed)
			}
			if got := buf.String(); got != wantBuffered {
				t.Errorf("buffered data = %q, want %q", got, wantBuffered)
			}

			if tt.responseWriter.CalledReadFrom() != tt.wantReadFrom {
				if tt.wantReadFrom {
					t.Errorf("ReadFrom() should have been called")
				} else {
					t.Errorf("ReadFrom() should not have been called")
				}
			}
		})
	}
}

func TestResponseRecorderSwitchingProtocolsIsHijackAware(t *testing.T) {
	w := newHijackRespWriter()
	var buf bytes.Buffer

	rr := NewResponseRecorder(w, &buf, func(status int, header http.Header) bool {
		return true
	})
	rr.WriteHeader(http.StatusSwitchingProtocols)

	if rr.Status() != http.StatusSwitchingProtocols {
		t.Fatalf("status = %d, want %d", rr.Status(), http.StatusSwitchingProtocols)
	}
	if w.status != http.StatusSwitchingProtocols {
		t.Fatalf("underlying status = %d, want %d", w.status, http.StatusSwitchingProtocols)
	}

	hj, ok := rr.(http.Hijacker)
	if !ok {
		t.Fatal("response recorder does not implement http.Hijacker")
	}
	conn, _, err := hj.Hijack()
	if err != nil {
		t.Fatalf("Hijack() error = %v", err)
	}
	defer conn.Close()

	if rr.Buffered() {
		t.Fatal("hijacked response should not remain buffered")
	}
	if rr.DetachAfterHijack(true) {
		t.Fatal("response recorder should report hijacked state by returning false")
	}
	if DetachResponseWriterAfterHijack(rr, true) {
		t.Fatal("DetachResponseWriterAfterHijack() should report false after hijack")
	}
	if err := rr.WriteResponse(); err != nil {
		t.Fatalf("WriteResponse() after hijack returned error: %v", err)
	}
	if rr.Size() != 0 {
		t.Fatalf("size = %d, want 0 after hijack handshake", rr.Size())
	}
	if got := w.Written(); got != "" {
		t.Fatalf("unexpected buffered body write after hijack: %q", got)
	}
}

func TestResponseRecorderNestedDetach(t *testing.T) {
	for _, detached := range []bool{false, true} {
		name := "attached"
		if detached {
			name = "detached"
		}
		t.Run(name, func(t *testing.T) {
			inner := NewResponseRecorder(wrapNativeResponseWriter(newHijackRespWriter()), nil, nil)
			// Include an intervening middleware wrapper in the recorder chain.
			outer := NewResponseRecorder(&ResponseWriterWrapper{ResponseWriter: inner}, nil, nil)
			outer.WriteHeader(http.StatusSwitchingProtocols)
			if !DetachResponseWriterAfterHijack(outer, detached) {
				t.Fatal("detach configuration failed")
			}
			conn, brw, err := http.NewResponseController(outer).Hijack()
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			const payload = "stream payload"
			if _, err := conn.Write([]byte(payload)); err != nil {
				t.Fatal(err)
			}
			if _, err := brw.WriteString(payload); err != nil {
				t.Fatal(err)
			}
			if err := brw.Flush(); err != nil {
				t.Fatal(err)
			}
			want := 2 * len(payload)
			if detached {
				want = 0
			}
			for name, rr := range map[string]ResponseRecorder{"inner": inner, "outer": outer} {
				if rr.Size() != want {
					t.Errorf("%s recorder size = %d, want %d", name, rr.Size(), want)
				}
			}
			if DetachResponseWriterAfterHijack(outer, true) {
				t.Error("detach should fail after hijack")
			}
		})
	}
}

// An explicit terminal may promise that its hijacked connection has no state
// tied to the middleware invocation. Unknown opaque terminals cannot do so.
type detachTerminalWriter struct {
	http.ResponseWriter
	accept bool
}

func (w detachTerminalWriter) DetachAfterHijack(bool) bool { return w.accept }

type cyclicDetachWriter struct {
	http.ResponseWriter
	next http.ResponseWriter
}

func (w *cyclicDetachWriter) Unwrap() http.ResponseWriter { return w.next }

// A value writer with a slice field reproduces the interface comparison panic
// of comparing an Unwrap result against the writer itself.
type nonComparableDetachWriter struct {
	http.ResponseWriter
	values []int
}

func (w nonComparableDetachWriter) Unwrap() http.ResponseWriter { return w }

func TestDetachResponseWriterSafety(t *testing.T) {
	terminal := newHijackRespWriter()
	native := wrapNativeResponseWriter(terminal)
	acceptedOuter := func(inner http.ResponseWriter) http.ResponseWriter { return NewResponseRecorder(inner, nil, nil) }
	self := &cyclicDetachWriter{ResponseWriter: terminal}
	self.next = self
	first := &cyclicDetachWriter{ResponseWriter: terminal}
	second := &cyclicDetachWriter{ResponseWriter: terminal, next: first}
	first.next = second
	deep := native
	for range 256 {
		deep = &ResponseWriterWrapper{ResponseWriter: deep}
	}
	for _, tc := range []struct {
		name   string
		writer http.ResponseWriter
		want   bool
	}{
		{"nil", nil, false},
		{"nil native", wrapNativeResponseWriter(nil), false},
		{"typed nil native", (*nativeResponseWriter)(nil), false},
		{"zero native", &nativeResponseWriter{}, false},
		{"unknown terminal", terminal, false},
		{"transparent unknown", &ResponseWriterWrapper{ResponseWriter: terminal}, false},
		{"native terminal", native, true},
		{"transparent native", &ResponseWriterWrapper{ResponseWriter: native}, true},
		{"idle native", &IdleTimeoutWriter{ResponseWriterWrapper: &ResponseWriterWrapper{ResponseWriter: native}}, true},
		{"accepted outer native", acceptedOuter(native), true},
		{"opaque inner after accepted outer", acceptedOuter(terminal), false},
		{"nil inner after accepted outer", acceptedOuter(nil), false},
		{"explicit safe terminal", detachTerminalWriter{terminal, true}, true},
		{"explicit rejecting terminal", acceptedOuter(detachTerminalWriter{terminal, false}), false},
		{"explicit reject before native", acceptedOuter(detachTerminalWriter{native, false}), false},
		{"self cycle", acceptedOuter(self), false},
		{"two writer cycle", acceptedOuter(first), false},
		{"non comparable cycle", acceptedOuter(nonComparableDetachWriter{terminal, []int{1}}), false},
		{"excessive depth", deep, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := DetachResponseWriterAfterHijack(tc.writer, true); got != tc.want {
				t.Fatalf("detach=%v, want %v", got, tc.want)
			}
		})
	}
}

func TestNativeResponseWriterForwardsTransportOperations(t *testing.T) {
	underlying := newHijackRespWriter()
	wrapped := wrapNativeResponseWriter(underlying)
	wrapped.Header().Set("X-Test", "forwarded")
	if underlying.Header().Get("X-Test") != "forwarded" {
		t.Fatal("header mutation not forwarded")
	}
	wrapped.WriteHeader(http.StatusSwitchingProtocols)
	if underlying.status != http.StatusSwitchingProtocols {
		t.Fatal("status not forwarded")
	}
	conn, _, err := http.NewResponseController(wrapped).Hijack()
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte("detached")); err != nil {
		t.Fatal(err)
	}
}

func TestNativeResponseWriterHTTP1Capabilities(t *testing.T) {
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseFlush := func() { releaseOnce.Do(func() { close(release) }) }
	notifications := make(chan (<-chan bool), 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w = wrapNativeResponseWriter(w)
		hijacker, hijackOK := w.(http.Hijacker)
		flusher, flushOK := w.(http.Flusher)
		readerFrom, readFromOK := w.(io.ReaderFrom)
		// This structural assertion is the legacy http.CloseNotifier interface.
		notifier, notifyOK := w.(interface{ CloseNotify() <-chan bool })
		if !hijackOK || !flushOK || !readFromOK || !notifyOK {
			t.Errorf("native H1 capabilities: hijack=%v flush=%v readerFrom=%v closeNotify=%v", hijackOK, flushOK, readFromOK, notifyOK)
			http.Error(w, "missing H1 capabilities", 500)
			return
		}
		if r.URL.Path == "/hijack" {
			conn, brw, err := hijacker.Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			defer conn.Close()
			if _, err := brw.WriteString("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: test\r\n\r\nnative hijack"); err != nil {
				t.Error(err)
				return
			}
			if err := brw.Flush(); err != nil {
				t.Error(err)
			}
			return
		}
		rc := http.NewResponseController(w)
		deadline := time.Now().Add(5 * time.Second)
		for _, err := range []error{rc.SetReadDeadline(deadline), rc.SetWriteDeadline(deadline), rc.SetReadDeadline(time.Time{}), rc.SetWriteDeadline(time.Time{}), rc.EnableFullDuplex()} {
			if err != nil {
				t.Error(err)
				http.Error(w, "response controller failure", 500)
				return
			}
		}
		notifications <- notifier.CloseNotify()
		n, err := readerFrom.ReadFrom(strings.NewReader("reader-from"))
		if err != nil || n != 11 {
			t.Errorf("ReadFrom()=%d,%v", n, err)
			return
		}
		flusher.Flush()
		// The client must receive bytes while this handler is still active, proving
		// that the direct Flush capability reached the real transport.
		<-release
	}))
	defer server.Close()
	defer releaseFlush()
	client := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{DisableKeepAlives: true}}
	defer client.CloseIdleConnections()
	response, err := client.Get(server.URL + "/flush")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.ProtoMajor != 1 || response.StatusCode != 200 {
		t.Fatalf("response=%s %d", response.Proto, response.StatusCode)
	}
	payload := make([]byte, 11)
	if _, err := io.ReadFull(response.Body, payload); err != nil {
		t.Fatal(err)
	}
	if string(payload) != "reader-from" {
		t.Fatalf("unexpected payload %q", payload)
	}
	var disconnected <-chan bool
	select {
	case disconnected = <-notifications:
	case <-time.After(5 * time.Second):
		t.Fatal("missing CloseNotify channel")
	}
	response.Body.Close()
	select {
	case <-disconnected:
	case <-time.After(5 * time.Second):
		t.Fatal("CloseNotify did not observe client disconnect")
	}
	releaseFlush()
	conn, err := net.DialTimeout("tcp", strings.TrimPrefix(server.URL, "http://"), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := io.WriteString(conn, "GET /hijack HTTP/1.1\r\nHost: localhost\r\nConnection: Upgrade\r\nUpgrade: test\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(conn)
	upgrade, err := http.ReadResponse(reader, &http.Request{Method: http.MethodGet})
	if err != nil {
		t.Fatal(err)
	}
	if upgrade.StatusCode != http.StatusSwitchingProtocols {
		t.Fatalf("hijack status=%d", upgrade.StatusCode)
	}
	hijacked, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if string(hijacked) != "native hijack" {
		t.Fatalf("hijacked payload=%q", hijacked)
	}
}

package caddyhttp

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/quic-go/quic-go/http3"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.uber.org/zap"
	"golang.org/x/net/http2"

	"github.com/caddyserver/caddy/v2"
)

type shutdownTestState struct {
	entered            chan struct{}
	release            chan struct{}
	canceled           chan struct{}
	cleaned            chan struct{}
	hook               chan shutdownHookResult
	addr               chan string
	failures           chan error
	failProvision      bool
	hookRelease        chan struct{}
	ignoreCancellation bool
	cleanupCount       atomic.Int32
	hookCount          atomic.Int32
	accepted           chan net.Conn
	newGate            <-chan struct{}
	server             *Server
	releaseOnce        sync.Once
}
type shutdownHookResult struct {
	err     error
	bounded bool
}

var (
	shutdownTestStates sync.Map
	shutdownTestID     atomic.Uint64
)

type shutdownTestHandler struct {
	ID    string `json:"id"`
	state *shutdownTestState
}

func (shutdownTestHandler) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{ID: "http.handlers.shutdown_lifetime_test", New: func() caddy.Module { return new(shutdownTestHandler) }}
}

func (h *shutdownTestHandler) Provision(ctx caddy.Context) error {
	state, ok := shutdownTestStates.Load(h.ID)
	if !ok {
		return errors.New("missing shutdown test state")
	}
	h.state = state.(*shutdownTestState)
	server := ctx.Value(ServerCtxKey).(*Server)
	h.state.server = server
	server.RegisterConnState(func(conn net.Conn, state http.ConnState) {
		if state == http.StateNew && h.state.accepted != nil {
			h.state.accepted <- conn
			if h.state.newGate != nil {
				<-h.state.newGate
			}
		}
	})
	server.RegisterOnStop(func(ctx context.Context) error {
		h.state.hookCount.Add(1)
		_, bounded := ctx.Deadline()
		h.state.hook <- shutdownHookResult{ctx.Err(), bounded}
		if h.state.hookRelease != nil {
			<-h.state.hookRelease
		}
		return nil
	})
	if h.state.failProvision {
		return errors.New("intentional handler provisioning failure")
	}
	return nil
}

func (h *shutdownTestHandler) ServeHTTP(w http.ResponseWriter, r *http.Request, _ Handler) error {
	close(h.state.entered)
	fmt.Fprintln(w, "before")
	if err := http.NewResponseController(w).Flush(); err != nil {
		return err
	}
	select {
	case <-h.state.release:
	case <-r.Context().Done():
		close(h.state.canceled)
		if h.state.ignoreCancellation {
			<-h.state.release
		}
	}
	if h.state.cleanupCount.Load() != 0 {
		h.state.failures <- errors.New("handler used module after cleanup")
	}
	fmt.Fprintln(w, "after")
	return nil
}

func (h *shutdownTestHandler) Cleanup() error {
	if h.state != nil && h.state.cleanupCount.Add(1) == 1 {
		close(h.state.cleaned)
	}
	return nil
}

func init() {
	caddy.RegisterModule(shutdownTestHandler{})
	caddy.RegisterNetwork("shutdown-lifetime-test", func(ctx context.Context, _, host, port string, _ uint, _ net.ListenConfig) (any, error) {
		value, ok := shutdownTestStates.Load(host)
		if !ok {
			return nil, errors.New("missing listener test state")
		}
		state := value.(*shutdownTestState)
		if port == "2" {
			select {
			case <-state.entered:
				return nil, errors.New("intentional second listener failure")
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err == nil {
			state.addr <- ln.Addr().String()
		}
		return ln, err
	})
}

func newShutdownTestState(t *testing.T) (string, *shutdownTestState) {
	t.Helper()
	id := fmt.Sprintf("test%d", shutdownTestID.Add(1))
	state := &shutdownTestState{entered: make(chan struct{}), release: make(chan struct{}), canceled: make(chan struct{}), cleaned: make(chan struct{}), hook: make(chan shutdownHookResult, 1), addr: make(chan string, 1), failures: make(chan error, 1)}
	shutdownTestStates.Store(id, state)
	t.Cleanup(func() {
		state.releaseOnce.Do(func() { close(state.release) })
		if err := caddy.Stop(); err != nil {
			t.Error(err)
		}
		shutdownTestStates.Delete(id)
	})
	return id, state
}

func shutdownTestConfig(id string, grace time.Duration, failSecond bool, h2 bool) []byte {
	listen := []string{"shutdown-lifetime-test/" + id + ":1"}
	if failSecond {
		listen = append(listen, "shutdown-lifetime-test/"+id+":2")
	}
	protocols := []string{"h1"}
	if h2 {
		protocols = append(protocols, "h2c")
	}
	config := map[string]any{"admin": map[string]any{"disabled": true}, "apps": map[string]any{"http": map[string]any{"grace_period": int64(grace), "servers": map[string]any{"test": map[string]any{"listen": listen, "protocols": protocols, "automatic_https": map[string]any{"disable": true}, "routes": []any{map[string]any{"handle": []any{map[string]any{"handler": "shutdown_lifetime_test", "id": id}}}}}}}}}
	raw, _ := json.Marshal(config)
	return raw
}

func awaitShutdownTest[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for shutdown event")
		var zero T
		return zero
	}
}

func shutdownTestRequest(t *testing.T, addr string, h2 bool) *http.Response {
	t.Helper()
	client := &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{DisableKeepAlives: true}}
	if h2 {
		client.Transport = &http2.Transport{AllowHTTP: true, DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, addr)
		}}
	}
	req, err := http.NewRequestWithContext(t.Context(), "GET", "http://"+addr+"/", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { response.Body.Close(); client.CloseIdleConnections() })
	if h2 && response.ProtoMajor != 2 {
		t.Fatalf("expected HTTP/2, got %s", response.Proto)
	}
	return response
}

func assertShutdownTestRetained(t *testing.T, state *shutdownTestState) {
	t.Helper()
	select {
	case <-state.cleaned:
		t.Fatal("module cleaned up while its handler was active")
	default:
	}
	select {
	case <-state.hook:
		t.Fatal("stop hook ran while its handler was active")
	default:
	}
}

func finishShutdownTest(t *testing.T, state *shutdownTestState) {
	t.Helper()
	state.releaseOnce.Do(func() { close(state.release) })
	hook := awaitShutdownTest(t, state.hook)
	if hook.err != nil || !hook.bounded {
		t.Errorf("stop hook needs fresh bounded context: %+v", hook)
	}
	awaitShutdownTest(t, state.cleaned)
	if state.cleanupCount.Load() != 1 || state.hookCount.Load() != 1 {
		t.Fatal("shutdown finalizer did not run exactly once")
	}
	select {
	case err := <-state.failures:
		t.Error(err)
	default:
	}
}

func TestReloadRetainsActiveHandlerModules(t *testing.T) {
	for _, h2 := range []bool{false, true} {
		t.Run(fmt.Sprintf("http2=%t", h2), func(t *testing.T) {
			id, state := newShutdownTestState(t)
			if err := caddy.Load(shutdownTestConfig(id, 0, false, h2), true); err != nil {
				t.Fatal(err)
			}
			response := shutdownTestRequest(t, awaitShutdownTest(t, state.addr), h2)
			awaitShutdownTest(t, state.entered)
			reloaded := make(chan error, 1)
			go func() { reloaded <- caddy.Load([]byte(`{"admin":{"disabled":true}}`), true) }()
			if err := awaitShutdownTest(t, reloaded); err != nil {
				t.Fatal(err)
			}
			assertShutdownTestRetained(t, state)
			finishShutdownTest(t, state)
			body, err := io.ReadAll(response.Body)
			if err != nil || string(body) != "before\nafter\n" {
				t.Fatalf("response did not drain: %q, %v", body, err)
			}
		})
	}
}

func TestGraceTimeoutClosesConnectionsBeforeModuleCleanup(t *testing.T) {
	for _, h2 := range []bool{false, true} {
		t.Run(fmt.Sprintf("http2=%t", h2), func(t *testing.T) {
			id, state := newShutdownTestState(t)
			state.ignoreCancellation = true
			if err := caddy.Load(shutdownTestConfig(id, 20*time.Millisecond, false, h2), true); err != nil {
				t.Fatal(err)
			}
			response := shutdownTestRequest(t, awaitShutdownTest(t, state.addr), h2)
			if err := caddy.Load([]byte(`{"admin":{"disabled":true}}`), true); err != nil {
				t.Fatal(err)
			}
			awaitShutdownTest(t, state.canceled)
			assertShutdownTestRetained(t, state)
			if _, err := io.ReadAll(response.Body); err == nil {
				t.Fatal("grace timeout did not close client connection")
			}
			finishShutdownTest(t, state)
		})
	}
}

func TestFailedStartRetainsActiveHandlerModules(t *testing.T) {
	id, state := newShutdownTestState(t)
	loaded := make(chan error, 1)
	go func() { loaded <- caddy.Load(shutdownTestConfig(id, 0, true, false), true) }()
	response := shutdownTestRequest(t, awaitShutdownTest(t, state.addr), false)
	if err := awaitShutdownTest(t, loaded); err == nil {
		t.Fatal("expected second listener startup failure")
	}
	assertShutdownTestRetained(t, state)
	finishShutdownTest(t, state)
	if _, err := io.ReadAll(response.Body); err != nil {
		t.Fatal(err)
	}
}

func TestStopFinalizesOnce(t *testing.T) {
	hooks := atomic.Int32{}
	app := &App{stopOnce: new(sync.Once), logger: zap.NewNop(), Servers: map[string]*Server{"test": {}}}
	app.Servers["test"].RegisterOnStop(func(ctx context.Context) error { hooks.Add(1); return ctx.Err() })
	var calls sync.WaitGroup
	for range 8 {
		calls.Go(func() {
			if err := app.stop(false); err != nil {
				t.Error(err)
			}
		})
	}
	calls.Wait()
	awaitShutdownTest(t, app.shutdownDone)
	if err := app.Cleanup(); err != nil {
		t.Fatal(err)
	}
	if hooks.Load() != 1 {
		t.Fatalf("stop hook ran %d times", hooks.Load())
	}
}

func TestSealedServerRejectsLateDispatch(t *testing.T) {
	server := new(Server)
	server.finishRequests()
	recorder := httptest.NewRecorder()
	// A zero-value server would panic if this request touched provisioned state.
	server.ServeHTTP(recorder, httptest.NewRequest("GET", "/", nil))
	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("late dispatch status: %d", recorder.Code)
	}
}

type shutdownContextReader struct {
	*sdkmetric.ManualReader
	flush    chan shutdownHookResult
	shutdown chan shutdownHookResult
}

func (r *shutdownContextReader) ForceFlush(ctx context.Context) error {
	_, bounded := ctx.Deadline()
	r.flush <- shutdownHookResult{ctx.Err(), bounded}
	return nil
}

func (r *shutdownContextReader) Shutdown(ctx context.Context) error {
	_, bounded := ctx.Deadline()
	r.shutdown <- shutdownHookResult{ctx.Err(), bounded}
	return r.ManualReader.Shutdown(ctx)
}

func TestStopUsesFreshMetricsContextAfterGraceTimeout(t *testing.T) {
	app, response, _ := appWithPendingResponse(t, false)
	app.GracePeriod = caddy.Duration(20 * time.Millisecond)
	reader := &shutdownContextReader{ManualReader: sdkmetric.NewManualReader(), flush: make(chan shutdownHookResult, 1), shutdown: make(chan shutdownHookResult, 1)}
	app.Metrics = &Metrics{meterProvider: sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))}
	if err := app.stop(false); err != nil {
		t.Fatal(err)
	}
	awaitShutdownTest(t, app.shutdownDone)
	for _, event := range []shutdownHookResult{awaitShutdownTest(t, reader.flush), awaitShutdownTest(t, reader.shutdown)} {
		if event.err != nil || !event.bounded {
			t.Errorf("metrics needs fresh bounded context: %+v", event)
		}
	}
	if _, err := io.ReadAll(response.Body); err == nil {
		t.Fatal("grace timeout did not close the connection")
	}
}

// HTTP/3 uses its own transport and shutdown implementation; exercise both a
// normal drain and grace expiry through the real HTTP app configuration.
func TestHTTP3ReloadRetainsActiveHandlerModules(t *testing.T) {
	for _, timeout := range []bool{false, true} {
		t.Run(fmt.Sprintf("timeout=%t", timeout), func(t *testing.T) {
			id, state := newShutdownTestState(t)
			state.ignoreCancellation = timeout
			grace := time.Duration(0)
			if timeout {
				grace = 20 * time.Millisecond
			}
			var config map[string]any
			if err := json.Unmarshal(shutdownTestConfig(id, grace, false, false), &config); err != nil {
				t.Fatal(err)
			}
			apps := config["apps"].(map[string]any)
			server := apps["http"].(map[string]any)["servers"].(map[string]any)["test"].(map[string]any)
			server["listen"] = []string{"127.0.0.1:0"}
			server["protocols"] = []string{"h3"}
			server["tls_connection_policies"] = []any{map[string]any{}}
			certServer := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
			certificate := certServer.TLS.Certificates[0]
			certServer.Close()
			key, err := x509.MarshalPKCS8PrivateKey(certificate.PrivateKey)
			if err != nil {
				t.Fatal(err)
			}
			apps["tls"] = map[string]any{"certificates": map[string]any{"load_pem": []any{map[string]any{"certificate": string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate.Certificate[0]})), "key": string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: key}))}}}}
			raw, err := json.Marshal(config)
			if err != nil {
				t.Fatal(err)
			}
			if err := caddy.Load(raw, true); err != nil {
				t.Fatal(err)
			}
			transport := &http3.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true, ServerName: "example.com"}}
			t.Cleanup(func() { transport.Close() })
			client := &http.Client{Transport: transport, Timeout: 10 * time.Second}
			request, err := http.NewRequestWithContext(t.Context(), "GET", "https://"+state.server.quicListeners[0].Addr().String()+"/", nil)
			if err != nil {
				t.Fatal(err)
			}
			response, err := client.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { response.Body.Close() })
			if response.ProtoMajor != 3 {
				t.Fatalf("expected HTTP/3, got %s", response.Proto)
			}
			if err := caddy.Load([]byte(`{"admin":{"disabled":true}}`), true); err != nil {
				t.Fatal(err)
			}
			if timeout {
				awaitShutdownTest(t, state.canceled)
			}
			assertShutdownTestRetained(t, state)
			if timeout {
				if _, err := io.ReadAll(response.Body); err == nil {
					t.Fatal("grace expiry did not close HTTP/3 stream")
				}
			} else {
				state.releaseOnce.Do(func() { close(state.release) })
				body, err := io.ReadAll(response.Body)
				response.Body.Close()
				if err != nil || string(body) != "before\nafter\n" {
					t.Fatalf("HTTP/3 response did not drain: %q, %v", body, err)
				}
			}
			finishShutdownTest(t, state)
		})
	}
}

func TestUnstartedConfigWaitsForStopHooks(t *testing.T) {
	for _, failProvision := range []bool{false, true} {
		t.Run(fmt.Sprintf("provision_failure=%t", failProvision), func(t *testing.T) {
			id, state := newShutdownTestState(t)
			state.failProvision = failProvision
			state.hookRelease = make(chan struct{})
			releaseHook := sync.OnceFunc(func() { close(state.hookRelease) })
			t.Cleanup(releaseHook)
			finished := make(chan error, 1)
			raw := shutdownTestConfig(id, 0, false, false)
			go func() {
				if failProvision {
					finished <- caddy.Load(raw, true)
					return
				}
				var config caddy.Config
				if err := json.Unmarshal(raw, &config); err != nil {
					finished <- err
					return
				}
				finished <- caddy.Validate(&config)
			}()
			hook := awaitShutdownTest(t, state.hook)
			if hook.err != nil || !hook.bounded {
				t.Errorf("unstarted config hook context: %+v", hook)
			}
			select {
			case err := <-finished:
				t.Fatalf("unstarted configuration returned before stop hook finished: %v", err)
			default:
			}
			releaseHook()
			err := awaitShutdownTest(t, finished)
			if failProvision && err == nil {
				t.Fatal("expected handler provisioning error")
			}
			if !failProvision && err != nil {
				t.Fatal(err)
			}
			if state.hookCount.Load() != 1 {
				t.Fatalf("stop hook ran %d times", state.hookCount.Load())
			}
		})
	}
}

func TestCleanupFinalizesMetricsWithoutStop(t *testing.T) {
	reader := &shutdownContextReader{ManualReader: sdkmetric.NewManualReader(), flush: make(chan shutdownHookResult, 1), shutdown: make(chan shutdownHookResult, 1)}
	app := &App{stopOnce: new(sync.Once), logger: zap.NewNop(), Metrics: &Metrics{meterProvider: sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))}}
	if err := app.Cleanup(); err != nil {
		t.Fatal(err)
	}
	// Cleanup must have waited for both exporter operations, even without Stop.
	for _, events := range []chan shutdownHookResult{reader.flush, reader.shutdown} {
		select {
		case event := <-events:
			if event.err != nil || !event.bounded {
				t.Errorf("cleanup metrics context: %+v", event)
			}
		default:
			t.Fatal("Cleanup returned before metrics finalization")
		}
	}
}

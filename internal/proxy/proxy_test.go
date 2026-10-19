package proxy

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/asmahdy1420-cmyk/switchyard/internal/config"
	"github.com/asmahdy1420-cmyk/switchyard/internal/metrics"
)

// backend is an httptest server that answers with its own name and counts
// the requests it received.
type backend struct {
	*httptest.Server
	name   string
	hits   atomic.Int64
	status atomic.Int64
	last   atomic.Pointer[http.Request]
	body   atomic.Pointer[string]
}

func newBackend(t *testing.T, name string) *backend {
	t.Helper()
	b := &backend{name: name}
	b.status.Store(http.StatusOK)
	b.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b.hits.Add(1)
		data, _ := io.ReadAll(r.Body)
		s := string(data)
		b.body.Store(&s)
		b.last.Store(r.Clone(r.Context()))
		w.Header().Set("X-Backend", name)
		w.WriteHeader(int(b.status.Load()))
		_, _ = io.WriteString(w, name)
	}))
	t.Cleanup(b.Close)
	return b
}

func mustConfig(t *testing.T, yaml string) *config.Config {
	t.Helper()
	cfg, err := config.Parse([]byte(yaml))
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func newHandler(t *testing.T, cfg *config.Config) (*Handler, *metrics.Registry) {
	t.Helper()
	reg := metrics.NewRegistry()
	h, err := New(cfg, Options{Registry: reg})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(h.Close)
	return h, reg
}

func do(t *testing.T, h http.Handler, method, target string, body io.Reader, hdr ...string) *http.Response {
	t.Helper()
	req := httptest.NewRequest(method, target, body)
	req.RemoteAddr = "198.51.100.9:40000"
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec.Result()
}

func bodyOf(t *testing.T, resp *http.Response) string {
	t.Helper()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

func TestRoutesByHostAndLongestPrefix(t *testing.T) {
	api, admin, web := newBackend(t, "api"), newBackend(t, "admin"), newBackend(t, "web")
	h, _ := newHandler(t, mustConfig(t, fmt.Sprintf(`
pools:
  api:   {upstreams: [{url: %q}]}
  admin: {upstreams: [{url: %q}]}
  web:   {upstreams: [{url: %q}]}
routes:
  - {name: web, pool: web}
  - {name: api, host: api.example.com, path_prefix: /v1, pool: api}
  - {name: admin, host: api.example.com, path_prefix: /v1/admin, pool: admin, strip_prefix: true}
`, api.URL, admin.URL, web.URL)))

	cases := []struct{ target, want string }{
		{"http://api.example.com/v1/users", "api"},
		{"http://api.example.com:8080/v1", "api"},
		{"http://api.example.com/v1/admin/keys", "admin"},
		{"http://api.example.com/v1admin", "web"}, // prefix match is segment aware
		{"http://other.example.com/v1/users", "web"},
	}
	for _, tc := range cases {
		if got := bodyOf(t, do(t, h, "GET", tc.target, nil)); got != tc.want {
			t.Errorf("%s routed to %q, want %q", tc.target, got, tc.want)
		}
	}
	if p := admin.last.Load().URL.Path; p != "/keys" {
		t.Errorf("strip_prefix sent %q upstream, want /keys", p)
	}
}

func TestNoRouteReturns404(t *testing.T) {
	b := newBackend(t, "a")
	h, _ := newHandler(t, mustConfig(t, fmt.Sprintf(`
pools: {a: {upstreams: [{url: %q}]}}
routes: [{pool: a, host: only.example.com}]
`, b.URL)))
	if resp := do(t, h, "GET", "http://elsewhere.example.com/", nil); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status %d, want 404", resp.StatusCode)
	}
}

func TestRetriesIdempotentRequestOnAnotherUpstream(t *testing.T) {
	bad, good := newBackend(t, "bad"), newBackend(t, "good")
	bad.status.Store(http.StatusBadGateway)
	h, reg := newHandler(t, mustConfig(t, fmt.Sprintf(`
pools: {p: {strategy: round_robin, passive: {max_failures: 0}, upstreams: [{url: %q}, {url: %q}]}}
routes: [{name: r, pool: p, retries: 1}]
`, bad.URL, good.URL)))

	for i := 0; i < 4; i++ {
		resp := do(t, h, "GET", "http://x/", nil)
		if resp.StatusCode != http.StatusOK || bodyOf(t, resp) != "good" {
			t.Fatalf("request %d: status %d, want 200 from good", i, resp.StatusCode)
		}
	}
	if bad.hits.Load() == 0 {
		t.Fatal("round robin never tried the failing upstream; the retry path was not exercised")
	}
	var out strings.Builder
	_, _ = reg.WriteTo(&out)
	if !strings.Contains(out.String(), fmt.Sprintf(`switchyard_retries_total{route="r"} %d`, bad.hits.Load())) {
		t.Fatalf("retry counter missing or wrong:\n%s", out.String())
	}
}

func TestRetryReplaysBufferedBody(t *testing.T) {
	bad, good := newBackend(t, "bad"), newBackend(t, "good")
	bad.Close() // connection refused
	h, _ := newHandler(t, mustConfig(t, fmt.Sprintf(`
pools: {p: {strategy: weighted, passive: {max_failures: 0}, upstreams: [{url: %q, weight: 100}, {url: %q}]}}
routes: [{pool: p, retries: 2}]
`, bad.URL, good.URL)))

	resp := do(t, h, "PUT", "http://x/item/1", strings.NewReader(`{"qty":3}`))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d, want 200", resp.StatusCode)
	}
	if got := *good.body.Load(); got != `{"qty":3}` {
		t.Fatalf("retried body = %q", got)
	}
}

func TestNonIdempotentRequestIsNotRetried(t *testing.T) {
	bad, good := newBackend(t, "bad"), newBackend(t, "good")
	bad.status.Store(http.StatusServiceUnavailable)
	h, _ := newHandler(t, mustConfig(t, fmt.Sprintf(`
pools: {p: {strategy: weighted, passive: {max_failures: 0}, upstreams: [{url: %q, weight: 100}, {url: %q}]}}
routes: [{pool: p, retries: 3}]
`, bad.URL, good.URL)))

	resp := do(t, h, "POST", "http://x/orders", strings.NewReader("{}"))
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status %d, want the upstream's 503 passed through", resp.StatusCode)
	}
	if good.hits.Load() != 0 {
		t.Fatal("POST was retried on another upstream")
	}
}

func TestPassiveEjectionRemovesFailingUpstream(t *testing.T) {
	bad, good := newBackend(t, "bad"), newBackend(t, "good")
	bad.status.Store(http.StatusInternalServerError)
	h, reg := newHandler(t, mustConfig(t, fmt.Sprintf(`
pools: {p: {passive: {max_failures: 2, ejection_time: 1m}, upstreams: [{url: %q}, {url: %q}]}}
routes: [{pool: p, retries: 0}]
`, bad.URL, good.URL)))

	for i := 0; i < 20; i++ {
		do(t, h, "GET", "http://x/", nil)
	}
	if n := bad.hits.Load(); n != 2 {
		t.Fatalf("failing upstream received %d requests, want exactly 2 before ejection", n)
	}
	var out strings.Builder
	_, _ = reg.WriteTo(&out)
	host := strings.TrimPrefix(bad.URL, "http://")
	for _, want := range []string{
		fmt.Sprintf(`switchyard_upstream_ejections_total{pool="p",upstream=%q} 1`, host),
		fmt.Sprintf(`switchyard_upstream_up{pool="p",upstream=%q} 0`, host),
	} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("metrics missing %s\n%s", want, out.String())
		}
	}
}

func TestAllUpstreamsDownReturns503(t *testing.T) {
	b := newBackend(t, "a")
	b.status.Store(http.StatusInternalServerError)
	h, _ := newHandler(t, mustConfig(t, fmt.Sprintf(`
pools: {p: {passive: {max_failures: 1, ejection_time: 1m}, upstreams: [{url: %q}]}}
routes: [{pool: p, retries: 0}]
`, b.URL)))
	do(t, h, "GET", "http://x/", nil)
	if resp := do(t, h, "GET", "http://x/", nil); resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status %d, want 503 when every upstream is ejected", resp.StatusCode)
	}
}

func TestRouteTimeoutReturns504(t *testing.T) {
	release := make(chan struct{})
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(slow.Close)
	t.Cleanup(func() { close(release) })
	h, _ := newHandler(t, mustConfig(t, fmt.Sprintf(`
pools: {p: {upstreams: [{url: %q}]}}
routes: [{pool: p, timeout: 50ms}]
`, slow.URL)))

	start := time.Now()
	resp := do(t, h, "GET", "http://x/", nil)
	if resp.StatusCode != http.StatusGatewayTimeout {
		t.Fatalf("status %d, want 504", resp.StatusCode)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("request took %v despite a 50ms route timeout", elapsed)
	}
}

func TestForwardedHeadersAndRequestID(t *testing.T) {
	b := newBackend(t, "a")
	h, _ := newHandler(t, mustConfig(t, fmt.Sprintf(`
pools: {p: {upstreams: [{url: %q}]}}
routes: [{pool: p}]
`, b.URL)))

	resp := do(t, h, "GET", "http://shop.example.com/cart", nil,
		"X-Forwarded-For", "6.6.6.6", "X-Request-ID", "abc-123")
	got := b.last.Load()
	if v := got.Header.Get("X-Forwarded-For"); v != "198.51.100.9" {
		t.Errorf("X-Forwarded-For = %q, want only the real client IP", v)
	}
	if v := got.Header.Get("X-Forwarded-Host"); v != "shop.example.com" {
		t.Errorf("X-Forwarded-Host = %q", v)
	}
	if v := got.Header.Get("X-Forwarded-Proto"); v != "http" {
		t.Errorf("X-Forwarded-Proto = %q", v)
	}
	if got.Host != strings.TrimPrefix(b.URL, "http://") {
		t.Errorf("upstream saw Host %q, want the upstream's host", got.Host)
	}
	if v := got.Header.Get("X-Request-ID"); v != "abc-123" {
		t.Errorf("upstream X-Request-ID = %q, want the caller's id", v)
	}
	if v := resp.Header.Get("X-Request-ID"); v != "abc-123" {
		t.Errorf("response X-Request-ID = %q", v)
	}

	resp = do(t, h, "GET", "http://shop.example.com/", nil, "X-Request-ID", "bad id with spaces")
	if id := resp.Header.Get("X-Request-ID"); len(id) != 32 {
		t.Errorf("invalid inbound id should be replaced by a fresh random id, got %q", id)
	}
}

func TestReloadSwapsConfig(t *testing.T) {
	blue, green := newBackend(t, "blue"), newBackend(t, "green")
	h, reg := newHandler(t, mustConfig(t, fmt.Sprintf(`
pools: {p: {upstreams: [{url: %q}]}}
routes: [{pool: p}]
`, blue.URL)))
	if got := bodyOf(t, do(t, h, "GET", "http://x/", nil)); got != "blue" {
		t.Fatalf("before reload: %q", got)
	}

	if err := h.Reload(mustConfig(t, fmt.Sprintf(`
pools: {p: {upstreams: [{url: %q}]}}
routes: [{pool: p}]
`, green.URL))); err != nil {
		t.Fatal(err)
	}
	if got := bodyOf(t, do(t, h, "GET", "http://x/", nil)); got != "green" {
		t.Fatalf("after reload: %q", got)
	}
	var out strings.Builder
	_, _ = reg.WriteTo(&out)
	if !strings.Contains(out.String(), `switchyard_config_reloads_total{result="success"} 1`) {
		t.Fatalf("reload not counted:\n%s", out.String())
	}
}

func TestReloadUnderConcurrentTraffic(t *testing.T) {
	blue, green := newBackend(t, "blue"), newBackend(t, "green")
	cfgFor := func(b *backend) *config.Config {
		return mustConfig(t, fmt.Sprintf("pools: {p: {upstreams: [{url: %q}]}}\nroutes: [{pool: p}]\n", b.URL))
	}
	h, _ := newHandler(t, cfgFor(blue))
	front := httptest.NewServer(h)
	defer front.Close()

	stop := make(chan struct{})
	errs := make(chan error, 1)
	go func() {
		defer close(errs)
		for {
			select {
			case <-stop:
				return
			default:
			}
			resp, err := http.Get(front.URL)
			if err != nil {
				errs <- err
				return
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				errs <- fmt.Errorf("status %d during reload", resp.StatusCode)
				return
			}
		}
	}()
	for i := 0; i < 20; i++ {
		next := blue
		if i%2 == 0 {
			next = green
		}
		if err := h.Reload(cfgFor(next)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Millisecond)
	}
	close(stop)
	if err := <-errs; err != nil {
		t.Fatal(err)
	}
}

func TestReloadKeepsHealthVerdict(t *testing.T) {
	b := newBackend(t, "a")
	cfg := mustConfig(t, fmt.Sprintf(`
pools: {p: {health_check: {interval: 1h}, upstreams: [{url: %q}]}}
routes: [{pool: p}]
`, b.URL))
	h, _ := newHandler(t, cfg)
	h.current.Load().pools["p"].Upstreams[0].SetHealthy(false)
	if err := h.Reload(cfg); err != nil {
		t.Fatal(err)
	}
	if h.current.Load().pools["p"].Upstreams[0].Healthy() {
		t.Fatal("reload put an unhealthy upstream back into rotation")
	}
}

func TestLeastConnectionsThroughProxy(t *testing.T) {
	release := make(chan struct{})
	var slowHits atomic.Int64
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		slowHits.Add(1)
		<-release
		_, _ = io.WriteString(w, "slow")
	}))
	t.Cleanup(slow.Close)
	fast := newBackend(t, "fast")
	h, _ := newHandler(t, mustConfig(t, fmt.Sprintf(`
pools: {p: {strategy: least_connections, upstreams: [{url: %q}, {url: %q}]}}
routes: [{pool: p}]
`, slow.URL, fast.URL)))

	// Park one request on the slow upstream.
	done := make(chan struct{})
	go func() {
		defer close(done)
		for slowHits.Load() == 0 {
			do(t, h, "GET", "http://x/", nil)
		}
	}()
	deadline := time.Now().Add(3 * time.Second)
	for slowHits.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	before := fast.hits.Load()
	for i := 0; i < 10; i++ {
		if got := bodyOf(t, do(t, h, "GET", "http://x/", nil)); got != "fast" {
			t.Fatalf("request %d went to %q while slow upstream was busy", i, got)
		}
	}
	if fast.hits.Load()-before != 10 {
		t.Fatal("fast upstream did not receive all requests")
	}
	close(release)
	<-done
}

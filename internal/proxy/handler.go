// Package proxy wires routing, balancing, retries, logging and metrics into
// an http.Handler whose configuration can be swapped atomically.
package proxy

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/asmahdy1420-cmyk/switchyard/internal/config"
	"github.com/asmahdy1420-cmyk/switchyard/internal/metrics"
)

// StatusClientClosed is logged when the client goes away before a response
// could be written (the nginx convention).
const StatusClientClosed = 499

// Options configures a Handler.
type Options struct {
	Logger   *slog.Logger
	Registry *metrics.Registry
}

// Handler is the proxy's http.Handler. The active configuration lives
// behind an atomic.Pointer: reload builds a complete new runtime and swaps
// it in, so requests never observe a half-applied config and the listener
// is never closed.
type Handler struct {
	current  atomic.Pointer[runtime]
	reloadMu sync.Mutex
	log      *slog.Logger
	m        *proxyMetrics
}

// New builds a Handler from cfg and starts its health checkers.
func New(cfg *config.Config, opts Options) (*Handler, error) {
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.DiscardHandler)
	}
	if opts.Registry == nil {
		opts.Registry = metrics.NewRegistry()
	}
	h := &Handler{log: opts.Logger}
	h.m = newProxyMetrics(opts.Registry, h)
	rt, err := buildRuntime(cfg, h)
	if err != nil {
		return nil, err
	}
	rt.start()
	h.current.Store(rt)
	return h, nil
}

// Config returns the configuration currently being served.
func (h *Handler) Config() *config.Config { return h.current.Load().cfg }

// Reload builds a runtime from cfg and swaps it in. Upstreams that exist in
// both configs keep their active health verdict, so a known-bad backend is
// not put back into rotation just because the file was reloaded. On error
// the running configuration is left untouched.
func (h *Handler) Reload(cfg *config.Config) error {
	h.reloadMu.Lock()
	defer h.reloadMu.Unlock()

	next, err := buildRuntime(cfg, h)
	if err != nil {
		h.m.reloads.With("failure").Inc()
		return err
	}
	old := h.current.Load()
	carryHealth(old, next)
	next.start()
	h.current.Store(next)
	old.stop()
	h.m.reloads.With("success").Inc()
	return nil
}

// Close stops health checking and drops idle upstream connections.
func (h *Handler) Close() {
	if rt := h.current.Load(); rt != nil {
		rt.stop()
	}
}

func carryHealth(old, next *runtime) {
	for name, np := range next.pools {
		op, ok := old.pools[name]
		if !ok {
			continue
		}
		prev := map[string]bool{}
		for _, u := range op.Upstreams {
			prev[u.URL.String()] = u.Healthy()
		}
		for _, u := range np.Upstreams {
			if healthy, ok := prev[u.URL.String()]; ok {
				u.SetHealthy(healthy)
			}
		}
	}
}

type stateKey struct{}

// reqState travels in the request context from the handler to the
// transport and back, carrying what the access log needs.
type reqState struct {
	id       string
	route    *Route
	upstream string
	attempts int
}

func stateFrom(ctx context.Context) *reqState {
	st, _ := ctx.Value(stateKey{}).(*reqState)
	return st
}

// ServeHTTP implements http.Handler.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	rt := h.current.Load()

	id := requestID(r.Header.Get("X-Request-ID"))
	w.Header().Set("X-Request-ID", id)
	rec := &recorder{ResponseWriter: w}

	route := rt.match(r)
	st := &reqState{id: id, route: route}
	routeName := ""
	if route == nil {
		http.Error(rec, "no route", http.StatusNotFound)
	} else {
		routeName = route.Name
		ctx := r.Context()
		if route.Timeout > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, route.Timeout)
			defer cancel()
		}
		r = r.WithContext(context.WithValue(ctx, stateKey{}, st))
		h.bufferForRetry(r, route, rt.cfg.MaxRetryBody)
		rt.proxy.ServeHTTP(rec, r)
	}

	elapsed := time.Since(start)
	status := rec.statusCode()
	h.m.requests.With(routeName, methodLabel(r.Method), strconv.Itoa(status)).Inc()
	h.m.duration.With(routeName).Observe(elapsed.Seconds())

	level := slog.LevelInfo
	if status >= 500 {
		level = slog.LevelWarn
	}
	h.log.LogAttrs(r.Context(), level, "request",
		slog.String("request_id", id),
		slog.String("route", routeName),
		slog.String("method", r.Method),
		slog.String("host", r.Host),
		slog.String("path", r.URL.Path),
		slog.Int("status", status),
		slog.Int64("bytes", rec.bytes),
		slog.Float64("duration_ms", float64(elapsed.Microseconds())/1000),
		slog.String("upstream", st.upstream),
		slog.Int("attempts", st.attempts),
		slog.String("remote_addr", r.RemoteAddr),
	)
}

// bufferForRetry reads a small request body into memory so that a retry
// can replay it. Bodies that are too large or of unknown length are
// streamed as usual and the request is simply not retried.
func (h *Handler) bufferForRetry(r *http.Request, route *Route, limit int64) {
	if route.Retries == 0 || !idempotent(r.Method) || r.Body == nil || r.Body == http.NoBody {
		return
	}
	if r.ContentLength <= 0 || r.ContentLength > limit {
		return
	}
	data, err := io.ReadAll(io.LimitReader(r.Body, r.ContentLength))
	_ = r.Body.Close()
	if err != nil {
		// Hand the transport a body that fails the same way.
		r.Body = io.NopCloser(&errReader{err: err})
		return
	}
	r.Body = io.NopCloser(bytes.NewReader(data))
	r.GetBody = func() (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(data)), nil
	}
}

type errReader struct{ err error }

func (e *errReader) Read([]byte) (int, error) { return 0, e.err }

// rewrite shapes the outbound request. The upstream host itself is chosen
// per attempt by retryTransport.
func rewrite(pr *httputil.ProxyRequest) {
	// Inbound X-Forwarded-* headers were already removed by ReverseProxy;
	// switchyard is treated as the edge and does not trust client values.
	pr.SetXForwarded()
	st := stateFrom(pr.In.Context())
	if st == nil || st.route == nil {
		return
	}
	pr.Out.Header.Set("X-Request-ID", st.id)
	if !st.route.PreserveHost {
		pr.Out.Host = ""
	}
	if st.route.StripPrefix && st.route.PathPrefix != "/" {
		p := strings.TrimPrefix(pr.Out.URL.Path, strings.TrimSuffix(st.route.PathPrefix, "/"))
		if !strings.HasPrefix(p, "/") {
			p = "/" + p
		}
		pr.Out.URL.Path = p
		pr.Out.URL.RawPath = ""
	}
}

func (h *Handler) proxyError(w http.ResponseWriter, r *http.Request, err error) {
	status := http.StatusBadGateway
	switch {
	case errors.Is(err, errNoUpstream):
		status = http.StatusServiceUnavailable
	case errors.Is(r.Context().Err(), context.DeadlineExceeded) || errors.Is(err, context.DeadlineExceeded):
		status = http.StatusGatewayTimeout
	case errors.Is(r.Context().Err(), context.Canceled):
		status = StatusClientClosed
	}
	if st := stateFrom(r.Context()); st != nil {
		h.log.Warn("proxy error", "request_id", st.id, "upstream", st.upstream, "error", err)
	}
	w.WriteHeader(status)
}

func requestID(incoming string) string {
	if n := len(incoming); n > 0 && n <= 128 && validID(incoming) {
		return incoming
	}
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func validID(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == '.' || c == ':') {
			return false
		}
	}
	return true
}

func idempotent(method string) bool {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodOptions, http.MethodTrace, http.MethodPut, http.MethodDelete:
		return true
	}
	return false
}

func methodLabel(m string) string {
	switch m {
	case http.MethodGet, http.MethodHead, http.MethodPost, http.MethodPut, http.MethodPatch,
		http.MethodDelete, http.MethodOptions, http.MethodConnect, http.MethodTrace:
		return m
	}
	return "OTHER"
}

// recorder captures status and size for logs and metrics. Unwrap lets
// http.ResponseController reach Flush and Hijack on the real writer.
type recorder struct {
	http.ResponseWriter
	status int
	bytes  int64
}

func (r *recorder) WriteHeader(code int) {
	if r.status == 0 && (code >= 200 || code == http.StatusSwitchingProtocols) {
		r.status = code
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *recorder) Write(b []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	n, err := r.ResponseWriter.Write(b)
	r.bytes += int64(n)
	return n, err
}

func (r *recorder) Unwrap() http.ResponseWriter { return r.ResponseWriter }

func (r *recorder) statusCode() int {
	if r.status == 0 {
		return http.StatusOK
	}
	return r.status
}

type proxyMetrics struct {
	requests  *metrics.CounterVec
	duration  *metrics.HistogramVec
	upstream  *metrics.CounterVec
	retries   *metrics.CounterVec
	ejections *metrics.CounterVec
	reloads   *metrics.CounterVec
}

func newProxyMetrics(reg *metrics.Registry, h *Handler) *proxyMetrics {
	m := &proxyMetrics{
		requests: reg.NewCounterVec("switchyard_http_requests_total",
			"Requests handled, by route, method and status code.", "route", "method", "code"),
		duration: reg.NewHistogramVec("switchyard_http_request_duration_seconds",
			"End-to-end request latency including retries.", nil, "route"),
		upstream: reg.NewCounterVec("switchyard_upstream_responses_total",
			"Upstream attempts by outcome class (2xx..5xx or error).", "pool", "upstream", "class"),
		retries: reg.NewCounterVec("switchyard_retries_total",
			"Requests re-sent to another upstream.", "route"),
		ejections: reg.NewCounterVec("switchyard_upstream_ejections_total",
			"Passive ejections after consecutive failures.", "pool", "upstream"),
		reloads: reg.NewCounterVec("switchyard_config_reloads_total",
			"Configuration reload attempts by result.", "result"),
	}
	eachUpstream := func(emit func(p *Pool, i int)) {
		rt := h.current.Load()
		if rt == nil {
			return
		}
		names := make([]string, 0, len(rt.pools))
		for n := range rt.pools {
			names = append(names, n)
		}
		sort.Strings(names)
		for _, n := range names {
			p := rt.pools[n]
			for i := range p.Upstreams {
				emit(p, i)
			}
		}
	}
	reg.NewGaugeFunc("switchyard_upstream_up",
		"1 when the upstream is eligible for traffic (healthy and not ejected).",
		[]string{"pool", "upstream"},
		func(emit func(float64, ...string)) {
			eachUpstream(func(p *Pool, i int) {
				u := p.Upstreams[i]
				v := 0.0
				if u.Available() {
					v = 1
				}
				emit(v, p.Name, u.Name())
			})
		})
	reg.NewGaugeFunc("switchyard_upstream_in_flight",
		"Requests currently in progress per upstream.",
		[]string{"pool", "upstream"},
		func(emit func(float64, ...string)) {
			eachUpstream(func(p *Pool, i int) {
				u := p.Upstreams[i]
				emit(float64(u.InFlight()), p.Name, u.Name())
			})
		})
	return m
}

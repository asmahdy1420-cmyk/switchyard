package proxy

import (
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"

	"github.com/asmahdy1420-cmyk/switchyard/internal/upstream"
)

var errNoUpstream = errors.New("no available upstream")

// retryTransport picks an upstream for every attempt. Idempotent requests
// whose body can be replayed are retried on a different upstream after a
// connection error or a 502/503/504 response. Every attempt feeds passive
// health: 5xx and connection errors count towards ejection, anything else
// resets the counter.
type retryTransport struct {
	base http.RoundTripper
	h    *Handler
}

func (t *retryTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	st := stateFrom(req.Context())
	if st == nil || st.route == nil {
		return nil, errNoUpstream
	}
	route, pool := st.route, st.route.Pool

	attempts := 1
	if idempotent(req.Method) && (req.Body == nil || req.Body == http.NoBody || req.GetBody != nil) {
		attempts += route.Retries
	}

	var (
		tried   []*upstream.Upstream
		held    *http.Response // a retryable response kept in case no other upstream is left
		lastErr error
	)
	skip := func(u *upstream.Upstream) bool {
		for _, x := range tried {
			if x == u {
				return true
			}
		}
		return false
	}

	for attempt := 0; attempt < attempts; attempt++ {
		u := pool.Balancer.Pick(req, skip)
		if u == nil {
			break
		}
		if held != nil {
			discard(held)
			held = nil
		}

		out := req.Clone(req.Context())
		if attempt > 0 {
			t.h.m.retries.With(route.Name).Inc()
			if req.GetBody != nil {
				body, err := req.GetBody()
				if err != nil {
					return nil, err
				}
				out.Body = body
			}
		}
		target(out, u)
		tried = append(tried, u)
		st.upstream, st.attempts = u.Name(), attempt+1

		u.Acquire()
		resp, err := t.base.RoundTrip(out)
		if err != nil {
			u.Release()
			t.observe(pool, u, "error", true)
			if req.Context().Err() != nil {
				// Deadline or client gone: another upstream will not help.
				return nil, err
			}
			lastErr = err
			continue
		}
		t.observe(pool, u, statusClass(resp.StatusCode), resp.StatusCode >= 500)

		if resp.StatusCode == http.StatusSwitchingProtocols {
			// Upgraded connections need the raw read-write body; the
			// in-flight count only covers the handshake.
			u.Release()
			return resp, nil
		}
		resp.Body = &releaseOnClose{ReadCloser: resp.Body, release: u.Release}
		if retryableStatus(resp.StatusCode) && attempt+1 < attempts {
			held = resp
			continue
		}
		return resp, nil
	}

	if held != nil {
		return held, nil
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, errNoUpstream
}

func (t *retryTransport) observe(p *Pool, u *upstream.Upstream, class string, failed bool) {
	t.h.m.upstream.With(p.Name, u.Name(), class).Inc()
	if !failed {
		u.ReportSuccess()
		return
	}
	if u.ReportFailure() {
		t.h.m.ejections.With(p.Name, u.Name()).Inc()
		t.h.log.Warn("upstream ejected", "pool", p.Name, "upstream", u.Name())
	}
}

// target points the outbound request at u, joining u's base path with the
// request path.
func target(out *http.Request, u *upstream.Upstream) {
	base := u.URL
	out.URL.Scheme = base.Scheme
	out.URL.Host = base.Host
	if bp := strings.TrimSuffix(base.Path, "/"); bp != "" {
		if out.URL.RawPath != "" {
			out.URL.RawPath = strings.TrimSuffix(base.EscapedPath(), "/") + out.URL.RawPath
		}
		out.URL.Path = bp + out.URL.Path
	}
	if base.RawQuery != "" {
		if out.URL.RawQuery == "" {
			out.URL.RawQuery = base.RawQuery
		} else {
			out.URL.RawQuery = base.RawQuery + "&" + out.URL.RawQuery
		}
	}
}

func retryableStatus(code int) bool {
	return code == http.StatusBadGateway || code == http.StatusServiceUnavailable || code == http.StatusGatewayTimeout
}

func statusClass(code int) string {
	switch {
	case code >= 500:
		return "5xx"
	case code >= 400:
		return "4xx"
	case code >= 300:
		return "3xx"
	case code >= 200:
		return "2xx"
	default:
		return "1xx"
	}
}

func discard(resp *http.Response) {
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	_ = resp.Body.Close()
}

// releaseOnClose decrements the in-flight count when the response body is
// closed, so least-connections sees requests that are still streaming.
type releaseOnClose struct {
	io.ReadCloser
	once    sync.Once
	release func()
}

func (r *releaseOnClose) Close() error {
	err := r.ReadCloser.Close()
	r.once.Do(r.release)
	return err
}

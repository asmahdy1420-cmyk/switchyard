package proxy

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/asmahdy1420-cmyk/switchyard/internal/balancer"
	"github.com/asmahdy1420-cmyk/switchyard/internal/config"
	"github.com/asmahdy1420-cmyk/switchyard/internal/health"
	"github.com/asmahdy1420-cmyk/switchyard/internal/upstream"
)

// Pool is the runtime form of a configured pool.
type Pool struct {
	Name      string
	Upstreams []*upstream.Upstream
	Balancer  balancer.Balancer
	checker   *health.Checker
}

// Route is the runtime form of a configured route.
type Route struct {
	Name         string
	Host         string // "" matches any host; "*.example.com" matches subdomains
	PathPrefix   string
	Pool         *Pool
	Timeout      time.Duration
	Retries      int
	StripPrefix  bool
	PreserveHost bool
}

// runtime is an immutable snapshot built from one configuration. The
// handler swaps whole snapshots on reload, so a request always sees one
// consistent set of routes, pools and balancers from start to finish.
type runtime struct {
	cfg       *config.Config
	routes    []*Route // sorted most specific first
	pools     map[string]*Pool
	transport *http.Transport
	proxy     *httputil.ReverseProxy

	cancel context.CancelFunc
	once   sync.Once
}

func newTransport() *http.Transport {
	return &http.Transport{
		Proxy: nil,
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:          512,
		MaxIdleConnsPerHost:   64,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ExpectContinueTimeout: time.Second,
	}
}

func buildRuntime(cfg *config.Config, h *Handler) (*runtime, error) {
	rt := &runtime{cfg: cfg, pools: map[string]*Pool{}, transport: newTransport()}

	for name, pc := range cfg.Pools {
		p := &Pool{Name: name}
		policy := upstream.PassivePolicy{MaxFailures: pc.Passive.MaxFailures, EjectionTime: pc.Passive.EjectionTime}
		for _, uc := range pc.Upstreams {
			u, err := upstream.New(uc.URL, uc.Weight, policy)
			if err != nil {
				return nil, fmt.Errorf("pool %q: %w", name, err)
			}
			p.Upstreams = append(p.Upstreams, u)
		}
		b, err := balancer.New(pc.Strategy, p.Upstreams, balancer.Options{
			HashKey:      pc.HashKey,
			VirtualNodes: pc.VirtualNodes,
		})
		if err != nil {
			return nil, fmt.Errorf("pool %q: %w", name, err)
		}
		p.Balancer = b
		if hc := pc.HealthCheck; hc != nil {
			p.checker = health.NewChecker(name, health.Config{
				Path:               hc.Path,
				Interval:           hc.Interval,
				Timeout:            hc.Timeout,
				HealthyThreshold:   hc.HealthyThreshold,
				UnhealthyThreshold: hc.UnhealthyThreshold,
			}, rt.transport, h.log)
		}
		rt.pools[name] = p
	}

	for _, rc := range cfg.Routes {
		rt.routes = append(rt.routes, &Route{
			Name:         rc.Name,
			Host:         rc.Host,
			PathPrefix:   rc.PathPrefix,
			Pool:         rt.pools[rc.Pool],
			Timeout:      rc.Timeout,
			Retries:      *rc.Retries,
			StripPrefix:  rc.StripPrefix,
			PreserveHost: rc.PreserveHost,
		})
	}
	sort.SliceStable(rt.routes, func(i, j int) bool {
		a, b := rt.routes[i], rt.routes[j]
		if hostRank(a.Host) != hostRank(b.Host) {
			return hostRank(a.Host) > hostRank(b.Host)
		}
		return len(a.PathPrefix) > len(b.PathPrefix)
	})

	rt.proxy = &httputil.ReverseProxy{
		Rewrite:      rewrite,
		Transport:    &retryTransport{base: rt.transport, h: h},
		ErrorHandler: h.proxyError,
		ErrorLog:     slog.NewLogLogger(h.log.Handler(), slog.LevelWarn),
	}
	return rt, nil
}

// start launches the active health checkers.
func (rt *runtime) start() {
	ctx, cancel := context.WithCancel(context.Background())
	rt.cancel = cancel
	for _, p := range rt.pools {
		if p.checker != nil {
			p.checker.Start(ctx, p.Upstreams)
		}
	}
}

// stop halts health checking and releases idle connections. Requests that
// are still in flight on this snapshot keep their connections and finish.
func (rt *runtime) stop() {
	rt.once.Do(func() {
		if rt.cancel != nil {
			rt.cancel()
		}
		for _, p := range rt.pools {
			if p.checker != nil {
				p.checker.Wait()
			}
		}
		rt.transport.CloseIdleConnections()
	})
}

func hostRank(h string) int {
	switch {
	case h == "":
		return 0
	case strings.HasPrefix(h, "*."):
		return 1
	default:
		return 2
	}
}

// match returns the most specific route for r, or nil.
func (rt *runtime) match(r *http.Request) *Route {
	host := strings.ToLower(r.Host)
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	for _, route := range rt.routes {
		if hostMatches(route.Host, host) && pathMatches(route.PathPrefix, r.URL.Path) {
			return route
		}
	}
	return nil
}

func hostMatches(pattern, host string) bool {
	switch {
	case pattern == "":
		return true
	case strings.HasPrefix(pattern, "*."):
		return strings.HasSuffix(host, pattern[1:]) && len(host) > len(pattern)-1
	default:
		return pattern == host
	}
}

// pathMatches is segment aware: "/api" matches "/api" and "/api/users" but
// not "/apiary".
func pathMatches(prefix, path string) bool {
	if !strings.HasPrefix(path, prefix) {
		return false
	}
	return len(path) == len(prefix) || strings.HasSuffix(prefix, "/") || path[len(prefix)] == '/'
}

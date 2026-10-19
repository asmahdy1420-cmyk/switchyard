// Package health runs active health checks against upstreams.
package health

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/asmahdy1420-cmyk/switchyard/internal/upstream"
)

// Config mirrors config.HealthCheck without importing it.
type Config struct {
	Path               string
	Interval           time.Duration
	Timeout            time.Duration
	HealthyThreshold   int
	UnhealthyThreshold int
}

// Tracker applies the threshold rules for one upstream. An upstream flips to
// unhealthy only after UnhealthyThreshold consecutive failed probes and back
// to healthy only after HealthyThreshold consecutive successes, which stops
// a flapping backend from bouncing in and out of rotation.
type Tracker struct {
	cfg       Config
	successes int
	failures  int
}

// NewTracker returns a tracker for cfg.
func NewTracker(cfg Config) *Tracker { return &Tracker{cfg: cfg} }

// Observe records one probe result and returns the state change it caused:
// changed is true when the upstream flipped, healthy is the new state.
func (t *Tracker) Observe(u *upstream.Upstream, ok bool) (changed, healthy bool) {
	if ok {
		t.failures = 0
		t.successes++
		if !u.Healthy() && t.successes >= t.cfg.HealthyThreshold {
			return u.SetHealthy(true), true
		}
		return false, u.Healthy()
	}
	t.successes = 0
	t.failures++
	if u.Healthy() && t.failures >= t.cfg.UnhealthyThreshold {
		return u.SetHealthy(false), false
	}
	return false, u.Healthy()
}

// Checker probes a set of upstreams on a fixed interval.
type Checker struct {
	cfg    Config
	pool   string
	client *http.Client
	log    *slog.Logger
	wg     sync.WaitGroup
}

// NewChecker returns a checker for the named pool.
func NewChecker(pool string, cfg Config, transport http.RoundTripper, log *slog.Logger) *Checker {
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Checker{
		cfg:  cfg,
		pool: pool,
		// The per-request context carries the timeout; the client must not
		// follow redirects so a 3xx is judged on its own.
		client: &http.Client{
			Transport: transport,
			CheckRedirect: func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
		log: log,
	}
}

// Start launches one probe loop per upstream. The loops stop when ctx is
// cancelled; Wait blocks until they have exited.
func (c *Checker) Start(ctx context.Context, ups []*upstream.Upstream) {
	for _, u := range ups {
		c.wg.Add(1)
		go c.loop(ctx, u)
	}
}

// Wait blocks until every probe loop has returned.
func (c *Checker) Wait() { c.wg.Wait() }

func (c *Checker) loop(ctx context.Context, u *upstream.Upstream) {
	defer c.wg.Done()
	tr := NewTracker(c.cfg)
	ticker := time.NewTicker(c.cfg.Interval)
	defer ticker.Stop()
	for {
		ok := c.Probe(ctx, u)
		if ctx.Err() != nil {
			return
		}
		if changed, healthy := tr.Observe(u, ok); changed {
			level := slog.LevelInfo
			if !healthy {
				level = slog.LevelWarn
			}
			c.log.Log(ctx, level, "upstream health changed",
				"pool", c.pool, "upstream", u.Name(), "healthy", healthy)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// Probe performs one GET against the health path. Any 2xx or 3xx within
// the timeout counts as success.
func (c *Checker) Probe(ctx context.Context, u *upstream.Upstream) bool {
	ctx, cancel := context.WithTimeout(ctx, c.cfg.Timeout)
	defer cancel()
	target := strings.TrimSuffix(u.URL.String(), "/") + c.cfg.Path
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return false
	}
	req.Header.Set("User-Agent", "switchyard-healthcheck")
	resp, err := c.client.Do(req)
	if err != nil {
		return false
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	_ = resp.Body.Close()
	return resp.StatusCode >= 200 && resp.StatusCode < 400
}

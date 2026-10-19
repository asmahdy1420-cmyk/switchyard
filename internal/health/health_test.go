package health

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/asmahdy1420-cmyk/switchyard/internal/upstream"
)

func TestTrackerThresholds(t *testing.T) {
	u := upstream.MustNew("http://10.0.0.1:8080", 1, upstream.PassivePolicy{})
	tr := NewTracker(Config{HealthyThreshold: 2, UnhealthyThreshold: 3})

	steps := []struct {
		ok          bool
		wantChanged bool
		wantHealthy bool
	}{
		{false, false, true},
		{false, false, true},
		{true, false, true}, // a success resets the failure streak
		{false, false, true},
		{false, false, true},
		{false, true, false}, // third consecutive failure flips to unhealthy
		{false, false, false},
		{true, false, false},
		{true, true, true}, // second consecutive success flips back
	}
	for i, s := range steps {
		changed, healthy := tr.Observe(u, s.ok)
		if changed != s.wantChanged || healthy != s.wantHealthy || u.Healthy() != s.wantHealthy {
			t.Fatalf("step %d (ok=%v): changed=%v healthy=%v, want changed=%v healthy=%v",
				i, s.ok, changed, healthy, s.wantChanged, s.wantHealthy)
		}
	}
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func TestCheckerMarksUnhealthyAndRecovers(t *testing.T) {
	var failing atomic.Bool
	var probes atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/healthz" {
			t.Errorf("probe hit %q, want /healthz", r.URL.Path)
		}
		probes.Add(1)
		if failing.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	u := upstream.MustNew(srv.URL, 1, upstream.PassivePolicy{})
	c := NewChecker("api", Config{
		Path: "/healthz", Interval: 10 * time.Millisecond, Timeout: time.Second,
		HealthyThreshold: 2, UnhealthyThreshold: 2,
	}, http.DefaultTransport, nil)
	ctx, cancel := context.WithCancel(context.Background())
	c.Start(ctx, []*upstream.Upstream{u})
	defer func() { cancel(); c.Wait() }()

	eventually(t, "first probes", func() bool { return probes.Load() >= 2 })
	if !u.Healthy() {
		t.Fatal("healthy upstream marked unhealthy")
	}
	failing.Store(true)
	eventually(t, "unhealthy transition", func() bool { return !u.Healthy() })
	failing.Store(false)
	eventually(t, "recovery", func() bool { return u.Healthy() })
}

func TestProbeTimeoutCountsAsFailure(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()
	defer close(release)

	u := upstream.MustNew(srv.URL, 1, upstream.PassivePolicy{})
	c := NewChecker("api", Config{Path: "/", Interval: time.Second, Timeout: 20 * time.Millisecond}, http.DefaultTransport, nil)
	start := time.Now()
	if c.Probe(context.Background(), u) {
		t.Fatal("probe that exceeded its timeout reported success")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("probe took %v, timeout was not enforced", elapsed)
	}
}

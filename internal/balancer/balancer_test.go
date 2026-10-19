package balancer

import (
	"fmt"
	"net/http/httptest"
	"testing"

	"github.com/asmahdy1420-cmyk/switchyard/internal/upstream"
)

func pool(t *testing.T, weights ...int) []*upstream.Upstream {
	t.Helper()
	ups := make([]*upstream.Upstream, len(weights))
	for i, w := range weights {
		ups[i] = upstream.MustNew(fmt.Sprintf("http://10.0.0.%d:8080", i+1), w, upstream.PassivePolicy{})
	}
	return ups
}

func count(b Balancer, n int) map[*upstream.Upstream]int {
	got := map[*upstream.Upstream]int{}
	req := httptest.NewRequest("GET", "/", nil)
	for i := 0; i < n; i++ {
		got[b.Pick(req, nil)]++
	}
	return got
}

func TestRoundRobinDistributesEvenly(t *testing.T) {
	ups := pool(t, 1, 1, 1)
	got := count(NewRoundRobin(ups), 300)
	for _, u := range ups {
		if got[u] != 100 {
			t.Fatalf("%s got %d picks, want 100", u.Name(), got[u])
		}
	}
}

func TestRoundRobinSkipsUnavailableAndExcluded(t *testing.T) {
	ups := pool(t, 1, 1, 1)
	ups[1].SetHealthy(false)
	b := NewRoundRobin(ups)
	skipFirst := func(u *upstream.Upstream) bool { return u == ups[0] }
	req := httptest.NewRequest("GET", "/", nil)
	for i := 0; i < 10; i++ {
		if got := b.Pick(req, skipFirst); got != ups[2] {
			t.Fatalf("pick %d: got %v, want %s", i, got, ups[2].Name())
		}
	}
	ups[2].SetHealthy(false)
	if got := b.Pick(req, skipFirst); got != nil {
		t.Fatalf("expected nil when nothing is eligible, got %s", got.Name())
	}
}

func TestLeastConnectionsPrefersLeastLoaded(t *testing.T) {
	ups := pool(t, 1, 1, 1)
	ups[0].Acquire()
	ups[0].Acquire()
	ups[1].Acquire()
	b := NewLeastConnections(ups)
	req := httptest.NewRequest("GET", "/", nil)
	for i := 0; i < 5; i++ {
		if got := b.Pick(req, nil); got != ups[2] {
			t.Fatalf("got %s, want idle upstream %s", got.Name(), ups[2].Name())
		}
	}
	// With equal load, ties rotate instead of hammering the first upstream.
	ups[1].Acquire()
	ups[2].Acquire()
	ups[2].Acquire()
	got := count(b, 300)
	for _, u := range ups {
		if got[u] != 100 {
			t.Fatalf("tie-break: %s got %d, want 100", u.Name(), got[u])
		}
	}
}

func TestWeightedDistributionMatchesWeights(t *testing.T) {
	ups := pool(t, 5, 3, 1)
	got := count(NewWeighted(ups), 900)
	want := []int{500, 300, 100}
	for i, u := range ups {
		if got[u] != want[i] {
			t.Fatalf("%s got %d picks, want %d", u.Name(), got[u], want[i])
		}
	}
}

func TestWeightedIsSmooth(t *testing.T) {
	ups := pool(t, 5, 1, 1)
	b := NewWeighted(ups)
	req := httptest.NewRequest("GET", "/", nil)
	// Naive weighted round-robin yields a a a a a b c. Smooth WRR spreads
	// the light upstreams through the cycle, and the cycle repeats exactly.
	want := []int{0, 0, 1, 0, 2, 0, 0}
	for cycle := 0; cycle < 3; cycle++ {
		for i, idx := range want {
			if got := b.Pick(req, nil); got != ups[idx] {
				t.Fatalf("cycle %d pick %d: got %s, want %s", cycle, i, got.Name(), ups[idx].Name())
			}
		}
	}
}

func TestConsistentHashIsDeterministicPerKey(t *testing.T) {
	ups := pool(t, 1, 1, 1, 1)
	b := NewConsistentHash(ups, 100, KeyFunc("header:X-User"))
	for i := 0; i < 50; i++ {
		req := httptest.NewRequest("GET", "/", nil)
		req.Header.Set("X-User", fmt.Sprintf("user-%d", i))
		first := b.Pick(req, nil)
		for j := 0; j < 5; j++ {
			if got := b.Pick(req, nil); got != first {
				t.Fatalf("key user-%d moved from %s to %s", i, first.Name(), got.Name())
			}
		}
	}
}

func TestConsistentHashSpreadsKeys(t *testing.T) {
	ups := pool(t, 1, 1, 1, 1, 1)
	b := NewConsistentHash(ups, 160, nil)
	got := map[*upstream.Upstream]int{}
	const keys = 10000
	for i := 0; i < keys; i++ {
		got[b.PickKey(fmt.Sprintf("10.1.%d.%d", i/256, i%256), nil)]++
	}
	for _, u := range ups {
		share := float64(got[u]) / keys
		if share < 0.12 || share > 0.28 {
			t.Fatalf("%s owns %.1f%% of keys, want roughly 20%%", u.Name(), share*100)
		}
	}
}

func TestConsistentHashStableWhenNodeRemoved(t *testing.T) {
	all := pool(t, 1, 1, 1, 1, 1)
	before := NewConsistentHash(all, 100, nil)
	removed := all[2]
	remaining := append(append([]*upstream.Upstream{}, all[:2]...), all[3:]...)
	after := NewConsistentHash(remaining, 100, nil)

	const keys = 5000
	moved := 0
	for i := 0; i < keys; i++ {
		key := fmt.Sprintf("session-%d", i)
		a, b := before.PickKey(key, nil), after.PickKey(key, nil)
		if a == removed {
			moved++
			continue
		}
		if a != b {
			t.Fatalf("key %s moved from %s to %s although its node was not removed", key, a.Name(), b.Name())
		}
	}
	// Only the removed node's share, about 1/5, may move.
	if frac := float64(moved) / keys; frac < 0.10 || frac > 0.30 {
		t.Fatalf("%.1f%% of keys moved, want about 20%%", frac*100)
	}
}

func TestConsistentHashFailsOverAndReturns(t *testing.T) {
	ups := pool(t, 1, 1, 1)
	b := NewConsistentHash(ups, 100, nil)
	owner := b.PickKey("tenant-42", nil)
	owner.SetHealthy(false)
	spill := b.PickKey("tenant-42", nil)
	if spill == nil || spill == owner {
		t.Fatalf("expected failover away from %s, got %v", owner.Name(), spill)
	}
	owner.SetHealthy(true)
	if got := b.PickKey("tenant-42", nil); got != owner {
		t.Fatalf("key did not return to %s after recovery, got %s", owner.Name(), got.Name())
	}
}

func TestKeyFuncFallsBackToClientIP(t *testing.T) {
	key := KeyFunc("header:X-User")
	req := httptest.NewRequest("GET", "/", nil)
	req.RemoteAddr = "192.0.2.7:51000"
	if got := key(req); got != "192.0.2.7" {
		t.Fatalf("got %q, want client IP", got)
	}
	req.Header.Set("X-User", "alice")
	if got := key(req); got != "alice" {
		t.Fatalf("got %q, want header value", got)
	}
}

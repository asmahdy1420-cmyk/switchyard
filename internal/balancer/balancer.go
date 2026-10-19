// Package balancer implements the upstream selection strategies.
//
// Every strategy only considers upstreams that are Available and not
// rejected by the caller's skip function, which is how retries avoid the
// upstream that just failed.
package balancer

import (
	"fmt"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/asmahdy1420-cmyk/switchyard/internal/config"
	"github.com/asmahdy1420-cmyk/switchyard/internal/upstream"
)

// SkipFunc excludes upstreams from a single pick. It may be nil.
type SkipFunc func(*upstream.Upstream) bool

// Balancer picks an upstream for a request, or returns nil when none is
// eligible.
type Balancer interface {
	Pick(r *http.Request, skip SkipFunc) *upstream.Upstream
}

// Options carries the strategy-specific settings.
type Options struct {
	HashKey      string // "ip" or "header:<Name>"
	VirtualNodes int
}

// New builds the balancer for strategy.
func New(strategy string, ups []*upstream.Upstream, opts Options) (Balancer, error) {
	switch strategy {
	case config.RoundRobin, "":
		return NewRoundRobin(ups), nil
	case config.LeastConnections:
		return NewLeastConnections(ups), nil
	case config.Weighted:
		return NewWeighted(ups), nil
	case config.ConsistentHash:
		return NewConsistentHash(ups, opts.VirtualNodes, KeyFunc(opts.HashKey)), nil
	default:
		return nil, fmt.Errorf("unknown strategy %q", strategy)
	}
}

func eligible(u *upstream.Upstream, skip SkipFunc) bool {
	return u.Available() && (skip == nil || !skip(u))
}

// RoundRobin cycles through upstreams in order.
type RoundRobin struct {
	ups  []*upstream.Upstream
	next atomic.Uint64
}

// NewRoundRobin returns a round-robin balancer.
func NewRoundRobin(ups []*upstream.Upstream) *RoundRobin { return &RoundRobin{ups: ups} }

// Pick implements Balancer.
func (b *RoundRobin) Pick(_ *http.Request, skip SkipFunc) *upstream.Upstream {
	n := uint64(len(b.ups))
	if n == 0 {
		return nil
	}
	start := b.next.Add(1) - 1
	for i := uint64(0); i < n; i++ {
		if u := b.ups[(start+i)%n]; eligible(u, skip) {
			return u
		}
	}
	return nil
}

// LeastConnections picks the upstream with the fewest in-flight requests.
// Ties are broken by a rotating start index so idle pools still spread load.
type LeastConnections struct {
	ups  []*upstream.Upstream
	next atomic.Uint64
}

// NewLeastConnections returns a least-connections balancer.
func NewLeastConnections(ups []*upstream.Upstream) *LeastConnections {
	return &LeastConnections{ups: ups}
}

// Pick implements Balancer.
func (b *LeastConnections) Pick(_ *http.Request, skip SkipFunc) *upstream.Upstream {
	n := uint64(len(b.ups))
	if n == 0 {
		return nil
	}
	start := b.next.Add(1) - 1
	var best *upstream.Upstream
	var bestLoad int64
	for i := uint64(0); i < n; i++ {
		u := b.ups[(start+i)%n]
		if !eligible(u, skip) {
			continue
		}
		if load := u.InFlight(); best == nil || load < bestLoad {
			best, bestLoad = u, load
		}
	}
	return best
}

// Weighted is smooth weighted round-robin: over any window of sum(weights)
// picks every upstream receives exactly its weight, and picks for heavy
// upstreams are interleaved rather than sent in bursts.
type Weighted struct {
	mu      sync.Mutex
	ups     []*upstream.Upstream
	current []int
}

// NewWeighted returns a smooth weighted round-robin balancer.
func NewWeighted(ups []*upstream.Upstream) *Weighted {
	return &Weighted{ups: ups, current: make([]int, len(ups))}
}

// Pick implements Balancer.
func (b *Weighted) Pick(_ *http.Request, skip SkipFunc) *upstream.Upstream {
	b.mu.Lock()
	defer b.mu.Unlock()
	best, total := -1, 0
	for i, u := range b.ups {
		if !eligible(u, skip) {
			continue
		}
		b.current[i] += u.Weight
		total += u.Weight
		if best < 0 || b.current[i] > b.current[best] {
			best = i
		}
	}
	if best < 0 {
		return nil
	}
	b.current[best] -= total
	return b.ups[best]
}

// KeyFunc returns the request-key extractor for a hash_key setting. A
// header key falls back to the client IP when the header is absent.
func KeyFunc(spec string) func(*http.Request) string {
	if name, ok := strings.CutPrefix(spec, "header:"); ok {
		name = http.CanonicalHeaderKey(strings.TrimSpace(name))
		return func(r *http.Request) string {
			if v := r.Header.Get(name); v != "" {
				return v
			}
			return clientIP(r)
		}
	}
	return clientIP
}

func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

type ringPoint struct {
	hash uint64
	idx  int
}

// ConsistentHash maps request keys onto a hash ring with virtual nodes.
// Each upstream owns VirtualNodes*Weight points derived from its URL alone,
// so adding or removing one upstream only remaps the keys it owned.
type ConsistentHash struct {
	ups  []*upstream.Upstream
	ring []ringPoint
	key  func(*http.Request) string
}

// NewConsistentHash builds the ring. vnodes below 1 is treated as 1.
func NewConsistentHash(ups []*upstream.Upstream, vnodes int, key func(*http.Request) string) *ConsistentHash {
	if vnodes < 1 {
		vnodes = 1
	}
	if key == nil {
		key = clientIP
	}
	b := &ConsistentHash{ups: ups, key: key}
	for i, u := range ups {
		id := u.URL.String()
		for v := 0; v < vnodes*u.Weight; v++ {
			b.ring = append(b.ring, ringPoint{hash: hashString(id + "#" + strconv.Itoa(v)), idx: i})
		}
	}
	sort.Slice(b.ring, func(i, j int) bool {
		if b.ring[i].hash != b.ring[j].hash {
			return b.ring[i].hash < b.ring[j].hash
		}
		return b.ring[i].idx < b.ring[j].idx
	})
	return b
}

// Pick implements Balancer. It walks clockwise from the key's position to
// the first eligible upstream, so an ejected node's keys spill onto its
// ring neighbours and come back when it recovers.
func (b *ConsistentHash) Pick(r *http.Request, skip SkipFunc) *upstream.Upstream {
	return b.PickKey(b.key(r), skip)
}

// PickKey is Pick for an already extracted key.
func (b *ConsistentHash) PickKey(key string, skip SkipFunc) *upstream.Upstream {
	n := len(b.ring)
	if n == 0 {
		return nil
	}
	h := hashString(key)
	start := sort.Search(n, func(i int) bool { return b.ring[i].hash >= h })
	for i := 0; i < n; i++ {
		if u := b.ups[b.ring[(start+i)%n].idx]; eligible(u, skip) {
			return u
		}
	}
	return nil
}

// hashString is 64-bit FNV-1a followed by the splitmix64 finaliser. FNV on
// its own clusters similar strings such as "host#1" and "host#2"; the
// finaliser spreads them evenly around the ring.
func hashString(s string) uint64 {
	const (
		offset = 14695981039346656037
		prime  = 1099511628211
	)
	h := uint64(offset)
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= prime
	}
	h ^= h >> 30
	h *= 0xbf58476d1ce4e5b9
	h ^= h >> 27
	h *= 0x94d049bb133111eb
	h ^= h >> 31
	return h
}

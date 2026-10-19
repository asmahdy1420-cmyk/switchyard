// Package metrics is a small, dependency-free implementation of the
// Prometheus text exposition format: counters, histograms and gauges whose
// values are read at scrape time.
package metrics

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// Collector writes one metric family in text format.
type Collector interface {
	Collect(w io.Writer)
}

// Registry holds collectors in registration order.
type Registry struct {
	mu         sync.Mutex
	collectors []Collector
}

// NewRegistry returns an empty registry.
func NewRegistry() *Registry { return &Registry{} }

// Register adds c to the registry.
func (r *Registry) Register(c Collector) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.collectors = append(r.collectors, c)
}

// WriteTo writes every collector to w.
func (r *Registry) WriteTo(w io.Writer) (int64, error) {
	r.mu.Lock()
	cs := append([]Collector(nil), r.collectors...)
	r.mu.Unlock()
	cw := &countingWriter{w: bufio.NewWriter(w)}
	for _, c := range cs {
		c.Collect(cw)
	}
	err := cw.w.Flush()
	if err == nil {
		err = cw.err
	}
	return cw.n, err
}

// ServeHTTP exposes the registry at a scrape endpoint.
func (r *Registry) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	_, _ = r.WriteTo(w)
}

type countingWriter struct {
	w   *bufio.Writer
	n   int64
	err error
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	if err != nil && c.err == nil {
		c.err = err
	}
	return n, err
}

// series is the shared label bookkeeping for vectors.
type series[T any] struct {
	mu     sync.Mutex
	labels []string
	byKey  map[string]*entry[T]
	newT   func() *T
}

type entry[T any] struct {
	values []string
	v      *T
}

func newSeries[T any](labels []string, newT func() *T) series[T] {
	return series[T]{labels: labels, byKey: map[string]*entry[T]{}, newT: newT}
}

func (s *series[T]) with(values []string) *T {
	if len(values) != len(s.labels) {
		panic(fmt.Sprintf("metrics: got %d label values, want %d", len(values), len(s.labels)))
	}
	key := strings.Join(values, "\xff")
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.byKey[key]
	if !ok {
		e = &entry[T]{values: append([]string(nil), values...), v: s.newT()}
		s.byKey[key] = e
	}
	return e.v
}

// sorted returns a stable snapshot of the series, ordered by label values.
func (s *series[T]) sorted() []*entry[T] {
	s.mu.Lock()
	keys := make([]string, 0, len(s.byKey))
	for k := range s.byKey {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]*entry[T], len(keys))
	for i, k := range keys {
		out[i] = s.byKey[k]
	}
	s.mu.Unlock()
	return out
}

// Counter is a monotonically increasing integer.
type Counter struct{ v atomic.Uint64 }

// Inc adds one.
func (c *Counter) Inc() { c.v.Add(1) }

// Add adds n.
func (c *Counter) Add(n uint64) { c.v.Add(n) }

// Value returns the current count.
func (c *Counter) Value() uint64 { return c.v.Load() }

// CounterVec is a family of counters partitioned by labels.
type CounterVec struct {
	name, help string
	s          series[Counter]
}

// NewCounterVec registers and returns a counter family.
func (r *Registry) NewCounterVec(name, help string, labels ...string) *CounterVec {
	c := &CounterVec{name: name, help: help, s: newSeries(labels, func() *Counter { return &Counter{} })}
	r.Register(c)
	return c
}

// With returns the counter for the given label values.
func (c *CounterVec) With(values ...string) *Counter { return c.s.with(values) }

// Collect implements Collector.
func (c *CounterVec) Collect(w io.Writer) {
	header(w, c.name, c.help, "counter")
	for _, e := range c.s.sorted() {
		fmt.Fprintf(w, "%s%s %d\n", c.name, labelString(c.s.labels, e.values, "", ""), e.v.Value())
	}
}

// DefaultBuckets suits request latencies in seconds.
var DefaultBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

// Histogram counts observations into cumulative buckets.
type Histogram struct {
	mu      sync.Mutex
	buckets []float64
	counts  []uint64 // per bucket, not cumulative; last slot is +Inf
	sum     float64
	count   uint64
}

// Observe records v.
func (h *Histogram) Observe(v float64) {
	i := sort.SearchFloat64s(h.buckets, v) // first bucket with upper bound >= v
	h.mu.Lock()
	h.counts[i]++
	h.sum += v
	h.count++
	h.mu.Unlock()
}

// HistogramVec is a family of histograms partitioned by labels.
type HistogramVec struct {
	name, help string
	buckets    []float64
	s          series[Histogram]
}

// NewHistogramVec registers and returns a histogram family. Buckets must be
// sorted ascending; nil selects DefaultBuckets.
func (r *Registry) NewHistogramVec(name, help string, buckets []float64, labels ...string) *HistogramVec {
	if buckets == nil {
		buckets = DefaultBuckets
	}
	if !sort.Float64sAreSorted(buckets) {
		panic("metrics: histogram buckets must be sorted")
	}
	b := append([]float64(nil), buckets...)
	h := &HistogramVec{name: name, help: help, buckets: b}
	h.s = newSeries(labels, func() *Histogram {
		return &Histogram{buckets: b, counts: make([]uint64, len(b)+1)}
	})
	r.Register(h)
	return h
}

// With returns the histogram for the given label values.
func (h *HistogramVec) With(values ...string) *Histogram { return h.s.with(values) }

// Collect implements Collector.
func (h *HistogramVec) Collect(w io.Writer) {
	header(w, h.name, h.help, "histogram")
	for _, e := range h.s.sorted() {
		hist := e.v
		hist.mu.Lock()
		counts := append([]uint64(nil), hist.counts...)
		sum, count := hist.sum, hist.count
		hist.mu.Unlock()

		var cum uint64
		for i, upper := range h.buckets {
			cum += counts[i]
			fmt.Fprintf(w, "%s_bucket%s %d\n", h.name, labelString(h.s.labels, e.values, "le", formatFloat(upper)), cum)
		}
		cum += counts[len(h.buckets)]
		fmt.Fprintf(w, "%s_bucket%s %d\n", h.name, labelString(h.s.labels, e.values, "le", "+Inf"), cum)
		fmt.Fprintf(w, "%s_sum%s %s\n", h.name, labelString(h.s.labels, e.values, "", ""), formatFloat(sum))
		fmt.Fprintf(w, "%s_count%s %d\n", h.name, labelString(h.s.labels, e.values, "", ""), count)
	}
}

// GaugeFunc is a gauge family whose samples are produced at scrape time.
type GaugeFunc struct {
	name, help string
	labels     []string
	fn         func(emit func(value float64, labelValues ...string))
}

// NewGaugeFunc registers a gauge family backed by fn.
func (r *Registry) NewGaugeFunc(name, help string, labels []string, fn func(emit func(value float64, labelValues ...string))) *GaugeFunc {
	g := &GaugeFunc{name: name, help: help, labels: labels, fn: fn}
	r.Register(g)
	return g
}

// Collect implements Collector.
func (g *GaugeFunc) Collect(w io.Writer) {
	header(w, g.name, g.help, "gauge")
	g.fn(func(v float64, values ...string) {
		fmt.Fprintf(w, "%s%s %s\n", g.name, labelString(g.labels, values, "", ""), formatFloat(v))
	})
}

func header(w io.Writer, name, help, typ string) {
	fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n", name, escapeHelp(help), name, typ)
}

func labelString(names, values []string, extraName, extraValue string) string {
	if len(names) == 0 && extraName == "" {
		return ""
	}
	var b strings.Builder
	b.WriteByte('{')
	for i, n := range names {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(n)
		b.WriteString(`="`)
		b.WriteString(escapeLabel(values[i]))
		b.WriteByte('"')
	}
	if extraName != "" {
		if len(names) > 0 {
			b.WriteByte(',')
		}
		b.WriteString(extraName)
		b.WriteString(`="`)
		b.WriteString(extraValue)
		b.WriteByte('"')
	}
	b.WriteByte('}')
	return b.String()
}

var (
	labelEscaper = strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)
	helpEscaper  = strings.NewReplacer(`\`, `\\`, "\n", `\n`)
)

func escapeLabel(s string) string { return labelEscaper.Replace(s) }
func escapeHelp(s string) string  { return helpEscaper.Replace(s) }

func formatFloat(v float64) string {
	switch {
	case math.IsInf(v, 1):
		return "+Inf"
	case math.IsInf(v, -1):
		return "-Inf"
	case math.IsNaN(v):
		return "NaN"
	}
	return strconv.FormatFloat(v, 'g', -1, 64)
}

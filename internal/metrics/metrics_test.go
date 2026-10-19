package metrics

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestExpositionFormat(t *testing.T) {
	reg := NewRegistry()
	reqs := reg.NewCounterVec("app_requests_total", "Requests served.", "route", "code")
	lat := reg.NewHistogramVec("app_latency_seconds", "Latency.", []float64{0.1, 0.5, 1}, "route")
	reg.NewGaugeFunc("app_up", "Upstream state.", []string{"upstream"}, func(emit func(float64, ...string)) {
		emit(1, "a:80")
		emit(0, "b:80")
	})

	reqs.With("api", "200").Inc()
	reqs.With("api", "200").Inc()
	reqs.With("api", "502").Add(3)
	lat.With("api").Observe(0.0625)
	lat.With("api").Observe(0.5) // upper bounds are inclusive
	lat.With("api").Observe(0.75)
	lat.With("api").Observe(4)

	rec := httptest.NewRecorder()
	reg.ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	if ct := rec.Header().Get("Content-Type"); !strings.HasPrefix(ct, "text/plain; version=0.0.4") {
		t.Fatalf("content type %q", ct)
	}
	want := `# HELP app_requests_total Requests served.
# TYPE app_requests_total counter
app_requests_total{route="api",code="200"} 2
app_requests_total{route="api",code="502"} 3
# HELP app_latency_seconds Latency.
# TYPE app_latency_seconds histogram
app_latency_seconds_bucket{route="api",le="0.1"} 1
app_latency_seconds_bucket{route="api",le="0.5"} 2
app_latency_seconds_bucket{route="api",le="1"} 3
app_latency_seconds_bucket{route="api",le="+Inf"} 4
app_latency_seconds_sum{route="api"} 5.3125
app_latency_seconds_count{route="api"} 4
# HELP app_up Upstream state.
# TYPE app_up gauge
app_up{upstream="a:80"} 1
app_up{upstream="b:80"} 0
`
	if got := rec.Body.String(); got != want {
		t.Fatalf("exposition mismatch\n--- got ---\n%s\n--- want ---\n%s", got, want)
	}
}

func TestLabelValuesAreEscaped(t *testing.T) {
	reg := NewRegistry()
	reg.NewCounterVec("x_total", "Help with \\ and\nnewline.", "path").With("a\"b\\c\nd").Inc()
	var b strings.Builder
	if _, err := reg.WriteTo(&b); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(b.String(), `x_total{path="a\"b\\c\nd"} 1`) {
		t.Fatalf("label not escaped:\n%s", b.String())
	}
	if !strings.Contains(b.String(), `# HELP x_total Help with \\ and\nnewline.`) {
		t.Fatalf("help not escaped:\n%s", b.String())
	}
}

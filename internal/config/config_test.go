package config

import (
	"strings"
	"testing"
	"time"
)

func TestParseAppliesDefaults(t *testing.T) {
	cfg, err := Parse([]byte(`
pools:
  api:
    upstreams:
      - url: http://10.0.0.1:9000
    health_check:
      path: /healthz
routes:
  - pool: api
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Listen != ":8080" || cfg.AdminListen != ":9090" {
		t.Fatalf("listen defaults: %q %q", cfg.Listen, cfg.AdminListen)
	}
	p := cfg.Pools["api"]
	if p.Strategy != RoundRobin || p.Upstreams[0].Weight != 1 || p.VirtualNodes != 100 {
		t.Fatalf("pool defaults not applied: %+v", p)
	}
	if hc := p.HealthCheck; hc.Interval != 10*time.Second || hc.HealthyThreshold != 2 || hc.UnhealthyThreshold != 3 {
		t.Fatalf("health check defaults not applied: %+v", hc)
	}
	if p.Passive.MaxFailures != 5 || p.Passive.EjectionTime != 30*time.Second {
		t.Fatalf("passive defaults not applied: %+v", p.Passive)
	}
	r := cfg.Routes[0]
	if r.Name != "route-0" || r.PathPrefix != "/" || *r.Retries != 1 || r.Timeout != 30*time.Second {
		t.Fatalf("route defaults not applied: %+v", r)
	}
}

func TestExplicitZeroRetriesIsKept(t *testing.T) {
	cfg, err := Parse([]byte(`
pools: {api: {upstreams: [{url: "http://a:1"}]}}
routes: [{pool: api, retries: 0, timeout: 250ms}]
`))
	if err != nil {
		t.Fatal(err)
	}
	if *cfg.Routes[0].Retries != 0 || cfg.Routes[0].Timeout != 250*time.Millisecond {
		t.Fatalf("got retries=%d timeout=%v", *cfg.Routes[0].Retries, cfg.Routes[0].Timeout)
	}
}

func TestParseRejectsInvalidConfig(t *testing.T) {
	cases := map[string]struct{ yaml, want string }{
		"unknown pool": {`
pools: {api: {upstreams: [{url: "http://a:1"}]}}
routes: [{pool: web}]`, `unknown pool "web"`},
		"bad strategy": {`
pools: {api: {strategy: random, upstreams: [{url: "http://a:1"}]}}
routes: [{pool: api}]`, `unknown strategy "random"`},
		"bad url": {`
pools: {api: {upstreams: [{url: "ftp://a:1"}]}}
routes: [{pool: api}]`, `invalid upstream url`},
		"bad hash key": {`
pools: {api: {strategy: consistent_hash, hash_key: cookie, upstreams: [{url: "http://a:1"}]}}
routes: [{pool: api}]`, `hash_key`},
		"typo in key": {`
pools: {api: {upstream: [{url: "http://a:1"}]}}
routes: [{pool: api}]`, `field upstream not found`},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := Parse([]byte(tc.yaml))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got error %v, want it to mention %q", err, tc.want)
			}
		})
	}
}

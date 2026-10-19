// Package config loads and validates the switchyard YAML configuration.
package config

import (
	"bytes"
	"errors"
	"fmt"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Balancing strategies understood by the balancer package.
const (
	RoundRobin       = "round_robin"
	LeastConnections = "least_connections"
	Weighted         = "weighted"
	ConsistentHash   = "consistent_hash"
)

// Config is the root of the configuration file.
type Config struct {
	Listen          string           `yaml:"listen"`
	AdminListen     string           `yaml:"admin_listen"`
	ShutdownTimeout time.Duration    `yaml:"shutdown_timeout"`
	LogFormat       string           `yaml:"log_format"`
	MaxRetryBody    int64            `yaml:"max_retry_body"`
	Pools           map[string]*Pool `yaml:"pools"`
	Routes          []*Route         `yaml:"routes"`
}

// Pool is a named group of upstreams sharing a balancing strategy.
type Pool struct {
	Strategy     string        `yaml:"strategy"`
	HashKey      string        `yaml:"hash_key"`
	VirtualNodes int           `yaml:"virtual_nodes"`
	Upstreams    []Upstream    `yaml:"upstreams"`
	HealthCheck  *HealthCheck  `yaml:"health_check"`
	Passive      *PassiveCheck `yaml:"passive"`
}

// Upstream is a single backend server.
type Upstream struct {
	URL    string `yaml:"url"`
	Weight int    `yaml:"weight"`
}

// HealthCheck configures active probing. A nil HealthCheck disables it.
type HealthCheck struct {
	Path               string        `yaml:"path"`
	Interval           time.Duration `yaml:"interval"`
	Timeout            time.Duration `yaml:"timeout"`
	HealthyThreshold   int           `yaml:"healthy_threshold"`
	UnhealthyThreshold int           `yaml:"unhealthy_threshold"`
}

// PassiveCheck configures ejection on consecutive failed proxied requests.
// MaxFailures of zero disables passive ejection.
type PassiveCheck struct {
	MaxFailures  int           `yaml:"max_failures"`
	EjectionTime time.Duration `yaml:"ejection_time"`
}

// Route maps a host and path prefix to a pool.
type Route struct {
	Name         string        `yaml:"name"`
	Host         string        `yaml:"host"`
	PathPrefix   string        `yaml:"path_prefix"`
	Pool         string        `yaml:"pool"`
	Timeout      time.Duration `yaml:"timeout"`
	Retries      *int          `yaml:"retries"`
	StripPrefix  bool          `yaml:"strip_prefix"`
	PreserveHost bool          `yaml:"preserve_host"`
}

// Load reads, decodes, defaults and validates the file at path.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return Parse(data)
}

// Parse decodes and validates YAML bytes. Unknown keys are rejected so that
// typos fail loudly instead of being silently ignored.
func Parse(data []byte) (*Config, error) {
	var cfg Config
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("decode config: %w", err)
	}
	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func (c *Config) applyDefaults() {
	if c.Listen == "" {
		c.Listen = ":8080"
	}
	if c.AdminListen == "" {
		c.AdminListen = ":9090"
	}
	if c.ShutdownTimeout == 0 {
		c.ShutdownTimeout = 15 * time.Second
	}
	if c.LogFormat == "" {
		c.LogFormat = "json"
	}
	if c.MaxRetryBody == 0 {
		c.MaxRetryBody = 1 << 20
	}
	for _, p := range c.Pools {
		if p == nil {
			continue
		}
		if p.Strategy == "" {
			p.Strategy = RoundRobin
		}
		if p.HashKey == "" {
			p.HashKey = "ip"
		}
		if p.VirtualNodes == 0 {
			p.VirtualNodes = 100
		}
		for i := range p.Upstreams {
			if p.Upstreams[i].Weight == 0 {
				p.Upstreams[i].Weight = 1
			}
		}
		if hc := p.HealthCheck; hc != nil {
			if hc.Path == "" {
				hc.Path = "/"
			}
			if hc.Interval == 0 {
				hc.Interval = 10 * time.Second
			}
			if hc.Timeout == 0 {
				hc.Timeout = 2 * time.Second
			}
			if hc.HealthyThreshold == 0 {
				hc.HealthyThreshold = 2
			}
			if hc.UnhealthyThreshold == 0 {
				hc.UnhealthyThreshold = 3
			}
		}
		if p.Passive == nil {
			p.Passive = &PassiveCheck{MaxFailures: 5}
		}
		if p.Passive.EjectionTime == 0 {
			p.Passive.EjectionTime = 30 * time.Second
		}
	}
	for i, r := range c.Routes {
		if r == nil {
			continue
		}
		if r.Name == "" {
			r.Name = fmt.Sprintf("route-%d", i)
		}
		if r.PathPrefix == "" {
			r.PathPrefix = "/"
		}
		if r.Timeout == 0 {
			r.Timeout = 30 * time.Second
		}
		if r.Retries == nil {
			one := 1
			r.Retries = &one
		}
		r.Host = strings.ToLower(r.Host)
	}
}

// Validate reports every problem it finds, joined into one error.
func (c *Config) Validate() error {
	var errs []error
	add := func(format string, args ...any) { errs = append(errs, fmt.Errorf(format, args...)) }

	if c.LogFormat != "json" && c.LogFormat != "text" {
		add("log_format must be json or text, got %q", c.LogFormat)
	}
	if len(c.Pools) == 0 {
		add("at least one pool is required")
	}
	names := make([]string, 0, len(c.Pools))
	for name := range c.Pools {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		p := c.Pools[name]
		if p == nil {
			add("pool %q: empty definition", name)
			continue
		}
		switch p.Strategy {
		case RoundRobin, LeastConnections, Weighted, ConsistentHash:
		default:
			add("pool %q: unknown strategy %q", name, p.Strategy)
		}
		if p.HashKey != "ip" && !strings.HasPrefix(p.HashKey, "header:") {
			add("pool %q: hash_key must be \"ip\" or \"header:<Name>\", got %q", name, p.HashKey)
		}
		if p.VirtualNodes < 1 {
			add("pool %q: virtual_nodes must be positive", name)
		}
		if len(p.Upstreams) == 0 {
			add("pool %q: at least one upstream is required", name)
		}
		seen := map[string]bool{}
		for _, u := range p.Upstreams {
			parsed, err := url.Parse(u.URL)
			if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
				add("pool %q: invalid upstream url %q", name, u.URL)
				continue
			}
			if seen[parsed.String()] {
				add("pool %q: duplicate upstream %q", name, u.URL)
			}
			seen[parsed.String()] = true
			if u.Weight < 1 {
				add("pool %q: upstream %q weight must be >= 1", name, u.URL)
			}
		}
		if hc := p.HealthCheck; hc != nil {
			if !strings.HasPrefix(hc.Path, "/") {
				add("pool %q: health_check.path must start with /", name)
			}
			if hc.Interval <= 0 || hc.Timeout <= 0 {
				add("pool %q: health_check interval and timeout must be positive", name)
			}
			if hc.HealthyThreshold < 1 || hc.UnhealthyThreshold < 1 {
				add("pool %q: health_check thresholds must be >= 1", name)
			}
		}
		if p.Passive.MaxFailures < 0 || p.Passive.EjectionTime < 0 {
			add("pool %q: passive values must not be negative", name)
		}
	}

	routeNames := map[string]bool{}
	for i, r := range c.Routes {
		if r == nil {
			add("route %d: empty definition", i)
			continue
		}
		if routeNames[r.Name] {
			add("route %q: duplicate name", r.Name)
		}
		routeNames[r.Name] = true
		if _, ok := c.Pools[r.Pool]; !ok {
			add("route %q: unknown pool %q", r.Name, r.Pool)
		}
		if !strings.HasPrefix(r.PathPrefix, "/") {
			add("route %q: path_prefix must start with /", r.Name)
		}
		if r.Timeout < 0 {
			add("route %q: timeout must not be negative", r.Name)
		}
		if *r.Retries < 0 {
			add("route %q: retries must not be negative", r.Name)
		}
	}
	if len(c.Routes) == 0 {
		add("at least one route is required")
	}
	return errors.Join(errs...)
}

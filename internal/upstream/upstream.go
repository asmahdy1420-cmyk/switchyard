// Package upstream models a single backend server and the health state that
// both active checks and passive observation write to.
package upstream

import (
	"fmt"
	"net/url"
	"sync/atomic"
	"time"
)

// PassivePolicy controls ejection after consecutive failed requests.
// MaxFailures of zero disables passive ejection.
type PassivePolicy struct {
	MaxFailures  int
	EjectionTime time.Duration
}

// Upstream is safe for concurrent use. Its state is a handful of atomics so
// the hot path never takes a lock.
type Upstream struct {
	URL    *url.URL
	Weight int

	policy PassivePolicy
	now    func() time.Time

	healthy      atomic.Bool  // written by the active checker
	ejectedUntil atomic.Int64 // unix nanos; written by passive observation
	failures     atomic.Int64 // consecutive failed proxied requests
	inflight     atomic.Int64 // requests currently in progress
}

// New parses rawURL and returns an upstream that starts out healthy.
func New(rawURL string, weight int, policy PassivePolicy) (*Upstream, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("parse upstream %q: %w", rawURL, err)
	}
	if weight < 1 {
		weight = 1
	}
	up := &Upstream{URL: u, Weight: weight, policy: policy, now: time.Now}
	up.healthy.Store(true)
	return up, nil
}

// MustNew is New for tests and static tables; it panics on a bad URL.
func MustNew(rawURL string, weight int, policy PassivePolicy) *Upstream {
	u, err := New(rawURL, weight, policy)
	if err != nil {
		panic(err)
	}
	return u
}

// Name is the label used in logs and metrics.
func (u *Upstream) Name() string { return u.URL.Host }

// Available reports whether the balancer may send traffic here: the active
// checker considers it healthy and it is not currently ejected.
func (u *Upstream) Available() bool {
	return u.healthy.Load() && !u.Ejected()
}

// Healthy reports the active health-check verdict.
func (u *Upstream) Healthy() bool { return u.healthy.Load() }

// SetHealthy records an active health-check verdict and reports whether it
// changed the state.
func (u *Upstream) SetHealthy(v bool) bool {
	return u.healthy.CompareAndSwap(!v, v)
}

// Ejected reports whether passive ejection is in effect.
func (u *Upstream) Ejected() bool {
	return u.now().UnixNano() < u.ejectedUntil.Load()
}

// ReportSuccess resets the consecutive failure count.
func (u *Upstream) ReportSuccess() { u.failures.Store(0) }

// ReportFailure counts a failed request (connection error or 5xx). It
// returns true when this failure caused the upstream to be ejected.
func (u *Upstream) ReportFailure() bool {
	if u.policy.MaxFailures <= 0 {
		return false
	}
	if u.failures.Add(1) < int64(u.policy.MaxFailures) {
		return false
	}
	u.failures.Store(0)
	u.ejectedUntil.Store(u.now().Add(u.policy.EjectionTime).UnixNano())
	return true
}

// Acquire marks the start of a request.
func (u *Upstream) Acquire() { u.inflight.Add(1) }

// Release marks the end of a request.
func (u *Upstream) Release() { u.inflight.Add(-1) }

// InFlight returns the number of requests currently in progress.
func (u *Upstream) InFlight() int64 { return u.inflight.Load() }

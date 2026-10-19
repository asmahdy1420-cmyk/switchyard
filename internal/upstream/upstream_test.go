package upstream

import (
	"testing"
	"time"
)

func TestPassiveEjectionAfterConsecutiveFailures(t *testing.T) {
	now := time.Unix(1_700_000_000, 0)
	u := MustNew("http://10.0.0.1:8080", 1, PassivePolicy{MaxFailures: 3, EjectionTime: 30 * time.Second})
	u.now = func() time.Time { return now }

	if u.ReportFailure() || u.ReportFailure() {
		t.Fatal("ejected before reaching max_failures")
	}
	if !u.Available() {
		t.Fatal("upstream should still be available after two failures")
	}
	if !u.ReportFailure() {
		t.Fatal("third consecutive failure should eject")
	}
	if u.Available() || !u.Ejected() {
		t.Fatal("ejected upstream must not be available")
	}

	now = now.Add(31 * time.Second)
	if !u.Available() {
		t.Fatal("upstream should return after the ejection time")
	}
}

func TestSuccessResetsFailureCount(t *testing.T) {
	u := MustNew("http://10.0.0.1:8080", 1, PassivePolicy{MaxFailures: 2, EjectionTime: time.Minute})
	u.ReportFailure()
	u.ReportSuccess()
	if u.ReportFailure() {
		t.Fatal("failures separated by a success are not consecutive")
	}
	if !u.ReportFailure() {
		t.Fatal("second consecutive failure should eject")
	}
}

func TestPassiveDisabled(t *testing.T) {
	u := MustNew("http://10.0.0.1:8080", 1, PassivePolicy{})
	for i := 0; i < 100; i++ {
		if u.ReportFailure() {
			t.Fatal("max_failures 0 must never eject")
		}
	}
}

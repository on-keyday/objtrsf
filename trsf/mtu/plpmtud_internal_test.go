package mtu

import (
	"testing"
	"time"
)

// The tests in this file are internal because they assert on the transition
// that lowers the estimate, which is deliberately unexported: fallBackToBase is
// the only place in the package where that happens and nothing outside should
// be able to call it. plpmtud_test.go is the external suite and stays that way.

func newTestTracker() *MTUTracker {
	return NewMTUTracker(1200, 1452, 30*time.Second)
}

// fallBackForTest takes the lock fallBackToBase expects its caller to hold.
// Defined here rather than as a production method so the package does not grow
// an exported entry point nothing in production would call.
func (t *MTUTracker) fallBackForTest() {
	t.m.Lock()
	defer t.m.Unlock()
	t.fallBackToBase()
}

// converge drives the binary search to completion against a path that carries
// everything up to pathMTU, and returns the estimate it settled on.
func converge(t *testing.T, tr *MTUTracker, pathMTU int, now time.Time) int {
	t.Helper()
	for i := 0; i < 64; i++ {
		size := tr.Probe(now)
		if size == -1 {
			return tr.CurrentMTU()
		}
		if size <= pathMTU {
			tr.OnACK(now)
			continue
		}
		// Three losses is what the tracker requires to rule a size out.
		tr.OnLost(now)
		tr.OnLost(now)
		tr.OnLost(now)
	}
	t.Fatalf("search did not terminate in 64 probes")
	return 0
}

// issueProbe advances past the re-probe interval and returns the size the
// tracker asks for. A converged tracker answers -1 until that interval has
// elapsed, so a test that wants an outstanding probe has to move the clock.
func issueProbe(t *testing.T, tr *MTUTracker, now time.Time) (int, time.Time) {
	t.Helper()
	now = now.Add(31 * time.Second)
	size := tr.Probe(now)
	if size == -1 {
		t.Fatalf("no probe issued after the re-probe interval elapsed")
	}
	return size, now
}

func TestFallbackLandsOnBaseAndReopensTheSearch(t *testing.T) {
	now := time.Now()
	tr := newTestTracker()
	got := converge(t, tr, 1300, now)
	if got <= 1200 || got > 1300 {
		t.Fatalf("converged at %d, want something in (1200,1300]", got)
	}

	tr.fallBackForTest()

	if tr.CurrentMTU() != 1200 {
		t.Errorf("CurrentMTU = %d after fallback, want the base 1200", tr.CurrentMTU())
	}
	if tr.Fallbacks() != 1 {
		t.Errorf("Fallbacks = %d, want 1", tr.Fallbacks())
	}
	// Reopened, not converged: the next Probe must issue immediately rather
	// than waiting on the re-probe backoff, which is only consulted when
	// low > high. Recovery after a fallback is the binary search, not a timer.
	if size := tr.Probe(now); size <= 1200 {
		t.Errorf("Probe right after a fallback = %d, want a size above the base", size)
	}
}

// A probe issued before a fallback can be acknowledged after it. OnACK raises
// the estimate on `lastProbe > mtu` and reads lastProbe unconditionally, so
// without re-pointing it the late ACK restores the very estimate the fallback
// ruled out -- and announces it through onMTUUpdate as an increase.
func TestLateACKOfAPreFallbackProbeDoesNotRestoreTheEstimate(t *testing.T) {
	now := time.Now()
	tr := newTestTracker()
	converge(t, tr, 1300, now)

	updates := []int{}
	tr.OnMTUUpdate(func(n int) { updates = append(updates, n) })

	inFlight, now := issueProbe(t, tr, now)
	tr.fallBackForTest()
	tr.OnACK(now) // the pre-fallback probe lands late

	if tr.CurrentMTU() != 1200 {
		t.Errorf("CurrentMTU = %d after a late ACK of a %d-byte probe, want 1200",
			tr.CurrentMTU(), inFlight)
	}
	for _, n := range updates {
		if n > 1200 {
			t.Errorf("onMTUUpdate announced %d: a probe from before the fallback was reported as an increase", n)
		}
	}
}

// The OnLost twin of the same race: a late loss must not shrink the range the
// fallback just reopened.
func TestLateLossOfAPreFallbackProbeDoesNotShrinkTheReopenedRange(t *testing.T) {
	now := time.Now()
	tr := newTestTracker()
	converge(t, tr, 1300, now)

	_, now = issueProbe(t, tr, now)
	tr.fallBackForTest()
	tr.OnLost(now)
	tr.OnLost(now)
	tr.OnLost(now)

	if tr.MinCandidate() > tr.MaxCandidate() {
		t.Errorf("search range inverted after a late loss: low=%d high=%d",
			tr.MinCandidate(), tr.MaxCandidate())
	}
	if size := tr.Probe(now); size <= 1200 {
		t.Errorf("Probe = %d after a late loss, want the reopened search to still offer a size", size)
	}
}

func TestTwoFallbacksInARowLeaveAConsistentState(t *testing.T) {
	now := time.Now()
	tr := newTestTracker()
	converge(t, tr, 1300, now)

	tr.fallBackForTest()
	tr.fallBackForTest()

	if tr.CurrentMTU() != 1200 {
		t.Errorf("CurrentMTU = %d, want 1200", tr.CurrentMTU())
	}
	if tr.MinCandidate() > tr.MaxCandidate() {
		t.Errorf("search range inverted: low=%d high=%d", tr.MinCandidate(), tr.MaxCandidate())
	}
	if tr.Fallbacks() != 2 {
		t.Errorf("Fallbacks = %d, want 2", tr.Fallbacks())
	}
}

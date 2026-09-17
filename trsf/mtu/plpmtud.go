package mtu

import (
	"sync"
	"sync/atomic"
	"time"
)

type MTUTracker struct {
	m         sync.RWMutex
	probeSent bool
	mtu       int
	min       int
	max       int
	low       int
	high      int
	lastProbe int

	// Two counters, not one. A search probe and a validation probe answer
	// different questions -- "is this larger size reachable" versus "is the
	// size already in use still reachable" -- so sharing a counter means two
	// lost search probes followed by one lost validation trips the
	// three-strike rule on evidence about two different sizes.
	searchLossCount     int
	validationLossCount int

	lastProbeConverged    time.Time
	reProbeAfterConverged time.Duration
	reprobeBackoffCount   int
	onMTUUpdate           func(int)

	fallbacks atomic.Uint64
}

// maxReprobeBackoff caps the exponential re-probe backoff at 2^n times the
// configured period.
//
// The backoff exists so a path that is not changing is not re-searched
// forever, and it resets in exactly one place: OnACK, when a probe raises the
// MTU. That reset can be structurally unreachable. Any path whose true MTU is
// below max -- a tunnel, a VPN, anything but the ceiling the caller guessed --
// converges to the same value on every re-probe and so never produces an
// increase, and the interval then doubles without bound: 30s * 2^10 is over
// eight hours. That matters because this is also the only way back from a
// search that converged too LOW, which three lost probes at a size the path
// would in fact have carried are enough to cause. Capping at 2^6 keeps the
// futile case cheap (one search per ~32 min at a 30s period) while leaving
// recovery on a timescale a connection can actually reach.
const maxReprobeBackoff = 6

func NewMTUTracker(min, max int, reprobePeriod time.Duration) *MTUTracker {
	return &MTUTracker{
		mtu:                   min,
		min:                   min,
		max:                   max,
		low:                   min + 1,
		high:                  max,
		reProbeAfterConverged: reprobePeriod,
	}
}

func (t *MTUTracker) OnMTUUpdate(fn func(int)) {
	t.onMTUUpdate = fn
}

func (t *MTUTracker) Probe(now time.Time) int {
	t.m.Lock()
	defer t.m.Unlock()
	if t.probeSent {
		return -1
	}
	// すでに探索範囲がなくなっている場合、再探索まで待つ
	if t.low > t.high {
		if !now.After(t.lastProbeConverged.Add(t.reProbeAfterConverged * time.Duration(1<<t.reprobeBackoffCount))) {
			return -1
		}
		if t.mtu >= t.max { // best case
			return -1
		}
		t.low = t.mtu + 1 // reset search range
		t.high = t.max
		t.lastProbeConverged = time.Time{}
		t.searchLossCount = 0
		if t.reprobeBackoffCount < maxReprobeBackoff {
			t.reprobeBackoffCount++
		}
	}
	t.probeSent = true

	// 探索範囲の中間を計算
	// ロス回数が閾値未満の場合、low/high は変化していないので
	// 自然と同じサイズ（lastProbe）が再計算されてリトライ動作になります
	t.lastProbe = (t.low + t.high + 1) / 2
	return t.lastProbe
}

func (t *MTUTracker) mayDetectConverged(now time.Time) {
	if t.low > t.high {
		if t.lastProbeConverged.IsZero() {
			t.lastProbeConverged = now
		}
	}
}

func (t *MTUTracker) OnACK(now time.Time) {
	t.m.Lock()
	defer t.m.Unlock()
	// 成功したので連続ロスカウンタをリセット
	t.searchLossCount = 0

	if t.lastProbe > t.mtu {
		t.mtu = t.lastProbe
		t.reprobeBackoffCount = 0
		if t.onMTUUpdate != nil {
			t.onMTUUpdate(t.mtu)
		}
	}
	t.low = t.lastProbe + 1
	t.probeSent = false
	t.mayDetectConverged(now)
}

func (t *MTUTracker) OnLost(now time.Time) {
	t.m.Lock()
	defer t.m.Unlock()
	t.probeSent = false

	// A probe at or below the current estimate is not search evidence. The
	// search only ever probes ABOVE the estimate, so in the ordinary case this
	// is never true; what it catches is a probe that was outstanding when
	// fallBackToBase re-pointed lastProbe at the new estimate. Without it that
	// late loss sets high = min-1 and inverts the range the fallback just
	// reopened.
	if t.lastProbe <= t.mtu {
		return
	}

	// 失敗をカウント
	t.searchLossCount++

	// 3回連続で失敗した場合のみ、上限を引き下げる
	if t.searchLossCount >= 3 {
		t.high = t.lastProbe - 1
		t.searchLossCount = 0 // 判定確定したのでカウンタをリセット
		t.mayDetectConverged(now)
	}
}

// fallBackToBase is the ONLY place in this package where the estimate
// decreases. Everything else raises it or narrows the search range, and that
// asymmetry is what makes this file readable: a reader asking "where can the
// MTU go down" gets one answer from one grep.
//
// Re-pointing lastProbe at the new estimate is load-bearing, not tidiness.
// OnACK raises on `lastProbe > mtu` and reads lastProbe unconditionally, so a
// probe still in flight when this runs would otherwise be acknowledged
// afterwards and restore exactly the size this ruled out -- announcing it
// through onMTUUpdate as an increase. OnLost's guard above reads the same
// field for the mirror case, so neither needs an epoch counter.
//
// The caller must hold t.m.
func (t *MTUTracker) fallBackToBase() {
	t.mtu = t.min
	t.low = t.min + 1
	t.high = t.max
	t.lastProbe = t.mtu
	t.probeSent = false
	t.searchLossCount = 0
	t.validationLossCount = 0
	t.lastProbeConverged = time.Time{}
	t.fallbacks.Add(1)
	if t.onMTUUpdate != nil {
		t.onMTUUpdate(t.mtu)
	}
}

// Fallbacks is how many times this tracker met a path that stopped carrying a
// size it had already proven. A fleet-wide rise means the detector is firing on
// congestion; zero everywhere means it is not firing at all, and those two are
// the same picture without this number.
func (t *MTUTracker) Fallbacks() uint64 { return t.fallbacks.Load() }

func (t *MTUTracker) CurrentMTU() int {
	t.m.RLock()
	defer t.m.RUnlock()
	return t.mtu
}

func (t *MTUTracker) MinCandidate() int {
	t.m.RLock()
	defer t.m.RUnlock()
	return t.low
}

func (t *MTUTracker) MaxCandidate() int {
	t.m.RLock()
	defer t.m.RUnlock()
	return t.high
}

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

	// The black-hole detector's state. srtt is a function rather than a
	// *congestion.RTTStats so this package does not import congestion; the
	// estimate is read live because the timeout is derived from it.
	srtt          func() time.Duration
	lastLargeACK  time.Time
	lastAnyACK    time.Time
	largeLostSeen bool

	// validating says which question the outstanding probe is asking, because
	// OnACK and OnLost mean different things for each: a search probe moves
	// the range, a validation probe tests the estimate itself.
	validating     bool
	lastValidation time.Time

	fallbacks    atomic.Uint64
	baseUnusable atomic.Uint64
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

// OnSRTT supplies the smoothed round-trip time the black-hole timeout is
// derived from. A setter rather than a constructor parameter, mirroring
// OnMTUUpdate directly above: the owner sets it once immediately after
// construction, and every caller that does not care -- the tests that build a
// tracker to drive the search -- is unaffected. blackHoleTimeout handles the
// unset case.
func (t *MTUTracker) OnSRTT(fn func() time.Duration) {
	t.srtt = fn
}

// The black-hole timeout is k round trips, bounded.
//
// What it has to outlast is a congestion episode, and an episode is measured in
// round trips -- a constant is far too long on a LAN and far too short on a
// satellite path. srtt is well defined at exactly the moment the verdict is
// evaluated, and that is not incidental: the verdict already requires that ACKs
// are arriving, so the estimate is live off the small packets still getting
// through.
//
// PTO would fold in RTT variance for free and is still the wrong unit. It backs
// off exponentially under sustained loss, which is the state this is trying to
// recognise, so N*PTO stretches away from detection exactly when it is needed.
//
// The floor exists because k*srtt on loopback is microseconds. The cap bounds
// worst-case detection latency on a very slow path. All three are meant to be
// replaced by the lossy and bufferbloat measurements, not defended by argument.
const (
	blackHoleRTTs  = 10
	blackHoleFloor = 1 * time.Second
	blackHoleCap   = 30 * time.Second
)

// blackHoleTimeout reads only srtt and constants, so it needs no lock of its
// own and may be called with t.m held.
func (t *MTUTracker) blackHoleTimeout() time.Duration {
	var srtt time.Duration
	if t.srtt != nil {
		srtt = t.srtt()
	}
	switch d := time.Duration(blackHoleRTTs) * srtt; {
	case d < blackHoleFloor:
		return blackHoleFloor
	case d > blackHoleCap:
		return blackHoleCap
	default:
		return d
	}
}

// OnLargePacketACKed records that a packet larger than the base crossed the
// path. A size at or below min is not evidence about SIZE -- those cross a
// shrunk path too, which is the reason min is the fallback target -- but it is
// still evidence the connection is alive.
func (t *MTUTracker) OnLargePacketACKed(size int, now time.Time) {
	t.m.Lock()
	defer t.m.Unlock()
	t.lastAnyACK = now
	if size <= t.min {
		return
	}
	t.lastLargeACK = now
	t.largeLostSeen = false
}

// OnPeerActivity records that something arrived from the peer, which is the
// liveness half of the verdict.
//
// "An ACK reached us" would be the obvious signal and is the WRONG one: in a
// black hole every large packet is lost, so on a bulk transfer nothing of ours
// is acknowledged at all and the liveness clause could never be satisfied on
// exactly the connection this is meant to recognise. What stays true is that
// the peer keeps sending -- its own ACKs of whatever crossed, its own data --
// so the signal is any received packet.
func (t *MTUTracker) OnPeerActivity(now time.Time) {
	t.m.Lock()
	defer t.m.Unlock()
	t.lastAnyACK = now
}

// OnLargePacketLost is the half of the detector that works while a transfer is
// running, and it is deliberately NOT a consecutive-loss count: congestion
// drops whole bursts, so three in a row is an ordinary Tuesday. What
// distinguishes a black hole is that nothing large gets through at all while
// the connection is otherwise alive.
func (t *MTUTracker) OnLargePacketLost(size int, now time.Time) {
	t.m.Lock()
	defer t.m.Unlock()
	if size <= t.min {
		return
	}
	t.largeLostSeen = true
	t.evaluateBlackHole(now)
}

// evaluateBlackHole requires t.m.
func (t *MTUTracker) evaluateBlackHole(now time.Time) {
	if t.mtu <= t.min {
		return // already at the base; there is nowhere to fall
	}
	if !t.largeLostSeen {
		return // nothing large has been lost, so nothing says the size is wrong
	}
	timeout := t.blackHoleTimeout()
	if t.lastLargeACK.IsZero() || now.Sub(t.lastLargeACK) <= timeout {
		return // something large is still getting through
	}
	if t.lastAnyACK.IsZero() || now.Sub(t.lastAnyACK) > timeout {
		return // nothing at all is arriving: a dead path, not a hole
	}
	t.fallBackToBase()
}

// validationInterval is how often a converged tracker re-proves the size it is
// already using.
//
// Separate from reProbeAfterConverged on purpose: that one backs off to ~32 min
// so a path which is not changing is not re-searched forever, and a detector on
// that schedule would take half an hour to notice a wedge.
const validationInterval = 60 * time.Second

// probesDiscoverable reports whether this transport has a path MTU at all.
//
// A stream transport is constructed min == max (peer.MTUForTransport returns
// StreamMTU for both on ws/wss): its size is a framing choice, so there is
// nothing to discover and nothing that can shrink. Gating on the VALUES rather
// than on a transport name is deliberate -- the values are what make the
// question meaningless, and a name-based predicate would be a second place to
// keep in step with the caller.
func (t *MTUTracker) probesDiscoverable() bool { return t.min < t.max }

func (t *MTUTracker) Probe(now time.Time) int {
	t.m.Lock()
	defer t.m.Unlock()
	if t.probeSent || !t.probesDiscoverable() {
		return -1
	}
	// すでに探索範囲がなくなっている場合、再探索か検証まで待つ
	if t.low > t.high {
		switch {
		case now.After(t.lastValidation.Add(validationInterval)):
			// Re-prove the size already in use. This is the ONLY detector that
			// works on a connection with no traffic, and it takes precedence
			// over reopening the search upward: the upward search is an
			// optimisation, and a probe ABOVE the estimate says nothing about
			// whether the estimate itself still crosses.
			//
			// It must also survive mtu >= max, or the ceiling is the one place
			// a shrink can never be noticed.
			t.probeSent = true
			t.validating = true
			t.lastValidation = now
			t.lastProbe = t.mtu
			return t.lastProbe

		case now.After(t.lastProbeConverged.Add(t.reProbeAfterConverged*time.Duration(1<<t.reprobeBackoffCount))) && t.mtu < t.max:
			t.low = t.mtu + 1 // reset search range
			t.high = t.max
			t.lastProbeConverged = time.Time{}
			t.searchLossCount = 0
			if t.reprobeBackoffCount < maxReprobeBackoff {
				t.reprobeBackoffCount++
			}

		default:
			return -1
		}
	}
	t.probeSent = true
	t.validating = false

	// 探索範囲の中間を計算
	// ロス回数が閾値未満の場合、low/high は変化していないので
	// 自然と同じサイズ（lastProbe）が再計算されてリトライ動作になります
	t.lastProbe = (t.low + t.high + 1) / 2
	return t.lastProbe
}

// NextDeadline is when this tracker next has something to do, for the run
// loop's wake computation. Reporting nothing is always safe: it only means the
// loop will not wake on our account.
//
// Without this the machine does not run at all on an idle connection. Probe's
// interval is checked inside Probe, and nothing else arms a timer for it, so
// Probe is reached only when the loop happens to wake for another reason -- and
// an idle connection has its loss timer disarmed and its pacer skipped.
func (t *MTUTracker) NextDeadline(now time.Time) (time.Time, bool) {
	t.m.Lock()
	defer t.m.Unlock()
	if !t.probesDiscoverable() || t.probeSent {
		return time.Time{}, false
	}
	if t.low <= t.high {
		// Searching. No deadline: the probe is issued by the next pass through
		// the send half, which the events that produce sends already reach, and
		// returning "now" here would hand the run loop a deadline in the past
		// on every iteration -- the invariant conn_spin_test.go exists to hold.
		// The timer is for the CONVERGED connection, which is the case that has
		// no other reason to wake.
		return time.Time{}, false
	}
	next := t.lastValidation.Add(validationInterval)
	if t.mtu < t.max {
		reprobe := t.lastProbeConverged.Add(t.reProbeAfterConverged * time.Duration(1<<t.reprobeBackoffCount))
		if reprobe.Before(next) {
			next = reprobe
		}
	}
	if next.Before(now) {
		// Overdue rather than spinning: the wake that follows issues a probe,
		// which sets probeSent and silences this until the probe resolves.
		return now, true
	}
	return next, true
}

// BaseUnusable is how many times the search collapsed with the base itself
// still being lost. Nothing below the base is attempted, so this is the number
// that explains a connection answering small calls and hanging on large ones.
func (t *MTUTracker) BaseUnusable() uint64 { return t.baseUnusable.Load() }

func (t *MTUTracker) mayDetectConverged(now time.Time) {
	if t.low > t.high {
		if t.lastProbeConverged.IsZero() {
			t.lastProbeConverged = now
		}
		if t.lastValidation.IsZero() {
			// The first validation is an interval after convergence, not
			// immediately: the search has just proven this size.
			t.lastValidation = now
		}
	}
}

func (t *MTUTracker) OnACK(now time.Time) {
	t.m.Lock()
	defer t.m.Unlock()
	wasValidating := t.validating
	t.probeSent = false
	t.validating = false

	if wasValidating {
		// The size already in use still crosses. That is the whole answer: it
		// is not a search result, so it neither raises the estimate nor moves
		// the search range.
		t.validationLossCount = 0
		return
	}

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
	t.mayDetectConverged(now)
}

func (t *MTUTracker) OnLost(now time.Time) {
	t.m.Lock()
	defer t.m.Unlock()
	wasValidating := t.validating
	t.probeSent = false
	t.validating = false

	if wasValidating {
		t.validationLossCount++
		if t.validationLossCount < 3 {
			return
		}
		t.validationLossCount = 0
		if t.mtu <= t.min {
			// The base itself is not crossing. Nothing below it is attempted:
			// RFC 9000 s14 -- "QUIC MUST NOT be used if the network path cannot
			// support a maximum datagram size of at least 1200 bytes." Counted
			// so the row explains a connection that answers small calls and
			// hangs on large ones, rather than repaired.
			t.baseUnusable.Add(1)
			return
		}
		t.fallBackToBase()
		return
	}

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
	// Without clearing these the very next large loss re-fires the verdict,
	// because lastLargeACK is still the stale timestamp that produced this one.
	t.largeLostSeen = false
	t.lastLargeACK = time.Time{}
	t.validating = false
	t.lastValidation = time.Time{}
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

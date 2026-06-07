package mtu

import (
	"sync"
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
	lossCount int // 連続ロス回数をカウントする変数を追加

	lastProbeConverged    time.Time
	reProbeAfterConverged time.Duration
	reprobeBackoffCount   int
	onMTUUpdate           func(int)
}

func NewMTUTracker(min, max int, reprobePeriod time.Duration) *MTUTracker {
	return &MTUTracker{
		mtu:                   min,
		min:                   min,
		max:                   max,
		low:                   min + 1,
		high:                  max,
		lossCount:             0, // 初期化
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
		t.lossCount = 0
		t.reprobeBackoffCount++
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
	t.lossCount = 0

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
	// 失敗をカウント
	t.lossCount++

	// 3回連続で失敗した場合のみ、上限を引き下げる
	if t.lossCount >= 3 {
		t.high = t.lastProbe - 1
		t.lossCount = 0 // 判定確定したのでカウンタをリセット
		t.mayDetectConverged(now)
	}
	t.probeSent = false
}

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

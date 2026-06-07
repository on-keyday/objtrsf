package congestion

import (
	"math"
	"time"
)

// from https://github.com/quic-go/quic-go/blob/bbcc555f171035db3bbd1edfe42258fbd2335a99/internal/congestion/pacer.go
type pacer struct {
	budgetAtLastSent  uint64 // in bytes
	lastMTU           uint64
	maxDatagramSize   func() int
	lastSentTime      time.Time
	adjustedBandwidth func() uint64 // in bytes/
}

const maxBurstScale = 10

func newPacer(mtu func() int, bandwidth func() uint64) *pacer {
	p := &pacer{
		budgetAtLastSent: 0,
		lastMTU:          uint64(mtu()),
		maxDatagramSize:  mtu,
		lastSentTime:     time.Time{},
		adjustedBandwidth: func() uint64 {
			bandwidthInBytes := bandwidth() / BytesPerSecond
			return bandwidthInBytes * 5 / 4 // add 25% margin
		},
	}
	p.budgetAtLastSent = p.maxBurstSize()
	return p
}

func (p *pacer) leastMaxBurstSize() uint64 {
	return maxBurstScale * p.lastMTU
}

const nsPerSecond = 1e9

func (p *pacer) Timer() time.Time {
	if p.budgetAtLastSent >= p.lastMTU {
		return time.Time{} // immediately
	}
	bytes := uint64(p.lastMTU - p.budgetAtLastSent) // bytes
	diff := bytes * nsPerSecond                     // bytes * (ns/sec)
	bw := p.adjustedBandwidth()                     // bytes/sec
	d := diff / bw                                  // (bytes * (ns/sec)) / (bytes/sec) = (bytes/sec) * ns / (bytes/sec) = ns
	if diff%bw != 0 {
		d++
	}
	candidate := max(1*time.Millisecond, time.Duration(d)*time.Nanosecond)
	return p.lastSentTime.Add(candidate)
}

func (p *pacer) OnSent(now time.Time, sentSize uint64) {
	p.lastMTU = uint64(p.maxDatagramSize())
	budget := p.Budget(now)
	if sentSize > budget {
		p.budgetAtLastSent = 0
	} else {
		p.budgetAtLastSent = budget - sentSize
	}
	p.lastSentTime = now
}

func (p *pacer) Budget(now time.Time) uint64 {
	if p.lastSentTime.IsZero() {
		return p.maxBurstSize()
	}
	delta := now.Sub(p.lastSentTime)
	added := uint64(0)
	if delta > 0 {
		added = p.timeScaledBandwidth(uint64(delta.Nanoseconds()))
	}
	budget := p.budgetAtLastSent + added
	if added > 0 && budget < p.budgetAtLastSent {
		// overflow
		return p.maxBurstSize()
	}
	return min(budget, p.maxBurstSize())
}

func (p *pacer) maxBurstSize() uint64 {
	leastMaxBurst := p.leastMaxBurstSize()
	// in original code,
	// protocol.MinPacingDelay = 1ms + protocol.TimerGranularity = 1ms is used
	// so we use 2ms here
	burstSize := p.timeScaledBandwidth(uint64((2 * time.Millisecond).Nanoseconds()))
	return max(leastMaxBurst, burstSize)
}

func (p *pacer) timeScaledBandwidth(ns uint64) uint64 {
	bw := p.adjustedBandwidth() // bytes/sec
	if bw == 0 {
		return 0
	}
	maxBurst := p.leastMaxBurstSize()
	var scaled uint64
	if ns > math.MaxUint64/bw {
		scaled = maxBurst
	} else {
		scaled = bw * ns / nsPerSecond // bytes/sec * ns / (ns/sec) = bytes
	}
	return scaled
}

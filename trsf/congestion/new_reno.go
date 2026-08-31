package congestion

import (
	"log/slog"
	"time"

	"github.com/on-keyday/objtrsf/trsf/mtu"
)

type CongestionControl interface {
	CanSend(size int) bool
	RecordSend(size int, now time.Time)
	RecordACK(size int, now time.Time)
	RecordLoss(size int, now time.Time)
	PacingTimer() time.Time
	GetCongestionWindow() int
}

const BytesPerSecond = 8

func (p *newReno) BandwidthEstimate() uint64 {
	smoothedRTT := p.rtt.SRTT
	if smoothedRTT < time.Millisecond {
		smoothedRTT = time.Millisecond
	}
	return uint64(p.cwnd) * uint64(time.Second) / uint64(smoothedRTT) * BytesPerSecond
}

type newReno struct {
	cwnd         int
	ssthresh     int
	lastLossTime time.Time
	mtu          *mtu.MTUTracker

	rtt    *RTTStats // shared with other components
	pacer  *pacer
	logger *slog.Logger
}

func NewNewReno(mtu *mtu.MTUTracker, rtt *RTTStats, logger *slog.Logger) CongestionControl {
	p := &newReno{
		cwnd:     mtu.CurrentMTU() * 2,
		ssthresh: 65536, // initial ssthresh
		rtt:      rtt,
		logger:   logger,
		mtu:      mtu,
	}
	p.pacer = newPacer(mtu.CurrentMTU, p.BandwidthEstimate)
	p.mtu.OnMTUUpdate(func(newMTU int) {
		// adjust cwnd and ssthresh based on new MTU
		if p.cwnd < newMTU*2 {
			p.cwnd = newMTU * 2
		}
	})
	return p
}

func (p *newReno) GetCongestionWindow() int {
	return p.cwnd
}

func (p *newReno) PacingTimer() time.Time {
	return p.pacer.Timer()
}

func (p *newReno) CanSend(bytesInFlight int) bool {
	return bytesInFlight < p.cwnd
}

func (p *newReno) RecordSend(size int, now time.Time) {
	p.pacer.OnSent(now, uint64(size))
}

func (p *newReno) RecordACK(size int, now time.Time) {
	var state string
	if p.cwnd < p.ssthresh {
		// slow start
		p.cwnd += size
		state = "slow_start"
	} else {
		// congestion avoidance
		p.cwnd += size * size / p.cwnd
		if p.cwnd == 0 {
			p.cwnd = 1
		}
		state = "congestion_avoidance"
	}
	p.logger.Debug("NewReno RecordACK", "cwnd", p.cwnd, "ssthresh", p.ssthresh, "state", state)
}

func (p *newReno) RecordLoss(size int, now time.Time) {
	if now.Sub(p.lastLossTime) < time.Second {
		// avoid multiple reductions in short time
		return
	}
	prev := p.cwnd
	p.ssthresh = p.cwnd / 2
	if p.ssthresh < p.mtu.CurrentMTU()*2 {
		p.ssthresh = p.mtu.CurrentMTU() * 2
	}
	// Multiplicative decrease: the window becomes ssthresh, i.e. half of what
	// it was. This line used to reset it to the initial mtu*2 instead, which
	// is the response to a TIMEOUT — nothing coming back at all — and not to
	// one packet looking lost while ACKs keep flowing. The two signals mean
	// different things and this function only ever sees the mild one; the PTO
	// path deliberately does not call it.
	//
	// Measured before the change, pushing 200 MB over a 2 ms link that dropped
	// nothing (tc reported `dropped 0` across 287 MB): the sending connection
	// sat at cwnd 2928-4517 bytes — mtu*2, this reset's value, re-applied
	// about once a second — while the peer connection on the same host reached
	// 336-902 KB. Throughput ≈ cwnd/RTT, so that pinned the transfer near
	// 2 MB/s on a path raw TCP crossed at 508 MB/s.
	p.cwnd = p.ssthresh
	p.lastLossTime = now
	p.logger.Debug("NewReno RecordLoss", "prev_cwnd", prev, "cwnd", p.cwnd, "ssthresh", p.ssthresh)
}

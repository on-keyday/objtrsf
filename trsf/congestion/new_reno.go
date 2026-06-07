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
	p.ssthresh = p.cwnd / 2
	if p.ssthresh < p.mtu.CurrentMTU()*2 {
		p.ssthresh = p.mtu.CurrentMTU() * 2
	}
	p.cwnd = p.mtu.CurrentMTU() * 2 // reset to initial CWND
	p.lastLossTime = now
	p.logger.Debug("NewReno RecordLoss", "cwnd", p.cwnd, "ssthresh", p.ssthresh)
}

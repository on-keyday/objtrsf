package congestion

import (
	"log/slog"
	"time"
)

type RTTStats struct {
	FirstAcked time.Time
	SRTT       time.Duration
	RTTVAR     time.Duration
	MinRTT     time.Duration
	LatestRTT  time.Duration
}

func NewRTTStats(initialRTT time.Duration) *RTTStats {
	return &RTTStats{
		SRTT:   initialRTT,
		RTTVAR: initialRTT / 2,
		MinRTT: time.Duration(1<<63 - 1),
	}
}

func absDuration(a time.Duration) time.Duration {
	if a < 0 {
		return -a
	}
	return a
}

func (rtt *RTTStats) NoACKReceived() bool {
	return rtt.FirstAcked.IsZero()
}

func (rtt *RTTStats) PTO(exponent int) time.Duration {
	return rtt.SRTT + max(4*rtt.RTTVAR, 1) + 25*time.Millisecond*(1<<exponent)
}

func (rtt *RTTStats) UpdateRTT(logger *slog.Logger, measured time.Duration, now time.Time) {
	if rtt.MinRTT > measured {
		rtt.MinRTT = measured
	}
	rtt.LatestRTT = measured
	if rtt.FirstAcked.IsZero() {
		rtt.SRTT = measured
		rtt.RTTVAR = measured / 2
		rtt.FirstAcked = now
		logger.Debug("RTT initialized", "measured", measured, "SRTT", rtt.SRTT, "RTTVAR", rtt.RTTVAR, "MinRTT", rtt.MinRTT, "FirstAcked", rtt.FirstAcked)
	} else {
		rtt.RTTVAR = (3*rtt.RTTVAR + absDuration(rtt.SRTT-measured)) / 4
		rtt.SRTT = (7*rtt.SRTT + measured) / 8
		logger.Debug("RTT updated", "measured", measured, "SRTT", rtt.SRTT, "RTTVAR", rtt.RTTVAR, "MinRTT", rtt.MinRTT)
	}
}

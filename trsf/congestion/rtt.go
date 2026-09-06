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

// noMinRTT is MinRTT before any sample the clock could resolve. It is a
// sentinel and not a measurement, so HasMinRTT is what readers ask.
const noMinRTT = time.Duration(1<<63 - 1)

func NewRTTStats(initialRTT time.Duration) *RTTStats {
	return &RTTStats{
		SRTT:   initialRTT,
		RTTVAR: initialRTT / 2,
		MinRTT: noMinRTT,
	}
}

// HasMinRTT reports whether any round trip has been measured at a resolution
// this machine's clock could express. False is a real state on a host whose
// clock is coarser than the path — see UpdateRTT — and it is NOT the same as a
// min_rtt of zero, which is why it is a separate question rather than a
// comparison against 0.
func (rtt *RTTStats) HasMinRTT() bool { return rtt.MinRTT != noMinRTT }

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

// UpdateRTT folds one sample in, following RFC 9002 sections 5.2 and 5.3.
//
// ackDelay is how long the peer held the largest acked packet before answering.
// Subtracting it is what stops a receiver that batches its ACKs from inflating
// this sender's smoothed_rtt with the far side's own scheduling -- and every
// timer here is derived from SRTT, so without the subtraction the loss detector
// and the PTO both wait for the peer's run loop as well as for the path.
//
// MinRTT is taken from the RAW sample (5.2) and never from the adjusted one. It
// is meant to be a lower bound on the path, and an ackDelay a peer over-reports
// would otherwise drive it below anything real. That also keeps SRTT - MinRTT
// readable as queueing delay rather than as a mixture of queue and peer delay.
//
// RFC 9002 also says to use the LESSER of the reported delay and the peer's
// max_ack_delay once the handshake is confirmed. This transport negotiates no
// such parameter, so that clamp does not exist here and the 5.3 guard is the
// whole defence: a delay large enough to push the sample below min_rtt is
// ignored outright rather than believed or clamped.
func (rtt *RTTStats) UpdateRTT(logger *slog.Logger, measured, ackDelay time.Duration, now time.Time) {
	// A sample of exactly zero is not a measurement; it is a clock that could
	// not resolve the interval. Go's nanotime on Windows reads the interrupt
	// time out of KUSER_SHARED_DATA (runtime/time_windows.h, _INTERRUPT_TIME at
	// 0x7ffe0008), which the kernel updates once per system timer interrupt —
	// 15.625 ms by default, ~0.5 ms on a machine that has lowered it. Measured
	// on this project's Windows host: 199,998 of 200,000 consecutive time.Now()
	// pairs reported the SAME instant, smallest non-zero gap 503.6 µs. Every
	// round trip faster than that reads as 0.
	//
	// MinRTT would latch that zero permanently, and srtt - min_rtt — which this
	// project reads as queueing delay — would then report the whole round trip
	// as queue on every connection that ever saw one fast ACK. SRTT still takes
	// the sample: there a zero is one small error inside an average, where in a
	// minimum it is forever.
	if measured > 0 && rtt.MinRTT > measured {
		rtt.MinRTT = measured
	}
	rtt.LatestRTT = measured
	if rtt.FirstAcked.IsZero() {
		// 5.3, first sample after initialization: smoothed_rtt = latest_rtt,
		// rttvar = latest_rtt / 2 — the RAW sample, unconditionally. The guard
		// below would reach the same value here (min_rtt was just set to this
		// same sample, so nothing can be subtracted), but only by coincidence,
		// and a spec followed by coincidence is one nobody can check.
		rtt.SRTT = measured
		rtt.RTTVAR = measured / 2
		rtt.FirstAcked = now
		logger.Debug("RTT initialized", "measured", measured, "ack_delay", ackDelay, "SRTT", rtt.SRTT, "RTTVAR", rtt.RTTVAR, "MinRTT", rtt.MinRTT, "FirstAcked", rtt.FirstAcked)
	} else {
		// 5.3: adjusted_rtt = latest_rtt - ack_delay, but ONLY when
		// latest_rtt >= min_rtt + ack_delay. Written as a subtraction on the
		// left to keep it in Durations without an overflow to reason about.
		adjusted := measured
		if measured-rtt.MinRTT >= ackDelay {
			adjusted = measured - ackDelay
		}
		rtt.RTTVAR = (3*rtt.RTTVAR + absDuration(rtt.SRTT-adjusted)) / 4
		rtt.SRTT = (7*rtt.SRTT + adjusted) / 8
		logger.Debug("RTT updated", "measured", measured, "ack_delay", ackDelay, "SRTT", rtt.SRTT, "RTTVAR", rtt.RTTVAR, "MinRTT", rtt.MinRTT)
	}
}

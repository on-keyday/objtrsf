package congestion

import (
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/on-keyday/objtrsf/trsf/mtu"
)

const testMTU = 1400

func newTestReno(t *testing.T) (CongestionControl, *mtu.MTUTracker) {
	t.Helper()
	tracker := mtu.NewMTUTracker(1200, testMTU, time.Minute)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	return NewNewReno(tracker, NewRTTStats(10*time.Millisecond), logger), tracker
}

// grow drives cwnd up through ACKs until it is at least want, or fails.
func grow(t *testing.T, cc CongestionControl, want int) {
	t.Helper()
	now := time.Now()
	for i := 0; i < 100000 && cc.GetCongestionWindow() < want; i++ {
		cc.RecordACK(testMTU, now)
	}
	if got := cc.GetCongestionWindow(); got < want {
		t.Fatalf("cwnd never reached %d (stuck at %d)", want, got)
	}
}

// A loss while ACKs are still flowing is the MILD signal: halve the window.
// Resetting it to the initial value is the response to a timeout, and applying
// that to this signal pinned real transfers at two packets — cwnd measured at
// mtu*2 for the length of a 200 MB push over a link that dropped nothing.
func TestRecordLossHalvesRatherThanResetting(t *testing.T) {
	cc, tracker := newTestReno(t)
	grow(t, cc, 400*1024)

	before := cc.GetCongestionWindow()
	cc.RecordLoss(testMTU, time.Now())
	after := cc.GetCongestionWindow()

	if after <= tracker.CurrentMTU()*2 {
		t.Fatalf("cwnd collapsed to the initial window: %d -> %d (initial is %d); "+
			"that is the timeout response, not the loss response",
			before, after, tracker.CurrentMTU()*2)
	}
	if want := before / 2; after != want {
		t.Errorf("cwnd = %d after loss, want %d (half of %d)", after, want, before)
	}
}

// The floor still applies: halving must not drive the window below two
// packets, or the connection cannot make progress at all.
func TestRecordLossFloorsAtTwoPackets(t *testing.T) {
	cc, tracker := newTestReno(t)
	floor := tracker.CurrentMTU() * 2

	cc.RecordLoss(testMTU, time.Now())
	if got := cc.GetCongestionWindow(); got < floor {
		t.Errorf("cwnd = %d, below the two-packet floor %d", got, floor)
	}
}

// One congestion response per second, however many losses are reported. This
// guard predates the change and is what stops a burst of losses from halving
// the window several times for a single congestion event.
func TestRecordLossIsRateLimited(t *testing.T) {
	cc, _ := newTestReno(t)
	grow(t, cc, 400*1024)

	now := time.Now()
	cc.RecordLoss(testMTU, now)
	halved := cc.GetCongestionWindow()

	cc.RecordLoss(testMTU, now.Add(100*time.Millisecond))
	if got := cc.GetCongestionWindow(); got != halved {
		t.Errorf("second loss within a second changed cwnd %d -> %d; the guard is not holding",
			halved, got)
	}

	cc.RecordLoss(testMTU, now.Add(2*time.Second))
	if got := cc.GetCongestionWindow(); got != halved/2 {
		t.Errorf("loss after the guard expired: cwnd = %d, want %d", got, halved/2)
	}
}

// Two losses far apart must not walk the window back up. Guards against a
// halving that reads its own previous ssthresh instead of the live cwnd.
func TestRepeatedLossesDecreaseMonotonically(t *testing.T) {
	cc, _ := newTestReno(t)
	grow(t, cc, 400*1024)

	now := time.Now()
	prev := cc.GetCongestionWindow()
	for i := 1; i <= 4; i++ {
		cc.RecordLoss(testMTU, now.Add(time.Duration(i)*2*time.Second))
		got := cc.GetCongestionWindow()
		if got > prev {
			t.Fatalf("loss %d raised cwnd: %d -> %d", i, prev, got)
		}
		prev = got
	}
}

// RFC 9002 5.2 and 5.3, checked against the text rather than from memory.
//
//	5.2  "An endpoint uses only locally observed times in computing the min_rtt
//	      and does not adjust for acknowledgment delays reported by the peer."
//	5.3  first sample: smoothed_rtt = latest_rtt, rttvar = latest_rtt / 2
//	     otherwise:    if (latest_rtt >= min_rtt + ack_delay):
//	                       adjusted_rtt = latest_rtt - ack_delay
//
// Getting 5.2 backwards is the quiet mistake: min_rtt would sink below anything
// the path can do, and srtt - min_rtt, which this project reads as queueing
// delay, would stop meaning that.
func TestUpdateRTTFollowsRFC9002(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	rtt := NewRTTStats(10 * time.Millisecond)
	now := time.Now()

	// 5.3 first sample: the RAW latest_rtt, even though a delay was reported.
	rtt.UpdateRTT(logger, 50*time.Millisecond, 20*time.Millisecond, now)
	if rtt.MinRTT != 50*time.Millisecond {
		t.Errorf("MinRTT = %v, want the raw 50ms (5.2)", rtt.MinRTT)
	}
	if rtt.SRTT != 50*time.Millisecond {
		t.Errorf("SRTT = %v, want the raw 50ms on the first sample (5.3)", rtt.SRTT)
	}

	// A faster sample lowers min_rtt, from the raw value.
	rtt.UpdateRTT(logger, 10*time.Millisecond, 0, now)
	if rtt.MinRTT != 10*time.Millisecond {
		t.Fatalf("MinRTT = %v, want 10ms", rtt.MinRTT)
	}
	want := (7*50*time.Millisecond + 10*time.Millisecond) / 8
	if rtt.SRTT != want {
		t.Errorf("SRTT = %v, want %v", rtt.SRTT, want)
	}

	// 5.3 subtraction: 60 >= 10 + 20, so the peer's 20ms comes out of the
	// sample — and NOT out of min_rtt, which stays at the raw 10ms.
	before := rtt.SRTT
	rtt.UpdateRTT(logger, 60*time.Millisecond, 20*time.Millisecond, now)
	if rtt.MinRTT != 10*time.Millisecond {
		t.Errorf("MinRTT = %v: an adjusted sample moved it (5.2 says it must not)", rtt.MinRTT)
	}
	if rtt.LatestRTT != 60*time.Millisecond {
		t.Errorf("LatestRTT = %v, want the raw 60ms", rtt.LatestRTT)
	}
	if want := (7*before + 40*time.Millisecond) / 8; rtt.SRTT != want {
		t.Errorf("SRTT = %v, want %v — 60ms less the peer's 20ms", rtt.SRTT, want)
	}

	// The guard: 25 >= 10 + 20 is false, so nothing is subtracted. There is no
	// negotiated max_ack_delay on this transport, so this is the only thing
	// standing between a peer's number and this sender's timers.
	before = rtt.SRTT
	rtt.UpdateRTT(logger, 25*time.Millisecond, 20*time.Millisecond, now)
	if want := (7*before + 25*time.Millisecond) / 8; rtt.SRTT != want {
		t.Errorf("SRTT = %v, want %v — the delay must be ignored, not applied", rtt.SRTT, want)
	}
}

// A peer that reports no delay must behave exactly as before the field existed.
func TestUpdateRTTWithZeroAckDelayIsUnchanged(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	rtt := NewRTTStats(10 * time.Millisecond)
	rtt.UpdateRTT(logger, 40*time.Millisecond, 0, time.Now())
	if rtt.SRTT != 40*time.Millisecond || rtt.MinRTT != 40*time.Millisecond {
		t.Errorf("SRTT=%v MinRTT=%v, want both 40ms", rtt.SRTT, rtt.MinRTT)
	}
}

// A clock that cannot resolve the path reports a round trip of exactly zero.
// That is not a measurement, and MinRTT must not latch it: a minimum is kept
// forever, so one unresolvable sample would tell every later reader that the
// entire round trip is queueing delay.
//
// Measured on this project's Windows host, where Go's nanotime reads the
// interrupt time out of KUSER_SHARED_DATA: 199,998 of 200,000 consecutive
// time.Now() pairs reported the same instant, smallest non-zero gap 503.6 us.
func TestZeroSampleDoesNotLatchMinRTT(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	rtt := NewRTTStats(10 * time.Millisecond)
	now := time.Now()

	if rtt.HasMinRTT() {
		t.Fatal("HasMinRTT before any sample")
	}
	// An unresolvable round trip. SRTT still takes it — one small error inside
	// an average — but the minimum must stay unset rather than become 0.
	rtt.UpdateRTT(logger, 0, 0, now)
	if rtt.HasMinRTT() {
		t.Errorf("a zero sample set MinRTT to %v: a coarse clock would pin it there forever", rtt.MinRTT)
	}
	if rtt.SRTT != 0 {
		t.Errorf("SRTT = %v, want the sample: it is an average and can absorb one", rtt.SRTT)
	}

	// The first sample the clock CAN resolve is the minimum.
	rtt.UpdateRTT(logger, 4*time.Millisecond, 0, now)
	if !rtt.HasMinRTT() || rtt.MinRTT != 4*time.Millisecond {
		t.Errorf("MinRTT = %v (valid %v), want 4ms", rtt.MinRTT, rtt.HasMinRTT())
	}
}

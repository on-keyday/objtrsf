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

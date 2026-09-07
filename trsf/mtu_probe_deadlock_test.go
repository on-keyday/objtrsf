package trsf

import (
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/on-keyday/objtrsf/trsf/congestion"
	"github.com/on-keyday/objtrsf/trsf/mtu"
	"github.com/on-keyday/objtrsf/trsf/wire"
)

// newProbeOnlyHandler puts a single MTU probe in flight and nothing else --
// the state a re-probe reaches on an idle connection, since the reprobe timer
// fires long after the last transfer finished.
func newProbeOnlyHandler(t *testing.T) (*SentPacketHandler, *mtu.MTUTracker) {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	rtt := congestion.NewRTTStats(333 * time.Millisecond)
	tracker := mtu.NewMTUTracker(1200, 1500, 30*time.Second)
	cong := congestion.NewNewReno(tracker, rtt, logger)
	sh := NewSentPacketHandler(logger, rtt, cong)

	size := tracker.Probe(time.Now())
	if size == -1 {
		t.Fatal("setup: tracker refused the first probe")
	}
	sh.OnSent(&SentPacket{
		PacketNumber: 0,
		PacketSize:   size,
		SentTime:     time.Now(),
		IsMTUProbe:   true,
		Kind:         wire.ApplicationPayloadKind_StreamData,
		OnACK:        func(now time.Time) { tracker.OnACK(now) },
		OnLost:       func(now time.Time) { tracker.OnLost(now) },
	})
	return sh, tracker
}

// A probe sent while nothing else is in flight must still be able to be
// declared lost. Probes are deliberately excluded from bytesInFlight so they
// cannot feed congestion control, and setLossDetectionTimer disables the timer
// whenever bytesInFlight == 0 -- so a lone probe arms no timer, the run loop
// parks with a zero deadline, and the probe's OnLost never fires.
func TestLoneMTUProbeArmsLossTimer(t *testing.T) {
	sh, _ := newProbeOnlyHandler(t)
	if lt := sh.LossDetectionTimeout(); lt.IsZero() {
		t.Fatal("lone MTU probe armed no loss timer: nextWakeDeadline parks with no deadline, so this probe's loss can never be detected")
	}
}

// The observable consequence: with the probe's loss undetectable, probeSent
// stays set and the tracker never issues another probe. Discovery stops at
// whatever estimate it had reached, which on a path that drops the first probe
// is the initial (minimum) MTU.
func TestLoneLostMTUProbeDoesNotWedgeDiscovery(t *testing.T) {
	sh, tracker := newProbeOnlyHandler(t)

	// The run loop only enters OnTimeout when the deadline is non-zero, so
	// emulate exactly that gate rather than calling OnTimeout unconditionally.
	now := time.Now().Add(10 * time.Second)
	if lt := sh.LossDetectionTimeout(); !lt.IsZero() && now.After(lt) {
		if _, err := sh.OnTimeout(now); err != nil {
			t.Fatalf("OnTimeout with only a probe in flight: %v", err)
		}
	}

	if got := tracker.Probe(now); got == -1 {
		t.Fatalf("discovery wedged at MTU %d: the lost probe was never declared lost, so no further probe is ever issued", tracker.CurrentMTU())
	}
}

// One probe is worth exactly one loss. The expiry that retires a probe has to
// remove it from sentRanges, because MTUTracker lowers its upper bound after
// three losses and does not care whether they came from three probes or from
// the same one being reported three times -- the latter would collapse the
// bound below the true MTU with no extra packet on the path.
func TestMTUProbeIsDeclaredLostOnce(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	rtt := congestion.NewRTTStats(333 * time.Millisecond)
	tracker := mtu.NewMTUTracker(1200, 1500, 30*time.Second)
	cong := congestion.NewNewReno(tracker, rtt, logger)
	sh := NewSentPacketHandler(logger, rtt, cong)

	size := tracker.Probe(time.Now())
	highBefore := tracker.MaxCandidate()
	losses := 0
	sh.OnSent(&SentPacket{
		PacketNumber: 0,
		PacketSize:   size,
		SentTime:     time.Now(),
		IsMTUProbe:   true,
		Kind:         wire.ApplicationPayloadKind_StreamData,
		OnLost: func(now time.Time) {
			losses++
			tracker.OnLost(now)
		},
	})

	// Three expiries in a row, as the run loop would deliver them if the probe
	// stayed outstanding. Only the first has a probe to retire.
	now := time.Now()
	for i := range 3 {
		now = now.Add(10 * time.Second)
		if lt := sh.LossDetectionTimeout(); !lt.IsZero() && now.After(lt) {
			sh.OnTimeout(now)
		} else if i == 0 {
			t.Fatal("first expiry: no loss timer was armed for the outstanding probe")
		}
	}

	if losses != 1 {
		t.Errorf("probe reported lost %d times, want 1", losses)
	}
	if got := tracker.MaxCandidate(); got != highBefore {
		t.Errorf("upper bound moved to %d (from %d) on a single probe loss", got, highBefore)
	}
}

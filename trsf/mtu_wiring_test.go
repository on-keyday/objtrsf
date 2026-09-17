package trsf

import (
	"testing"
	"time"

	"github.com/on-keyday/objtrsf/trsf/mtu"
)

// Streams is built as a partial literal here, following conn_spin_test.go:
// nextWakeDeadline reads three fields and nothing else, and going through
// NewStreams would drag in a packet-number issuer and a live context for a
// question that is about one function.
func wakeTestStreams(t *testing.T, min, max int) *Streams {
	t.Helper()
	return &Streams{
		sh:          spinTestSetup(t, 50),
		sendTrigger: newWithTriggerQueue[sendStream](),
		mtu:         mtu.NewMTUTracker(min, max, 30*time.Second),
	}
}

// Without a deadline of its own the PLPMTUD machine does not run on an idle
// connection at all. Probe checks its interval inside itself and nothing arms a
// timer for it, while an idle connection has its loss timer disarmed and its
// pacer skipped -- so the validation probe, whose entire job is the idle case,
// would never be issued.
func TestIdleConnectionHasAnMTUWakeDeadline(t *testing.T) {
	s := wakeTestStreams(t, DefaultInitialMTU, DefaultMaxMTU)
	converged := convergeTracker(t, s.mtu, 1300)
	if converged <= DefaultInitialMTU {
		t.Fatalf("tracker did not converge above the base: %d", converged)
	}

	// The converged connection is the one with no other reason to wake, and
	// the validation probe is the only detector that runs on it.
	if _, ok := s.mtu.NextDeadline(time.Now()); !ok {
		t.Fatal("a converged udp connection has no MTU deadline: the validation probe would never be issued")
	}
	deadline, _ := s.nextWakeDeadline()
	if deadline.IsZero() {
		t.Fatal("nextWakeDeadline is zero: the loop would park with nothing to wake it for the MTU")
	}
	if !deadline.After(time.Now()) {
		t.Errorf("nextWakeDeadline = %v is not in the future -> the loop wakes, cannot act, and spins", deadline)
	}
}

// convergeTracker drives the search against a path that carries everything up
// to pathMTU and returns the estimate it settles on.
func convergeTracker(t *testing.T, tr *mtu.MTUTracker, pathMTU int) int {
	t.Helper()
	now := time.Now()
	for i := 0; i < 64; i++ {
		size := tr.Probe(now)
		if size == -1 {
			return tr.CurrentMTU()
		}
		if size <= pathMTU {
			tr.OnACK(now)
			continue
		}
		tr.OnLost(now)
		tr.OnLost(now)
		tr.OnLost(now)
	}
	t.Fatal("search did not terminate in 64 probes")
	return 0
}

// A stream transport is constructed min == max. Waking every idle WebSocket
// connection in a fleet on a 60s timer would be a regression, and the gate that
// prevents it is on the values rather than on a transport name.
func TestIdleStreamTransportHasNoMTUWakeDeadline(t *testing.T) {
	s := wakeTestStreams(t, 16384, 16384)
	if d, ok := s.mtu.NextDeadline(time.Now()); ok {
		t.Errorf("a min==max transport asked for a wake at %v", d)
	}
}

// The deadline must never be in the past, whatever state the tracker is in:
// conn.go's own comment records what a past wake deadline costs -- the loop
// wakes, cannot act, and sees the same past time again, which on the wasm
// runtime starves the JS event loop.
func TestTheMTUDeadlineIsNeverInThePast(t *testing.T) {
	s := wakeTestStreams(t, DefaultInitialMTU, DefaultMaxMTU)
	now := time.Now()
	for i := 0; i < 32; i++ {
		if d, ok := s.mtu.NextDeadline(now); ok && d.Before(now) {
			t.Fatalf("iteration %d: deadline %v is before %v", i, d, now)
		}
		size := s.mtu.Probe(now)
		if size == -1 {
			now = now.Add(61 * time.Second)
			continue
		}
		// Alternate outcomes so the tracker walks through searching,
		// converged, and validating rather than one state repeatedly.
		if i%3 == 0 {
			s.mtu.OnLost(now)
		} else {
			s.mtu.OnACK(now)
		}
		now = now.Add(time.Second)
	}
}

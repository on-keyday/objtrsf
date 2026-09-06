package trsf

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"
)

// waitForState polls the run loop's own account of itself until cond holds, so
// a test waits for the thing it is asserting rather than for a fixed sleep.
func waitForState(t *testing.T, s *Streams, within time.Duration, what string, cond func(*InternalState) bool) *InternalState {
	t.Helper()
	deadline := time.Now().Add(within)
	for {
		st := s.GetInternalState()
		if cond(st) {
			return st
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s (blocks=%d blocked=%v wake_timer=%d wake_send=%d iters=%d)",
				within, what, st.Blocks, time.Duration(st.BlockedNs), st.WakeTimer, st.WakeSend, st.LoopIterations)
		}
		time.Sleep(time.Millisecond)
	}
}

func newIdleStreams(t *testing.T) *Streams {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	tr := NewStreams(ctx, false /* client */, DefaultInitialMTU, DefaultMaxMTU,
		&stubPNIssuer{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	return tr.(*Streams)
}

// An idle run loop parks ONCE and stays there: nothing is queued and nothing is
// in flight, so nextWakeDeadline returns zero and the select has no timer arm.
//
// The park is counted when it is ENTERED and its duration added only when it
// ends, so a loop that is still parked reads Blocks=1 with BlockedNs=0. That
// pairing is the point: it separates "parked right now" from "cycling through
// many short parks", which is the distinction the 540 µs-per-packet
// investigation needs and which LoopIterations alone cannot make.
func TestWakeStatsIdleLoopParksOnceAndReportsNoBlockedTimeYet(t *testing.T) {
	s := newIdleStreams(t)

	st := waitForState(t, s, 2*time.Second, "the loop to park", func(st *InternalState) bool {
		return st.Blocks >= 1
	})
	if st.Blocks != 1 {
		t.Errorf("Blocks = %d, want 1: an idle loop has nothing to wake it", st.Blocks)
	}
	if st.BlockedNs != 0 {
		t.Errorf("BlockedNs = %v, want 0 while still parked: the duration is added on WAKE",
			time.Duration(st.BlockedNs))
	}
	if st.WakeTimer != 0 {
		t.Errorf("WakeTimer = %d, want 0: an idle loop arms no timer", st.WakeTimer)
	}
	if st.ArmedPacer != 0 {
		t.Errorf("ArmedPacer = %d, want 0: the pacer is folded in only when a send could happen",
			st.ArmedPacer)
	}
}

// A park ended by the send trigger is attributed to it, and the time spent
// parked lands in BlockedNs. This is the signature that separates "the
// application is not feeding the transport" from "the transport is waiting on
// a timer of its own".
func TestWakeStatsAttributesASendTriggerWake(t *testing.T) {
	s := newIdleStreams(t)
	waitForState(t, s, 2*time.Second, "the loop to park", func(st *InternalState) bool {
		return st.Blocks >= 1
	})

	const parked = 50 * time.Millisecond
	time.Sleep(parked)
	s.sendTrigger.Notify()

	// Wait for the loop to come back around and park AGAIN, not merely for the
	// wake: Blocks is incremented on entering a park, so observing WakeSend
	// alone catches the loop mid-iteration with the second park not yet
	// counted. Waiting on the re-park is the state where every counter this
	// test reads has settled.
	st := waitForState(t, s, 2*time.Second, "a send-trigger wake and the re-park", func(st *InternalState) bool {
		return st.WakeSend >= 1 && st.Blocks >= 2
	})
	if st.WakeSend != 1 {
		t.Errorf("WakeSend = %d, want 1", st.WakeSend)
	}
	if st.WakeTimer != 0 {
		t.Errorf("WakeTimer = %d, want 0: the park was ended by the trigger, not a deadline", st.WakeTimer)
	}
	// Allow generous slack below the sleep: the assertion is that the parked
	// time is ACCOUNTED, not that the scheduler is punctual.
	if got := time.Duration(st.BlockedNs); got < parked*4/5 {
		t.Errorf("BlockedNs = %v, want at least %v — the park was not accounted", got, parked*4/5)
	}
}

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
// Its blocked time must keep GROWING while it sits there. The first version of
// this counter only accrued on the wake, so a loop parked across a whole
// sampling interval reported zero blocked time — which the operator's BLOCK%
// column rendered as 0%, i.e. "busy", the exact opposite of the truth. The
// reader adds the park in progress, and this is the test that says so.
func TestWakeStatsIdleLoopCountsTheParkItIsStillIn(t *testing.T) {
	s := newIdleStreams(t)

	st := waitForState(t, s, 2*time.Second, "the loop to park", func(st *InternalState) bool {
		return st.Blocks >= 1
	})
	if st.Blocks != 1 {
		t.Errorf("Blocks = %d, want 1: an idle loop has nothing to wake it", st.Blocks)
	}
	if st.WakeTimer != 0 {
		t.Errorf("WakeTimer = %d, want 0: an idle loop arms no timer", st.WakeTimer)
	}
	if st.ArmedPacer != 0 {
		t.Errorf("ArmedPacer = %d, want 0: the pacer is folded in only when a send could happen",
			st.ArmedPacer)
	}

	first := s.GetInternalState().BlockedNs
	const wait = 30 * time.Millisecond
	time.Sleep(wait)
	second := s.GetInternalState().BlockedNs

	if second <= first {
		t.Fatalf("BlockedNs did not grow while parked: %v then %v — a loop parked for the "+
			"whole interval would report as busy", time.Duration(first), time.Duration(second))
	}
	// It grew by roughly the sleep, not by some unrelated amount: the reader is
	// adding the CURRENT park, not double-counting a finished one.
	if grew := time.Duration(second - first); grew < wait*4/5 || grew > wait*3 {
		t.Errorf("BlockedNs grew by %v across a %v sleep — want about the sleep", grew, wait)
	}
	if s.GetInternalState().Blocks != 1 {
		t.Errorf("Blocks moved: the loop woke during the test, which invalidates the reading")
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

// WakeSend says which CHANNEL ended a park; it cannot say what pushed that
// channel. Two pushes with opposite meanings — the application supplying data,
// and an ACK retiring a range — produce the SAME wake, and reading the wake as
// the first of them is the mistake these push counts exist to correct.
//
// So this test asserts what the wake cannot: that the two are counted apart,
// and that the wake count does not distinguish them.
func TestSendPushCountsSeparateWhatWakeSendCannot(t *testing.T) {
	q := newWithTriggerQueue[sendStream]()
	st := &sendStream{}

	// Two pushes with OPPOSITE meanings — the application supplying data, and
	// an ACK retiring a range — plus a second ACK the dedupe will drop.
	q.PushBecause(st, pushApp)
	q.PushBecause(st, pushACK)
	q.PushBecause(st, pushACK)

	c := q.PushCounts()
	if c[pushApp] != 1 {
		t.Errorf("pushApp = %d, want 1", c[pushApp])
	}
	if c[pushACK] != 2 {
		t.Errorf("pushACK = %d, want 2 — the event is counted even when the dedupe drops it", c[pushACK])
	}
	if c[pushSelf] != 0 || c[pushCwnd] != 0 {
		t.Errorf("unrelated reasons moved: self=%d cwnd=%d", c[pushSelf], c[pushCwnd])
	}

	// And here is the whole reason these exist: three events, ONE pending
	// notification. A park ending on that notification cannot say which of the
	// three ended it, so the wake count can never separate them.
	pending := 0
	for {
		select {
		case <-q.Notification():
			pending++
			continue
		default:
		}
		break
	}
	if pending != 1 {
		t.Fatalf("pending notifications = %d, want 1: the premise of the push counts is that "+
			"several events collapse onto one wake", pending)
	}
}

// The counts reach InternalState through a real path rather than only through
// the queue: a created stream is a pushOther, and the reader must see it.
func TestSendPushCountsReachInternalState(t *testing.T) {
	s := newIdleStreams(t)
	before := s.GetInternalState()
	s.CreateSendStream()
	got := s.GetInternalState()
	if d := got.SendPushOther - before.SendPushOther; d != 1 {
		t.Errorf("SendPushOther delta = %d, want 1 for one opened stream", d)
	}
}

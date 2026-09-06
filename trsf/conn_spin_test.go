package trsf

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/on-keyday/objtrsf/trsf/congestion"
	"github.com/on-keyday/objtrsf/trsf/mtu"
	"github.com/on-keyday/objtrsf/trsf/wire"
)

// spinTestSetup builds a SentPacketHandler whose loss timer is armed (a packet
// is in flight, so multiModalTimer is in the future) and whose pacing budget is
// drained with a send timestamped in the past — so PacingTimeout() returns a
// NON-ZERO PAST time. That is the exact precondition of the WebUI upload
// freeze: pacer.Timer keys off budgetAtLastSent and only advances on a send, so
// once drained it returns a fixed past timestamp until the next send.
//
// inFlight controls CanSend(): cwnd starts at 2*MTU = 2400, so a large inFlight
// makes CanSend() false (congestion-blocked), a tiny one keeps it true.
func spinTestSetup(t *testing.T, inFlight int) *SentPacketHandler {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	rtt := congestion.NewRTTStats(333 * time.Millisecond)
	mtuTracker := mtu.NewMTUTracker(1200, 1500, 30*time.Second)
	cong := congestion.NewNewReno(mtuTracker, rtt, logger)
	sh := NewSentPacketHandler(logger, rtt, cong)

	// Arm the loss timer: a packet in flight -> multiModalTimer = now + PTO.
	sh.OnSent(&SentPacket{
		PacketNumber: 0,
		PacketSize:   inFlight,
		SentTime:     time.Now(),
		Kind:         wire.ApplicationPayloadKind_StreamData,
	})

	// Drain the pacing budget far in the past so PacingTimeout() is past+nonzero.
	cong.RecordSend(64*1024, time.Now().Add(-time.Second))

	now := time.Now()
	if pt := sh.PacingTimeout(); pt.IsZero() || !pt.Before(now) {
		t.Fatalf("setup: want non-zero past pacing timer, got %v (now %v)", pt, now)
	}
	if lt := sh.LossDetectionTimeout(); lt.IsZero() || lt.Before(now) {
		t.Fatalf("setup: want future loss timer, got %v (now %v)", lt, now)
	}
	return sh
}

// TestNextWakeDeadlineNoSpinNothingToSend covers the end-of-transfer case: the
// FIN is the only packet in flight, nothing is queued to send. A drained past
// pacing timer must NOT drag the wake deadline into the past (which would make
// the run loop wake immediately every iteration -> busy spin). The loop should
// wait on the future loss timer instead.
func TestNextWakeDeadlineNoSpinNothingToSend(t *testing.T) {
	sh := spinTestSetup(t, 50) // tiny in-flight -> CanSend() true
	s := &Streams{sh: sh, sendTrigger: newWithTriggerQueue[sendStream]()}
	if !sh.CanSend() {
		t.Fatalf("setup: expected CanSend true for tiny in-flight")
	}
	d, fromPacer := s.nextWakeDeadline()
	if !d.IsZero() && d.Before(time.Now()) {
		t.Fatalf("nothing-to-send: wake deadline is in the past (%v) -> busy spin", d)
	}
	if fromPacer {
		t.Errorf("nothing-to-send: deadline reported as the pacer's, but the pacer was not folded in")
	}
}

// TestNextWakeDeadlineNoSpinCongestionBlocked covers the mid-transfer case:
// data is queued but congestion control forbids sending (bytesInFlight >= cwnd).
// Again the drained past pacing timer must not drive the deadline into the past.
func TestNextWakeDeadlineNoSpinCongestionBlocked(t *testing.T) {
	sh := spinTestSetup(t, 8000) // >> cwnd(2400) -> CanSend() false
	s := &Streams{sh: sh, sendTrigger: newWithTriggerQueue[sendStream]()}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mtuTracker := mtu.NewMTUTracker(1200, 1500, 30*time.Second)
	st := newSendStream(context.Background(), mtuTracker, 1,
		newFlowController(InitialFlowWindow), logger, s.sendTrigger)
	s.sendTrigger.Push(st) // data queued
	if sh.CanSend() {
		t.Fatalf("setup: expected CanSend false (congestion-blocked)")
	}
	if s.sendTrigger.Len() == 0 {
		t.Fatalf("setup: expected a queued send stream")
	}
	d, fromPacer := s.nextWakeDeadline()
	if !d.IsZero() && d.Before(time.Now()) {
		t.Fatalf("congestion-blocked: wake deadline is in the past (%v) -> busy spin", d)
	}
	if fromPacer {
		t.Errorf("congestion-blocked: deadline reported as the pacer's, but CanSend() is false")
	}
}

// The two tests above pin the cases where the pacer is NOT consulted. This one
// pins the case where it is, and that nextWakeDeadline SAYS SO — the counter
// built on this answer is what separates "the loop is waiting on the pacer's
// 1 ms floor" from "the loop is waiting on loss detection", and those two look
// identical from a wake count alone.
//
// Here the deadline legitimately IS in the past: a send can happen, so waking
// immediately leads to one rather than to another blocked spin.
func TestNextWakeDeadlineNamesThePacerWhenItIsTheSource(t *testing.T) {
	sh := spinTestSetup(t, 50) // tiny in-flight -> CanSend() true
	s := &Streams{sh: sh, sendTrigger: newWithTriggerQueue[sendStream]()}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	mtuTracker := mtu.NewMTUTracker(1200, 1500, 30*time.Second)
	st := newSendStream(context.Background(), mtuTracker, 1,
		newFlowController(InitialFlowWindow), logger, s.sendTrigger)
	s.sendTrigger.Push(st) // data queued AND CanSend() -> the pacer governs

	d, fromPacer := s.nextWakeDeadline()
	if !fromPacer {
		t.Fatalf("deadline %v not reported as the pacer's, but it governs here", d)
	}
	if want := sh.PacingTimeout(); !d.Equal(want) {
		t.Errorf("deadline = %v, want the pacing timeout %v", d, want)
	}
}

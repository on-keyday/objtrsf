package trsf

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/on-keyday/objtrsf/trsf/mtu"
)

// Built the way retransmit_requeue_test.go builds one, so the scaffolding is
// the same shape the existing retransmit tests already use.
func newSendStreamForTest(t *testing.T) *sendStream {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	tracker := mtu.NewMTUTracker(1200, 1500, time.Minute)
	trigger := newWithTriggerQueue[sendStream]()
	return newSendStream(context.Background(), tracker, 1,
		newFlowController(InitialFlowWindow), logger, trigger)
}

// queueRetransmit puts a range in the state a lost packet leaves it in: on the
// retransmit queue AND still tracked in sentRanges, because sentRanges is
// cleared by an ACK, not by a loss.
func queueRetransmit(t *testing.T, s *sendStream, offset uint64, data []byte, eof bool) *SentRange {
	t.Helper()
	sr := &SentRange{ID: s.id, Offset: offset, Data: data, SentSize: len(data), Eof: eof}
	sr.OnACK = func(now time.Time) { s.onACK(sr, now) }
	sr.OnLost = func(now time.Time) {
		s.retransmitQueue.Push(sr)
		s.sendTrigger.PushBecause(s, pushLoss)
	}
	s.m.Lock()
	s.sentRanges = append(s.sentRanges, sr)
	s.m.Unlock()
	s.retransmitQueue.Push(sr)
	return sr
}

// drain pops fragments at a fixed budget until the queue is empty. A 1200-byte
// chunk at a 600-byte budget yields THREE fragments, not two: the tail is 607
// bytes and does not fit either. Any test that assumes a fragment count is
// asserting arithmetic it does not control.
func drain(t *testing.T, s *sendStream, budget int) []*SentRange {
	t.Helper()
	var out []*SentRange
	for i := 0; i < 64; i++ {
		f := s.triggerPacket(budget)
		if f == nil {
			return out
		}
		out = append(out, f)
	}
	t.Fatalf("retransmit queue did not drain in 64 fragments at a %d budget", budget)
	return nil
}

func trackedRanges(s *sendStream) []*SentRange {
	s.m.Lock()
	defer s.m.Unlock()
	return append([]*SentRange(nil), s.sentRanges...)
}

// A chunk captured at a larger budget must be split to fit the current one.
// Returning it whole is what makes a lowered MTU estimate change nothing for
// data already in flight.
func TestRetransmitSplitsToTheCurrentBudget(t *testing.T) {
	s := newSendStreamForTest(t)
	queueRetransmit(t, s, 0, make([]byte, 1200), false)

	frags := drain(t, s, 600)
	if len(frags) < 2 {
		t.Fatalf("got %d fragments, want the chunk split", len(frags))
	}

	total, offset := 0, uint64(0)
	for i, f := range frags {
		if got := StreamPacketHeaderSize(f.ID, f.Offset, len(f.Data)) + len(f.Data); got > 600 {
			t.Errorf("fragment %d encodes to %d bytes against a 600 budget", i, got)
		}
		if f.Offset != offset {
			t.Errorf("fragment %d starts at %d, want %d: the offsets are not contiguous", i, f.Offset, offset)
		}
		offset += uint64(len(f.Data))
		total += len(f.Data)
	}
	if total != 1200 {
		t.Errorf("split lost or duplicated bytes: %d != 1200", total)
	}
}

// EOF on the head ends the stream at the head's last offset and the remainder
// is never delivered.
func TestSplitPutsEofOnTheTailOnly(t *testing.T) {
	s := newSendStreamForTest(t)
	queueRetransmit(t, s, 0, make([]byte, 1200), true)

	frags := drain(t, s, 600)
	if len(frags) < 2 {
		t.Fatalf("got %d fragments, want the chunk split", len(frags))
	}
	for i, f := range frags[:len(frags)-1] {
		if f.Eof {
			t.Errorf("fragment %d of %d carries Eof: the receiver would end the stream before the rest arrives",
				i, len(frags))
		}
	}
	if !frags[len(frags)-1].Eof {
		t.Error("the last fragment does not carry Eof: the stream never ends")
	}
}

// Splitting is not a one-shot: the budget can drop again before the tail goes
// out.
func TestTheTailSplitsAgainWhenTheBudgetDropsTwice(t *testing.T) {
	s := newSendStreamForTest(t)
	queueRetransmit(t, s, 0, make([]byte, 1200), false)

	a := s.triggerPacket(800)
	b := s.triggerPacket(300)
	c := s.triggerPacket(300)
	if a == nil || b == nil || c == nil {
		t.Fatalf("expected three fragments, got %v %v %v", a != nil, b != nil, c != nil)
	}
	if total := len(a.Data) + len(b.Data) + len(c.Data); total > 1200 {
		t.Fatalf("fragments carry %d bytes, more than the original 1200", total)
	}
	for _, f := range []*SentRange{b, c} {
		if got := StreamPacketHeaderSize(f.ID, f.Offset, len(f.Data)) + len(f.Data); got > 300 {
			t.Errorf("fragment at offset %d encodes to %d bytes against a 300 budget", f.Offset, got)
		}
	}
}

// The original guard's job, which has to survive the rewrite: a budget with no
// room for even one byte pushes back rather than emitting an empty range that
// consumes a queue slot and advances nothing.
func TestABudgetTooSmallForOneBytePushesBack(t *testing.T) {
	s := newSendStreamForTest(t)
	queueRetransmit(t, s, 0, make([]byte, 1200), false)

	if got := s.triggerPacket(2); got != nil && len(got.Data) > 0 {
		t.Fatalf("triggerPacket returned a %d-byte range against a 2-byte budget", len(got.Data))
	}
	if got := s.triggerPacket(600); got == nil {
		t.Fatal("the range was dropped rather than pushed back")
	}
}

// A head with no bytes and no EOF advances nothing and the tail is the same
// range again: the shape that turns the retransmit queue into a spin.
func TestAnEofOnlyRangeIsNotSplit(t *testing.T) {
	s := newSendStreamForTest(t)
	queueRetransmit(t, s, 0, nil, true)

	got := s.triggerPacket(600)
	if got == nil {
		t.Fatal("an Eof-only range was not returned")
	}
	if !got.Eof || len(got.Data) != 0 {
		t.Errorf("got Data=%d Eof=%v, want the range returned whole", len(got.Data), got.Eof)
	}
}

// sentRanges is matched by pointer identity. Splitting must replace the one
// entry with the two fragments: leaving the original retires data that was
// never acknowledged, and appending without removing degrades onACK's O(1) head
// path into its O(n) fallback.
func TestSplitReplacesTheOriginalInSentRanges(t *testing.T) {
	s := newSendStreamForTest(t)
	original := queueRetransmit(t, s, 0, make([]byte, 1200), false)

	frags := drain(t, s, 600)
	if len(frags) < 2 {
		t.Fatalf("got %d fragments, want the chunk split", len(frags))
	}

	tracked := trackedRanges(s)
	for _, sr := range tracked {
		if sr == original {
			t.Error("the original range is still tracked after being split")
		}
	}
	for _, want := range frags {
		found := false
		for _, sr := range tracked {
			if sr == want {
				found = true
			}
		}
		if !found {
			t.Errorf("fragment at offset %d is not tracked in sentRanges", want.Offset)
		}
	}

	now := time.Now()
	for _, f := range frags {
		f.OnACK(now)
	}
	if n := len(trackedRanges(s)); n != 0 {
		t.Errorf("%d ranges still tracked after both fragments were acknowledged", n)
	}
}

// A retransmission is the same bytes again: the original send already consumed
// the window, so two ranges where there was one must not consume it twice.
func TestSplittingDoesNotDoubleCountFlowControl(t *testing.T) {
	s := newSendStreamForTest(t)
	queueRetransmit(t, s, 0, make([]byte, 1200), false)

	before := s.flow.SendableSize()
	drain(t, s, 600)
	if after := s.flow.SendableSize(); after != before {
		t.Errorf("flow window moved from %d to %d across a split retransmission", before, after)
	}
}

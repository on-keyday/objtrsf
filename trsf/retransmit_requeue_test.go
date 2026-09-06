package trsf

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/on-keyday/objtrsf/trsf/mtu"
)

// A retransmit must leave the stream ON the send trigger when it still has data.
//
// triggerPacket returns from the top when it takes something off the retransmit
// queue, which used to skip the re-queue at the bottom. The stream then sat off
// sendTrigger holding a full buffer with an open flow window and an open
// congestion window, and the only thing that puts a stream back in that state is
// sendStream.onACK — so the run loop parked until the next ACK: one round trip,
// about two thousand times a second on a lossy path.
//
// Measured cost on scripts/netem-lab at 1 ms: a 128 MB pull went 3.3 -> 47 MB/s
// and the run-to-run spread collapsed from 70% stdev to 4%.
func TestRetransmitLeavesTheStreamQueuedWhenItStillHasData(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	tracker := mtu.NewMTUTracker(1200, 1500, time.Minute)
	trigger := newWithTriggerQueue[sendStream]()
	st := newSendStream(context.Background(), tracker, 1,
		newFlowController(InitialFlowWindow), logger, trigger)

	// Data waiting to go out, and a range waiting to be retransmitted.
	if err := st.AppendData(false, make([]byte, 64*1024)); err != nil {
		t.Fatalf("AppendData: %v", err)
	}
	st.retransmitQueue.Push(&SentRange{ID: 1, Offset: 0, Data: make([]byte, 100), SentSize: 100})

	// Drain the queue the way the run loop does, so the state under test is
	// "the loop just took this stream off the queue".
	for trigger.Pop() != nil {
	}
	if got := trigger.Len(); got != 0 {
		t.Fatalf("setup: trigger holds %d streams, want 0", got)
	}

	sent := st.triggerPacket(tracker.CurrentMTU() - fixedOverhead)
	if sent == nil {
		t.Fatal("triggerPacket returned nothing; expected the retransmitted range")
	}
	if len(sent.Data) != 100 {
		t.Fatalf("got a %d-byte range, expected the 100-byte retransmit", len(sent.Data))
	}

	if trigger.Len() == 0 {
		t.Error("the stream is OFF the send trigger after a retransmit while it still " +
			"has buffered data: nothing will send it again until an ACK arrives, one " +
			"round trip away")
	}
}

// The mirror: with nothing left to send, a retransmit must NOT re-queue. A
// stream that re-queues itself with no work is the self-notify busy-spin the
// run loop's congestionBlocked comment exists to prevent.
func TestRetransmitDoesNotRequeueAnEmptyStream(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	tracker := mtu.NewMTUTracker(1200, 1500, time.Minute)
	trigger := newWithTriggerQueue[sendStream]()
	st := newSendStream(context.Background(), tracker, 1,
		newFlowController(InitialFlowWindow), logger, trigger)

	st.retransmitQueue.Push(&SentRange{ID: 1, Offset: 0, Data: make([]byte, 100), SentSize: 100})
	for trigger.Pop() != nil {
	}

	if sent := st.triggerPacket(tracker.CurrentMTU() - fixedOverhead); sent == nil {
		t.Fatal("triggerPacket returned nothing; expected the retransmitted range")
	}
	if got := trigger.Len(); got != 0 {
		t.Errorf("an empty stream re-queued itself (%d): that is the self-notify spin", got)
	}
}

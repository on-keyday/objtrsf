package trsf

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/on-keyday/objtrsf/objproto"
	"github.com/on-keyday/objtrsf/trsf/wire"
)

// newQuietStreams builds a Streams whose run loop exits immediately, so a test
// can drive handlePacket directly without racing the loop for the ACK tracker
// (run() drains it via GenerateACK on every pass).
func newQuietStreams(t *testing.T) *Streams {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	return NewStreams(ctx, true, DefaultInitialMTU, DefaultMaxMTU, &stubPNIssuer{}, logger).(*Streams)
}

func encodeDatagram(t *testing.T, payload []byte) []byte {
	t.Helper()
	var pkt wire.StreamAppPacket
	pkt.Header.Kind = wire.ApplicationPayloadKind_Datagram
	if !pkt.SetDatagram(wire.DatagramPacket{Data: payload}) {
		t.Fatal("SetDatagram refused a payload under the datagram kind")
	}
	encoded, err := pkt.EncodeCopy(nil)
	if err != nil {
		t.Fatalf("encode datagram: %v", err)
	}
	return encoded
}

// A received datagram reaches the application through its own queue, and its
// packet number reaches the ACK tracker. The second half is the reason the
// frame is transport-owned: without it the sender could never tell an arrival
// from a drop.
func TestReceivedDatagramIsQueuedAndAckEliciting(t *testing.T) {
	s := newQuietStreams(t)
	payload := []byte("hello datagram")

	s.handlePacket(&objproto.Message{Data: encodeDatagram(t, payload), PacketNumber: 42})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	got, err := s.ReceiveDatagram(ctx)
	if err != nil {
		t.Fatalf("ReceiveDatagram: %v", err)
	}
	if string(got) != string(payload) {
		t.Fatalf("payload = %q, want %q", got, payload)
	}

	ranges, _ := s.pt.GenerateACK()
	found := false
	for _, r := range ranges {
		if 42 >= r.Begin && 42 < r.End {
			found = true
		}
	}
	if !found {
		t.Fatalf("packet number 42 is not queued for acknowledgement (ranges=%v): the sender could never learn this datagram arrived", ranges)
	}

	st := s.GetInternalState()
	if st.DatagramsReceived != 1 {
		t.Errorf("DatagramsReceived = %d, want 1", st.DatagramsReceived)
	}
	if st.DatagramsDroppedReceiveQueue != 0 {
		t.Errorf("DatagramsDroppedReceiveQueue = %d, want 0", st.DatagramsDroppedReceiveQueue)
	}
}

// A datagram does not touch the stream machinery: no stream is created for it,
// which is what separates it from stream data carrying the same bytes.
func TestReceivedDatagramCreatesNoStream(t *testing.T) {
	s := newQuietStreams(t)
	s.handlePacket(&objproto.Message{Data: encodeDatagram(t, []byte("x")), PacketNumber: 1})

	st := s.GetInternalState()
	if st.ActiveReceiveStreams != 0 || st.ActiveSendStreams != 0 {
		t.Fatalf("a datagram created streams (recv=%d send=%d); it carries no stream id and must not",
			st.ActiveReceiveStreams, st.ActiveSendStreams)
	}
}

// The receive queue is bounded and drops rather than blocking. Blocking would
// convert one slow reader into latency for every other packet the run loop
// still has to demultiplex -- and a datagram has no retransmission to wait for.
func TestReceiveQueueOverflowDropsAndCounts(t *testing.T) {
	s := newQuietStreams(t)
	const over = 300 // queue is 256

	for i := range over {
		s.handlePacket(&objproto.Message{
			Data:         encodeDatagram(t, []byte("payload")),
			PacketNumber: objproto.PacketNumber(i),
		})
	}

	st := s.GetInternalState()
	if st.DatagramsDroppedReceiveQueue == 0 {
		t.Fatalf("no drops recorded after %d datagrams into a bounded queue: an overflowing receive buffer must be visible, not silent", over)
	}
	if total := st.DatagramsReceived + st.DatagramsDroppedReceiveQueue; total != over {
		t.Fatalf("received %d + dropped %d = %d, want %d: every datagram must be accounted for exactly once",
			st.DatagramsReceived, st.DatagramsDroppedReceiveQueue, total, over)
	}
}

// The drift guard must stay quiet on a healthy build. It fires only when the
// schema's routing predicate and its union disagree, and a nonzero count means
// some kind is being dropped in silence.
func TestNoUnroutedTransportKindsOnAHealthyBuild(t *testing.T) {
	s := newQuietStreams(t)
	if got := s.GetInternalState().UnroutedTransportKind; got != 0 {
		t.Fatalf("UnroutedTransportKind = %d on a fresh connection, want 0", got)
	}
}

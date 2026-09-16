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

// newLiveStreams builds a Streams whose run loop is running, which is what the
// send path needs: SendDatagram only queues, and the loop is what emits.
func newLiveStreams(t *testing.T) *Streams {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return NewStreams(ctx, false, DefaultInitialMTU, DefaultMaxMTU, &stubPNIssuer{}, logger).(*Streams)
}

// fillCongestionWindow puts enough in flight that CanSend() is false. It uses
// the handler directly rather than real streams so the test states the
// condition it needs instead of arranging it by side effect.
func fillCongestionWindow(t *testing.T, s *Streams) {
	t.Helper()
	for i := 0; s.sh.CanSend() && i < 1000; i++ {
		s.sh.OnSent(&SentPacket{
			PacketNumber:    objproto.PacketNumber(100000 + i),
			PacketSize:      DefaultInitialMTU,
			SentTime:        time.Now(),
			Kind:            wire.ApplicationPayloadKind_StreamData,
			PathEvidence:    true,
			Retransmittable: true,
			OnLost:          func(now time.Time) {},
		})
	}
	if s.sh.CanSend() {
		t.Fatal("setup: could not close the congestion window")
	}
}

// nextDatagramAction drains SendActions until one carries a datagram, so the
// assertion does not race the MTU probe the loop also emits.
func nextDatagramAction(t *testing.T, s *Streams) []byte {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for range 16 {
		action := s.Recv(ctx)
		if action == nil {
			t.Fatal("the run loop produced no further SendAction before the deadline")
		}
		if action.Data == nil {
			continue
		}
		var pkt wire.StreamAppPacket
		if err := pkt.DecodeExact(action.Data); err != nil {
			continue
		}
		if dg := pkt.Datagram(); dg != nil {
			return dg.Data
		}
	}
	t.Fatal("no datagram appeared among the emitted SendActions")
	return nil
}

// The size ceiling is trsf's to state, so no consumer restates
// CurrentMTU - fixedOverhead - frame header. An oversized payload is refused
// outright -- there is no fragmentation by design -- and counted, because
// otherwise nothing would explain why a tunnel drops large packets.
func TestMaxDatagramSizeRefusesAndCountsOversize(t *testing.T) {
	s := newLiveStreams(t)
	max := s.MaxDatagramSize()
	if max != DefaultInitialMTU-fixedOverhead-datagramFrameOverhead {
		t.Fatalf("MaxDatagramSize() = %d, want %d", max, DefaultInitialMTU-fixedOverhead-datagramFrameOverhead)
	}

	if err := s.SendDatagram(make([]byte, max+1)); err != ErrDatagramTooLarge {
		t.Fatalf("oversized send returned %v, want ErrDatagramTooLarge", err)
	}
	if got := s.GetInternalState().DatagramsDroppedOversize; got != 1 {
		t.Fatalf("DatagramsDroppedOversize = %d, want 1", got)
	}
}

// A controlled datagram meeting a closed window is DROPPED, not parked.
// Parking trades a visible drop for invisible latency and unbounded buffering,
// and for a tunnel the drop is what the path would have done anyway.
func TestControlledDatagramIsDroppedWhenTheWindowIsClosed(t *testing.T) {
	s := newLiveStreams(t)
	fillCongestionWindow(t, s)

	if err := s.SendDatagram([]byte("payload")); err != ErrCongestionBlocked {
		t.Fatalf("send with a closed window returned %v, want ErrCongestionBlocked", err)
	}
	st := s.GetInternalState()
	if st.DatagramsDroppedCongestion != 1 {
		t.Errorf("DatagramsDroppedCongestion = %d, want 1", st.DatagramsDroppedCongestion)
	}
	if st.DatagramsSent != 0 {
		t.Errorf("DatagramsSent = %d, want 0: the payload was dropped, not sent", st.DatagramsSent)
	}
}

// The uncontrolled mode exists precisely so a bounded-rate sender is not made
// to wait on a window it is not part of. The payload must actually reach the
// wire, which means the loop's drain has to sit before the congestion gate.
func TestUncontrolledDatagramIsSentWithTheWindowClosed(t *testing.T) {
	s := newLiveStreams(t)
	fillCongestionWindow(t, s)

	payload := []byte("uncontrolled payload")
	if err := s.SendDatagramUncontrolled(payload); err != nil {
		t.Fatalf("uncontrolled send with a closed window: %v", err)
	}

	if got := nextDatagramAction(t, s); string(got) != string(payload) {
		t.Fatalf("emitted payload = %q, want %q", got, payload)
	}
	if got := s.GetInternalState().DatagramsSentUncontrolled; got != 1 {
		t.Fatalf("DatagramsSentUncontrolled = %d, want 1", got)
	}
}

// The ordinary path: an open window, and the payload comes out intact.
func TestControlledDatagramReachesTheWire(t *testing.T) {
	s := newLiveStreams(t)
	payload := []byte("controlled payload")
	if err := s.SendDatagram(payload); err != nil {
		t.Fatalf("SendDatagram: %v", err)
	}

	if got := nextDatagramAction(t, s); string(got) != string(payload) {
		t.Fatalf("emitted payload = %q, want %q", got, payload)
	}
	st := s.GetInternalState()
	if st.DatagramsSent != 1 {
		t.Errorf("DatagramsSent = %d, want 1", st.DatagramsSent)
	}
	if st.DatagramsSentUncontrolled != 0 {
		t.Errorf("DatagramsSentUncontrolled = %d, want 0", st.DatagramsSentUncontrolled)
	}
}

// A sent datagram occupies the window when it is controlled, and does not when
// it is not. This is the property the two modes are named for.
func TestOnlyControlledDatagramsOccupyTheWindow(t *testing.T) {
	controlled := newLiveStreams(t)
	if err := controlled.SendDatagram([]byte("payload")); err != nil {
		t.Fatalf("SendDatagram: %v", err)
	}
	nextDatagramAction(t, controlled)
	if got := controlled.GetInternalState().BytesInFlight; got == 0 {
		t.Error("a congestion-controlled datagram left BytesInFlight at 0: it is not occupying the window it is subject to")
	}

	uncontrolled := newLiveStreams(t)
	if err := uncontrolled.SendDatagramUncontrolled([]byte("payload")); err != nil {
		t.Fatalf("SendDatagramUncontrolled: %v", err)
	}
	nextDatagramAction(t, uncontrolled)
	if got := uncontrolled.GetInternalState().BytesInFlight; got != 0 {
		t.Errorf("BytesInFlight = %d after an uncontrolled datagram, want 0", got)
	}
}

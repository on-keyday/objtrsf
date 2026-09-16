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

// newHandlerWithOne puts exactly one packet in flight and nothing else, so each
// test below exercises the "this is the only thing outstanding" state that the
// timer and PTO paths treat specially.
//
// Kind is StreamData deliberately: every accounting decision under test is
// driven by the four property flags, not by the wire kind, so none of these
// tests wait on the datagram kind existing.
func newHandlerWithOne(t *testing.T, p SentPacket) (*SentPacketHandler, *int) {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	rtt := congestion.NewRTTStats(333 * time.Millisecond)
	tracker := mtu.NewMTUTracker(1200, 1500, 30*time.Second)
	cong := congestion.NewNewReno(tracker, rtt, logger)
	sh := NewSentPacketHandler(logger, rtt, cong)

	lost := new(int)
	p.PacketNumber = 0
	p.SentTime = time.Now()
	p.Kind = wire.ApplicationPayloadKind_StreamData
	p.OnLost = func(now time.Time) { *lost++ }
	sh.OnSent(&p)
	return sh, lost
}

// uncontrolledDatagram is the packet class this whole split exists for: outside
// congestion control, but still evidence about the path, and nothing re-sends it.
func uncontrolledDatagram() SentPacket {
	return SentPacket{
		PacketSize:       1000,
		CongestionExempt: true,
		PathEvidence:     true,
		Retransmittable:  false,
	}
}

// controlledDatagram is the other mode: it occupies the window and its loss is a
// real congestion signal, but it is still never re-sent.
func controlledDatagram() SentPacket {
	return SentPacket{
		PacketSize:       1000,
		CongestionExempt: false,
		PathEvidence:     true,
		Retransmittable:  false,
	}
}

// fireTimer runs the expiry the way the run loop does -- only when the deadline
// is set and has passed -- rather than calling OnTimeout unconditionally.
func fireTimer(t *testing.T, sh *SentPacketHandler, at time.Time) (bool, error) {
	t.Helper()
	lt := sh.LossDetectionTimeout()
	if lt.IsZero() {
		t.Fatal("no timer was set, so the run loop would park with no deadline and this packet's loss could never be detected")
	}
	if !at.After(lt) {
		t.Fatalf("timer at %v has not passed by %v", lt, at)
	}
	return sh.OnTimeout(at)
}

// A congestion-exempt packet is in neither bytesInFlight nor
// mtuProbesOutstanding, so the pre-existing stop condition in
// setLossDetectionTimer would leave the loop parked with no deadline. That is
// the same hole 7db84f6 closed for a lone MTU probe, reopened by a second class
// of exempt packet.
func TestLoneExemptPacketSetsLossTimer(t *testing.T) {
	sh, _ := newHandlerWithOne(t, uncontrolledDatagram())
	if _, inFlight, _, _, _ := sh.GetInternal(); inFlight != 0 {
		t.Fatalf("bytesInFlight = %d, want 0: an exempt packet must not enter the window", inFlight)
	}
	if lt := sh.LossDetectionTimeout(); lt.IsZero() {
		t.Fatal("a lone congestion-exempt packet set no timer: its OnLost can never fire, so nothing can count the drop")
	}
}

// Exempt means "does not feed congestion control". It does NOT mean "invisible":
// a datagram's loss is the only evidence separating a path drop from one this
// process made itself, so it must reach loss.Packets while leaving the window
// alone.
func TestExemptPacketLossIsEvidenceButNotCongestion(t *testing.T) {
	sh, lost := newHandlerWithOne(t, uncontrolledDatagram())
	_, _, cwndBefore, _, _ := sh.GetInternal()

	if _, err := fireTimer(t, sh, time.Now().Add(10*time.Second)); err != nil {
		t.Fatalf("OnTimeout: %v", err)
	}

	if *lost != 1 {
		t.Fatalf("OnLost fired %d times, want 1", *lost)
	}
	_, inFlight, cwndAfter, _, ls := sh.GetInternal()
	if ls.Packets != 1 {
		t.Errorf("loss.Packets = %d, want 1: a datagram's loss IS evidence about the path", ls.Packets)
	}
	if ls.Events != 0 {
		t.Errorf("loss.Events = %d, want 0: an exempt packet must not trigger a congestion response", ls.Events)
	}
	if cwndAfter != cwndBefore {
		t.Errorf("cwnd moved %d -> %d on an exempt packet's loss", cwndBefore, cwndAfter)
	}
	if inFlight != 0 {
		t.Errorf("bytesInFlight = %d, want 0", inFlight)
	}
}

// The PTO path used to end in errors.New("BUG: no packets in flight") whenever
// bytesInFlight was zero and no MTU probe had been retired. An exempt packet
// reaches exactly that state legitimately, and reporting it as a bug would put
// a permanent error in the log of every connection carrying uncontrolled
// datagrams.
func TestLoneExemptPacketExpiryIsNotReportedAsABug(t *testing.T) {
	sh, _ := newHandlerWithOne(t, uncontrolledDatagram())
	isPTO, err := fireTimer(t, sh, time.Now().Add(10*time.Second))
	if err != nil {
		t.Fatalf("expiry of a lone exempt packet returned an error: %v", err)
	}
	if isPTO {
		t.Error("expiry reported a retransmission was triggered, but nothing here can be retransmitted")
	}
}

// A packet nothing re-sends must be RETIRED when its loss is declared, not left
// in sentRanges. declareLostUnretransmittable's predecessor recorded why: a
// later expiry would otherwise report the same packet as a second loss.
func TestUnretransmittablePacketIsRetiredOnExpiry(t *testing.T) {
	sh, lost := newHandlerWithOne(t, uncontrolledDatagram())
	if _, err := fireTimer(t, sh, time.Now().Add(10*time.Second)); err != nil {
		t.Fatalf("first expiry: %v", err)
	}
	if got := len(mustSentPackets(sh)); got != 0 {
		t.Fatalf("%d packets still outstanding after their loss was declared: a later expiry would count them again", got)
	}
	if *lost != 1 {
		t.Fatalf("OnLost fired %d times, want exactly 1", *lost)
	}
}

// The congestion-controlled mode is the other half: it DID occupy the window, so
// retiring it must release those bytes and take the congestion response that a
// real loss calls for.
func TestControlledDatagramPaysCongestionWhenRetired(t *testing.T) {
	sh, _ := newHandlerWithOne(t, controlledDatagram())
	if _, inFlight, _, _, _ := sh.GetInternal(); inFlight != 1000 {
		t.Fatalf("bytesInFlight = %d, want 1000: a controlled datagram occupies the window", inFlight)
	}

	if _, err := fireTimer(t, sh, time.Now().Add(10*time.Second)); err != nil {
		t.Fatalf("OnTimeout: %v", err)
	}

	_, inFlight, _, _, ls := sh.GetInternal()
	if inFlight != 0 {
		t.Errorf("bytesInFlight = %d after retiring a controlled datagram, want 0", inFlight)
	}
	if ls.Events != 1 {
		t.Errorf("loss.Events = %d, want 1: a controlled datagram's loss is a real congestion signal", ls.Events)
	}
	if ls.Packets != 1 {
		t.Errorf("loss.Packets = %d, want 1", ls.Packets)
	}
}

// PTO picks a packet to retransmit. Choosing one that nothing re-sends wastes
// the expiry and reports a retransmission that never happened.
func TestPTORetransmitsOnlyRetransmittablePackets(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	rtt := congestion.NewRTTStats(333 * time.Millisecond)
	tracker := mtu.NewMTUTracker(1200, 1500, 30*time.Second)
	sh := NewSentPacketHandler(logger, rtt, congestion.NewNewReno(tracker, rtt, logger))

	datagramLost, streamLost := 0, 0
	sh.OnSent(&SentPacket{
		PacketNumber: 0, PacketSize: 1000, SentTime: time.Now(),
		Kind: wire.ApplicationPayloadKind_StreamData,
		// A controlled datagram: in flight, so it alone would set the PTO timer.
		CongestionExempt: false, PathEvidence: true, Retransmittable: false,
		OnLost: func(now time.Time) { datagramLost++ },
	})
	sh.OnSent(&SentPacket{
		PacketNumber: 1, PacketSize: 1000, SentTime: time.Now(),
		Kind:             wire.ApplicationPayloadKind_StreamData,
		CongestionExempt: false, PathEvidence: true, Retransmittable: true,
		OnLost: func(now time.Time) { streamLost++ },
	})

	isPTO, err := fireTimer(t, sh, time.Now().Add(10*time.Second))
	if err != nil {
		t.Fatalf("OnTimeout: %v", err)
	}
	if !isPTO {
		t.Fatal("PTO reported no retransmission although a retransmittable packet was outstanding")
	}
	if streamLost != 1 {
		t.Errorf("the retransmittable packet's OnLost fired %d times, want 1", streamLost)
	}
	if datagramLost != 1 {
		t.Errorf("the unretransmittable packet's OnLost fired %d times, want 1 (retired by the same expiry)", datagramLost)
	}
}

// An exempt packet's bytes never entered the window, so acknowledging them must
// not grow it. The send side already excludes them from RecordSend and the loss
// side from RecordLoss; ACK was the odd one out. At one MTU probe per 30s
// reprobe period that was unmeasurable, but a high-rate uncontrolled sender
// would inflate the window of the controlled streams sharing its connection --
// a flow that ignores congestion enlarging everyone else's share.
func TestExemptAckDoesNotGrowCongestionWindow(t *testing.T) {
	sh, _ := newHandlerWithOne(t, uncontrolledDatagram())
	_, _, cwndBefore, _, _ := sh.GetInternal()

	if err := sh.ReceiveACK(time.Now(), []Range{{Begin: 0, End: 1}}, 0); err != nil {
		t.Fatalf("ReceiveACK: %v", err)
	}

	_, inFlight, cwndAfter, _, _ := sh.GetInternal()
	if cwndAfter != cwndBefore {
		t.Errorf("cwnd %d -> %d on acking a congestion-exempt packet: it grew on bytes that never occupied it", cwndBefore, cwndAfter)
	}
	if inFlight != 0 {
		t.Errorf("bytesInFlight = %d, want 0: retiring an exempt packet must not subtract from a window it never entered", inFlight)
	}
}

// The control: a packet that DID occupy the window still grows it on ACK.
func TestControlledAckStillGrowsCongestionWindow(t *testing.T) {
	sh, _ := newHandlerWithOne(t, controlledDatagram())
	_, _, cwndBefore, _, _ := sh.GetInternal()

	if err := sh.ReceiveACK(time.Now(), []Range{{Begin: 0, End: 1}}, 0); err != nil {
		t.Fatalf("ReceiveACK: %v", err)
	}

	if _, _, cwndAfter, _, _ := sh.GetInternal(); cwndAfter <= cwndBefore {
		t.Fatalf("cwnd %d -> %d: a packet that occupied the window must still grow it", cwndBefore, cwndAfter)
	}
}

// mustSentPackets returns the handler's outstanding set.
func mustSentPackets(sh *SentPacketHandler) []InternalSentPacket {
	packets, _, _, _, _ := sh.GetInternal()
	return packets
}

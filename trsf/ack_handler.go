package trsf

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/on-keyday/objtrsf/objproto"
	"github.com/on-keyday/objtrsf/trsf/congestion"
	"github.com/on-keyday/objtrsf/trsf/wire"
)

// QUICではpacket number spaceごとにACKを管理する必要があるが
// こちらは必要がないため簡素化
type SentPacketHandler struct {
	m             sync.Mutex
	largestSent   uint64
	largestAcked  uint64
	bytesInFlight int

	// mtuProbesOutstanding counts the MTU probes sitting in sentRanges
	// awaiting a verdict. It is a separate count rather than part of
	// bytesInFlight on purpose -- a probe must never reach congestion control
	// -- but bytesInFlight is ALSO what setLossDetectionTimer reads to decide
	// whether anything is worth waking for. Without this count a probe with no
	// data beside it leaves the timer disabled, the run loop parks with no
	// deadline, and that probe's loss is unobservable for the life of the
	// connection.
	mtuProbesOutstanding int

	// exemptOutstanding counts congestion-exempt packets sitting in sentRanges
	// whose loss is still worth detecting. It exists for exactly the reason
	// mtuProbesOutstanding does, and generalises it: such a packet is absent
	// from bytesInFlight, so without a term of its own setLossDetectionTimer
	// stops the timer while it is still outstanding and its OnLost never fires.
	exemptOutstanding int

	sentRanges []*SentPacket
	logger     *slog.Logger
	rtt        *congestion.RTTStats

	cong congestion.CongestionControl

	lossTime        time.Time
	multiModalTimer time.Time

	ptoCount int

	loss         LossStats
	declaredLost []objproto.PacketNumber

	*trigger
}

// LossStats is the loss detector's account of itself.
//
// Spurious is the diagnostic one. A packet this handler gave up on and then
// saw acknowledged was never lost, so the congestion response taken on it was
// taken on nothing — and since that response is what sets the sending rate,
// a Spurious that climbs with the transfer says the rate is being governed by
// a measurement error rather than by the path.
//
// Events is separate from Packets because one call to detectLost can retire
// several packets while producing a single congestion response; it is Events
// that the window pays for.
type LossStats struct {
	Events   int // congestion responses taken (RecordLoss calls)
	Packets  int // packets declared lost that were PathEvidence
	Spurious int // ...of which an ACK arrived afterwards
}

// How many declared-lost packet numbers to keep for the spurious check. It
// only has to outlive the reordering/ACK-delay window, not the connection, so
// this is a ring rather than a growing set: an unbounded one on a long
// transfer would be a leak in the name of diagnostics.
const maxDeclaredLostTracked = 256

func (ah *SentPacketHandler) rememberDeclaredLost(pn objproto.PacketNumber) {
	if len(ah.declaredLost) >= maxDeclaredLostTracked {
		copy(ah.declaredLost, ah.declaredLost[1:])
		ah.declaredLost = ah.declaredLost[:len(ah.declaredLost)-1]
	}
	ah.declaredLost = append(ah.declaredLost, pn)
}

// noteSpuriousLoss counts, and forgets, every declared-lost packet number the
// incoming ACK covers. Called before detectAck prunes sentRanges, though the
// order does not matter: these packets were already removed from sentRanges
// when they were declared lost, which is exactly why the ACK for them would
// otherwise pass unnoticed.
func (ah *SentPacketHandler) noteSpuriousLoss(ranges []Range) {
	if len(ah.declaredLost) == 0 {
		return
	}
	remain := ah.declaredLost[:0]
	for _, pn := range ah.declaredLost {
		covered := false
		for _, rg := range ranges {
			if pn >= rg.Begin && pn < rg.End {
				covered = true
				break
			}
		}
		if covered {
			ah.loss.Spurious++
			ah.logger.Debug("spurious loss: declared lost, later acked", "pn", pn,
				"spurious_total", ah.loss.Spurious, "loss_events", ah.loss.Events)
			continue
		}
		remain = append(remain, pn)
	}
	ah.declaredLost = remain
}

func NewSentPacketHandler(logger *slog.Logger, rtt *congestion.RTTStats, cong congestion.CongestionControl) *SentPacketHandler {
	return &SentPacketHandler{
		logger:  logger,
		rtt:     rtt,
		cong:    cong,
		trigger: newTrigger(),
	}
}

// SentPacket describes one packet awaiting a verdict. Four of its fields are
// independent properties that an MTU probe happened to share, and that used to
// ride IsMTUProbe alone. They came apart because a datagram wants some of them
// and not others -- notably it wants to be exempt from congestion control while
// still being evidence about the path, which is the combination a single flag
// could not express.
type SentPacket struct {
	OnACK        func(now time.Time)
	OnLost       func(now time.Time)
	PacketSize   int
	StreamID     StreamID
	PacketNumber objproto.PacketNumber
	SentTime     time.Time

	// CongestionExempt keeps this packet out of the congestion controller
	// entirely: bytesInFlight, RecordSend, RecordACK and RecordLoss. An MTU
	// probe and an uncontrolled datagram set it.
	CongestionExempt bool

	// PathEvidence says this packet's fate tells us something about the path,
	// so its loss belongs in loss.Packets and in the spurious accounting. An
	// MTU probe CLEARS it -- a probe's loss is the probe's answer about size,
	// not a statement about the path -- while a datagram sets it, because a
	// datagram's loss is the only thing separating a path drop from one this
	// process made itself.
	PathEvidence bool

	// Retransmittable says something will re-send this packet's contents if it
	// is declared lost, which is what lets PTO leave it in sentRanges and try
	// again. Clearing it does two things that the OnLost callback alone cannot:
	// PTO stops choosing it as the packet to retransmit, and it is RETIRED when
	// its loss is declared, so a later expiry cannot report the same packet as
	// a second loss. See declareLostUnretransmittable.
	Retransmittable bool

	// IsMTUProbe now means only "this is an MTU probe", for mtuProbesOutstanding
	// and for the tracker's own bookkeeping. It no longer decides congestion
	// participation, loss evidence, or retransmission; a probe states those
	// three separately.
	IsMTUProbe bool

	Kind wire.ApplicationPayloadKind
}

// GetInternal reports the handler's own state. The RTT stats travel as one
// value rather than as loose durations: MinRTT joined SRTT and RTTVAR here, and
// a seventh positional return is how a caller comes to pass them in the wrong
// order.
func (ah *SentPacketHandler) GetInternal() ([]InternalSentPacket, int, int, congestion.RTTStats, LossStats) {
	ah.m.Lock()
	defer ah.m.Unlock()
	var sentRanges []InternalSentPacket = make([]InternalSentPacket, 0, len(ah.sentRanges))
	for _, p := range ah.sentRanges {
		sentRanges = append(sentRanges, InternalSentPacket{
			SentTime:         p.SentTime,
			PacketSize:       p.PacketSize,
			IsMTUProbe:       p.IsMTUProbe,
			CongestionExempt: p.CongestionExempt,
			PathEvidence:     p.PathEvidence,
			Retransmittable:  p.Retransmittable,
			Kind:             p.Kind,
			StreamID:         p.StreamID,
		})
	}
	return sentRanges, ah.bytesInFlight, ah.cong.GetCongestionWindow(), *ah.rtt, ah.loss
}

func (ah *SentPacketHandler) CanSend() bool {
	ah.m.Lock()
	defer ah.m.Unlock()
	return ah.cong.CanSend(ah.bytesInFlight)
}

func (ah *SentPacketHandler) LossDetectionTimeout() time.Time {
	return ah.multiModalTimer
}

func (ah *SentPacketHandler) PacingTimeout() time.Time {
	return ah.cong.PacingTimer()
}

func (ah *SentPacketHandler) addBytesInFlight(size int) {
	prev := ah.bytesInFlight
	ah.bytesInFlight += size
	ah.auditBytesInFlight("Added bytes in flight", prev)
}

func (ah *SentPacketHandler) removeBytesInFlight(size int) {
	prev := ah.bytesInFlight
	ah.bytesInFlight -= size
	ah.auditBytesInFlight("Removed bytes in flight", prev)
}

// auditBytesInFlight re-derives bytesInFlight from sentRanges and complains if
// it disagrees with the counter the two callers maintain incrementally.
//
// It is gated on Debug being enabled, and that gate is load-bearing rather
// than tidiness. The audit allocates a slice the length of sentRanges and
// walks every in-flight packet — once per packet SENT, so O(n^2) across a
// window. That was invisible while the window was two packets wide. Once the
// UDP receive buffer stopped overflowing and the window reached 2.8 MB,
// sentRanges held ~2000 entries and this showed up on the server's CPU
// profile as 16% mallocgc plus 10% GC: fixing one bottleneck had fed the next
// one, because the cost scales with exactly the thing that got better.
//
// The counter itself is unchanged and still exact; only its verification is
// now something you opt into.
func (ah *SentPacketHandler) auditBytesInFlight(msg string, prev int) {
	if !ah.logger.Enabled(context.Background(), slog.LevelDebug) {
		return
	}
	sentRanges := make([]int, len(ah.sentRanges))
	sum := 0
	for i := range ah.sentRanges {
		if ah.sentRanges[i].CongestionExempt {
			continue // never entered bytesInFlight, so it is not part of the sum
		}
		sentRanges[i] = int(ah.sentRanges[i].PacketSize)
		sum += int(ah.sentRanges[i].PacketSize)
	}
	if sum != ah.bytesInFlight {
		ah.logger.Error("Inconsistent bytes in flight", "expected", ah.bytesInFlight, "actual", sum)
	}
	ah.logger.Debug(msg, "prev_bytes_in_flight", prev, "bytes_in_flight", ah.bytesInFlight, "ranges", sentRanges)
}

func (ah *SentPacketHandler) OnSent(s *SentPacket) error {
	ah.m.Lock()
	defer ah.m.Unlock()
	ah.sentRanges = append(ah.sentRanges, s)
	// Two independent conditions, not an if/else: congestion participation and
	// probe bookkeeping used to be the same branch, which is what made a
	// congestion-exempt non-probe impossible to express.
	if !s.CongestionExempt {
		ah.addBytesInFlight(s.PacketSize)
		ah.cong.RecordSend(s.PacketSize, s.SentTime)
	} else {
		ah.exemptOutstanding++
	}
	if s.IsMTUProbe {
		ah.mtuProbesOutstanding++
	}
	ah.largestSent = max(ah.largestSent, s.PacketNumber)
	ah.setLossDetectionTimer(s.SentTime)
	return nil
}

func (ah *SentPacketHandler) detectAck(rcvTime time.Time, ranges []Range) ([]*SentPacket, error) {
	var ackedPackets []*SentPacket
	var ackedPN []objproto.PacketNumber
	// Both the kept-set and the acked-packet-number list used to be built as
	// fresh slices on every ACK, growing back to the length of what was in
	// flight -- the same shape as the onACK filter and the cc75e99 audit, and
	// like them it only bites once the congestion window is open. The kept
	// set is now filtered in place, and ackedPN is only assembled when
	// something will actually read it.
	debug := ah.logger.Enabled(context.Background(), slog.LevelDebug)
	origLen := len(ah.sentRanges)
	newRemainPackets := ah.sentRanges[:0]
	for _, p := range ah.sentRanges {
		acked := false
		for i := range ranges {
			rg := ranges[i]
			if p.PacketNumber < rg.Begin {
				// not acked
				break
			}
			if p.PacketNumber >= rg.End {
				// check next range
				continue
			}
			// acked
			ackedPackets = append(ackedPackets, p)
			if debug {
				ackedPN = append(ackedPN, p.PacketNumber)
			}
			acked = true
			break
		}
		if !acked {
			newRemainPackets = append(newRemainPackets, p)
		}
	}
	if debug {
		ah.logger.Debug("Processing ACK", "acked_packets", ackedPN)
	}
	if len(newRemainPackets)+len(ackedPackets) != origLen {
		return nil, errors.New("BUG: inconsistent ack detection")
	}
	for i := len(newRemainPackets); i < origLen; i++ {
		ah.sentRanges[i] = nil
	}
	ah.sentRanges = newRemainPackets
	sentSize := 0
	exemptSize := 0

	for _, p := range ackedPackets {
		sentSize += p.PacketSize
		if p.OnACK != nil {
			p.OnACK(rcvTime)
		}
		if p.CongestionExempt {
			exemptSize += p.PacketSize
			ah.exemptOutstanding--
		}
		if p.IsMTUProbe {
			ah.mtuProbesOutstanding--
		}
	}
	if len(ackedPackets) > 0 {
		ah.removeBytesInFlight(sentSize - exemptSize)
		ah.cong.RecordACK(sentSize, rcvTime)
	}
	return ackedPackets, nil
}

const timeThreshold = 9.0 / 8

func (ah *SentPacketHandler) detectLost(now time.Time) {
	ah.lossTime = time.Time{} // reset
	maxRTT := float64(max(ah.rtt.LatestRTT, ah.rtt.SRTT))
	lossDelay := time.Duration(timeThreshold * maxRTT)

	// Minimum time of granularity before packets are deemed lost.
	lossDelay = max(lossDelay, 1*time.Millisecond)

	// Packets sent before this time are deemed lost.
	lostSendTime := now.Add(-lossDelay)

	somePacketLost := false

	// Filtered in place, like detectAck above. OnLost only pushes to the
	// stream's own retransmit queue and trigger, so it cannot touch this
	// slice while the loop is walking it.
	origLen := len(ah.sentRanges)
	remainRanges := ah.sentRanges[:0]
	lostSize := 0
	lostCount := 0
	exemptSize := 0
	congestionLostSize := 0
	congestionLostCount := 0
	evidenceCount := 0
	for _, p := range ah.sentRanges {
		if p.PacketNumber > ah.largestAcked {
			remainRanges = append(remainRanges, p)
			continue
		}

		var packetLost bool
		if !p.SentTime.After(lostSendTime) { // currently, only time threshold
			packetLost = true
		} else if ah.lossTime.IsZero() {
			// Note: This conditional is only entered once per call
			lossTime := p.SentTime.Add(lossDelay)
			ah.logger.Debug("Set loss timer", "from_now", lossTime.Sub(now))
			ah.lossTime = lossTime
		}
		if packetLost {
			somePacketLost = true
			lostSize += p.PacketSize
			if p.OnLost != nil {
				p.OnLost(now) // maybe queueing
			}
			lostCount++
			if p.CongestionExempt {
				exemptSize += p.PacketSize
				ah.exemptOutstanding--
			} else {
				congestionLostSize += p.PacketSize
				congestionLostCount++
			}
			if p.IsMTUProbe {
				ah.mtuProbesOutstanding--
			}
			// MTU probes are expected to be lost — that is how the probe
			// reports a too-large MTU — so they are not evidence about the
			// path and do not belong in the spurious count either. A
			// datagram's loss IS evidence about the path, which is why this
			// is now its own property rather than "not a probe".
			if p.PathEvidence {
				evidenceCount++
				ah.rememberDeclaredLost(p.PacketNumber)
			}
		} else {
			remainRanges = append(remainRanges, p)
		}
	}
	if len(remainRanges)+lostCount != origLen {
		ah.logger.Error("BUG: inconsistent loss detection", "expected", origLen, "actual", len(remainRanges)+lostCount)
	}
	for i := len(remainRanges); i < origLen; i++ {
		ah.sentRanges[i] = nil
	}
	ah.sentRanges = remainRanges
	if somePacketLost {
		ah.removeBytesInFlight(lostSize - exemptSize)
		// Two gates, not one. Evidence and congestion response used to be
		// decided together by "was it a probe"; an uncontrolled datagram's loss
		// belongs in the first and must stay out of the second.
		ah.loss.Packets += evidenceCount
		if congestionLostCount > 0 {
			ah.loss.Events++
			ah.cong.RecordLoss(congestionLostSize, now)
		}
	}
}

func (ah *SentPacketHandler) setLossDetectionTimer(now time.Time) {
	defer ah.trigger.Notify()
	if !ah.lossTime.IsZero() {
		ah.logger.Debug("Set loss timer", "from_now", ah.lossTime.Sub(now))
		ah.multiModalTimer = ah.lossTime
		return
	}
	// An outstanding congestion-exempt packet sets the timer even though it
	// contributes no bytesInFlight: this timer is the only thing that will ever
	// declare it lost. MTU probes were the first such packet and are still
	// counted separately, because their retirement is the tracker's business;
	// exemptOutstanding covers every other one. Stopping the timer here while
	// either is outstanding leaves the run loop parked with no deadline, and
	// that packet's OnLost never fires.
	if ah.bytesInFlight == 0 && ah.mtuProbesOutstanding == 0 && ah.exemptOutstanding == 0 {
		ah.logger.Debug("No packets in flight, disable loss timer")
		ah.multiModalTimer = time.Time{}
		return
	}
	pto := ah.rtt.PTO(ah.ptoCount)
	ah.logger.Debug("Set PTO timer", "from_now", pto)
	ah.multiModalTimer = now.Add(pto)
}

// declareLostUnretransmittable retires every outstanding packet that nothing
// will re-send, and reports how many it retired.
//
// For such a packet the timer expiry IS its loss declaration, which is not how
// a retransmittable packet is treated: for those, PTO only prompts a
// retransmission and leaves the packet in sentRanges, still eligible to be
// acked later. That treatment is unavailable here for two reasons, and both
// were first written down about MTU probes. Nothing retransmits it -- MTUTracker
// issues the next probe itself, at a size of its own choosing, and a datagram
// is simply gone -- and leaving it in sentRanges would let the following expiry
// report the SAME packet as a second loss. For a probe that meant three
// expiries could collapse the tracker's upper bound below the true path MTU
// without a single extra packet having been put on the path; for a datagram it
// would mean one drop counted many times.
//
// What differs between the two is congestion, and the fields say which is
// which. A probe was never in bytesInFlight and reports a size limit rather
// than anything about capacity, so no bytes leave and no response is taken. A
// CONGESTION-CONTROLLED datagram was in flight and its loss is a real signal,
// so it pays both.
func (ah *SentPacketHandler) declareLostUnretransmittable(now time.Time) int {
	// Deliberately not guarded by a counter the way this was when it only
	// handled probes: three classes now qualify, a guard would need to track
	// all of them in step, and this runs once per PTO expiry over a slice
	// bounded by the congestion window.
	origLen := len(ah.sentRanges)
	remain := ah.sentRanges[:0]
	lost := 0
	congestionLostSize := 0
	congestionLostCount := 0
	evidenceCount := 0
	for _, p := range ah.sentRanges {
		if p.Retransmittable {
			remain = append(remain, p)
			continue
		}
		lost++
		if p.OnLost != nil {
			p.OnLost(now)
		}
		if p.CongestionExempt {
			ah.exemptOutstanding--
		} else {
			congestionLostSize += p.PacketSize
			congestionLostCount++
		}
		if p.IsMTUProbe {
			ah.mtuProbesOutstanding--
		}
		if p.PathEvidence {
			evidenceCount++
			ah.rememberDeclaredLost(p.PacketNumber)
		}
	}
	for i := len(remain); i < origLen; i++ {
		ah.sentRanges[i] = nil
	}
	ah.sentRanges = remain
	if congestionLostCount > 0 {
		ah.removeBytesInFlight(congestionLostSize)
		ah.loss.Events++
		ah.cong.RecordLoss(congestionLostSize, now)
	}
	ah.loss.Packets += evidenceCount
	if lost > 0 {
		ah.logger.Debug("unretransmittable packets expired", "count", lost)
	}
	return lost
}

func (ah *SentPacketHandler) OnTimeout(now time.Time) (bool, error) {
	ah.m.Lock()
	defer ah.m.Unlock()
	defer ah.setLossDetectionTimer(now)
	if !ah.lossTime.IsZero() {
		ah.logger.Debug("Loss timer fired")
		// Early retransmit or time loss detection
		ah.detectLost(now)
		return false, nil
	}

	// PTO
	// When all outstanding are acknowledged, the alarm is canceled in setLossDetectionTimer.
	// However, there's no way to reset the timer in the connection.
	// When OnLossDetectionTimeout is called, we therefore need to make sure that there are
	// actually packets outstanding.
	// An outstanding unretransmittable packet expires HERE, and nowhere else,
	// whenever it sits above largestAcked: the ACK-driven detectLost pass skips
	// everything above that line, and a packet sent with no data behind it is
	// always above it. An MTU probe is the original case; a datagram that
	// nothing acknowledged is the same shape.
	lostUnretransmittable := ah.declareLostUnretransmittable(now)

	// Read AFTER the call above: retiring a congestion-controlled datagram
	// removes its bytes, so this can legitimately become zero here.
	if ah.bytesInFlight == 0 {
		if lostUnretransmittable > 0 {
			// Those packets were the only things outstanding, so retiring them
			// above was the whole point of this expiry. ptoCount is deliberately
			// not advanced: nothing here can be retransmitted, so inflating the
			// PTO backoff would only slow the next real retransmission.
			return false, nil
		}
		return false, errors.New("BUG: no packets in flight")
	}

	if len(ah.sentRanges) == 0 {
		return false, nil
	}
	ah.ptoCount++
	ah.logger.Debug("PTO fired, try retransmission")
	// declareLostUnretransmittable has already retired everything that cannot
	// be re-sent, so this finds a retransmittable packet on the first entry.
	// The filter is kept because the guarantee lives in another function.
	for _, p := range ah.sentRanges {
		if p.Retransmittable {
			p.OnLost(now) // trigger retransmission
			return true, nil
		}
	}
	return false, nil
}

// ReceiveACK folds one ACK in. ackDelay is what the peer reported holding the
// largest acked packet for; it reaches UpdateRTT and nothing else, because it
// describes the MEASUREMENT and not the path.
func (ah *SentPacketHandler) ReceiveACK(rcvTime time.Time, r []Range, ackDelay time.Duration) error {
	ah.m.Lock()
	defer ah.m.Unlock()
	largest := r[len(r)-1].End - 1
	if largest > ah.largestSent {
		return fmt.Errorf("received invalid ACK: largest acked %d > largest sent %d", largest, ah.largestSent)
	}
	// Before detectAck: a packet already declared lost is no longer in
	// sentRanges, so detectAck cannot see the ACK that vindicates it. This is
	// the only place that evidence exists.
	ah.noteSpuriousLoss(r)
	ackedPackets, err := ah.detectAck(rcvTime, r)
	if err != nil {
		return err
	}
	if len(ackedPackets) == 0 {
		return nil
	}
	// largest acked packet time update
	if lastPacket := ackedPackets[len(ackedPackets)-1]; lastPacket.PacketNumber == objproto.PacketNumber(largest) {
		ah.rtt.UpdateRTT(ah.logger, rcvTime.Sub(lastPacket.SentTime), ackDelay, rcvTime)
	}
	ah.largestAcked = max(ah.largestAcked, largest)
	ah.detectLost(rcvTime)
	ah.ptoCount = 0
	ah.setLossDetectionTimer(rcvTime)
	return nil
}

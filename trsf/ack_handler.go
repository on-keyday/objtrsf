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
	Packets  int // non-probe packets declared lost
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

type SentPacket struct {
	OnACK        func(now time.Time)
	OnLost       func(now time.Time)
	PacketSize   int
	StreamID     StreamID
	PacketNumber objproto.PacketNumber
	SentTime     time.Time
	IsMTUProbe   bool
	Kind         wire.ApplicationPayloadKind
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
			SentTime:   p.SentTime,
			PacketSize: p.PacketSize,
			IsMTUProbe: p.IsMTUProbe,
			Kind:       p.Kind,
			StreamID:   p.StreamID,
		})
	}
	rtt := *ah.rtt
	if rtt.NoACKReceived() {
		// MinRTT starts at the maximum duration and is only ever lowered, so
		// before the first ACK it is a sentinel rather than a measurement.
		// Normalised HERE, where the sentinel is known, so no reader has to
		// recognise 2562047h47m16s as "not measured yet".
		rtt.MinRTT = 0
	}
	return sentRanges, ah.bytesInFlight, ah.cong.GetCongestionWindow(), rtt, ah.loss
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
		if ah.sentRanges[i].IsMTUProbe {
			continue // ignore MTU probes
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
	if !s.IsMTUProbe {
		ah.addBytesInFlight(s.PacketSize)
		ah.cong.RecordSend(s.PacketSize, s.SentTime)
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
	probeSize := 0

	for _, p := range ackedPackets {
		sentSize += p.PacketSize
		if p.OnACK != nil {
			p.OnACK(rcvTime)
		}
		if p.IsMTUProbe {
			probeSize += p.PacketSize
		}
	}
	if len(ackedPackets) > 0 {
		ah.removeBytesInFlight(sentSize - probeSize)
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
	mtuProbe := 0
	probeSize := 0
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
			if p.IsMTUProbe {
				mtuProbe++
				probeSize += p.PacketSize
			} else {
				// MTU probes are expected to be lost — that is how the probe
				// reports a too-large MTU — so they are not evidence about the
				// path and do not belong in the spurious count either.
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
		ah.removeBytesInFlight(lostSize - probeSize)
		ah.loss.Packets += lostCount - mtuProbe
		if lostCount > mtuProbe { // ignore congestion for MTU probes
			ah.loss.Events++
			ah.cong.RecordLoss(lostSize-probeSize, now)
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
	if ah.bytesInFlight == 0 {
		ah.logger.Debug("No packets in flight, disable loss timer")
		ah.multiModalTimer = time.Time{}
		return
	}
	pto := ah.rtt.PTO(ah.ptoCount)
	ah.logger.Debug("Set PTO timer", "from_now", pto)
	ah.multiModalTimer = now.Add(pto)
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
	if ah.bytesInFlight == 0 {
		return false, errors.New("BUG: no packets in flight")
	}

	if len(ah.sentRanges) == 0 {
		return false, nil
	}
	ah.ptoCount++
	ah.logger.Debug("PTO fired, try retransmission")
	// first, try non MTU probe packet
	for _, p := range ah.sentRanges {
		if !p.IsMTUProbe {
			p.OnLost(now) // trigger retransmission
			return true, nil
		}
	}
	// all are MTU probes, retransmit the first one
	ah.sentRanges[0].OnLost(now) // retransmit the first packet
	return true, nil
}

func (ah *SentPacketHandler) ReceiveACK(rcvTime time.Time, r []Range) error {
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
		ah.rtt.UpdateRTT(ah.logger, rcvTime.Sub(lastPacket.SentTime), rcvTime)
	}
	ah.largestAcked = max(ah.largestAcked, largest)
	ah.detectLost(rcvTime)
	ah.ptoCount = 0
	ah.setLossDetectionTimer(rcvTime)
	return nil
}

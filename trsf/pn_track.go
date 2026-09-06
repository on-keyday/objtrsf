package trsf

import (
	"sync"
	"time"
)

type Range struct {
	Begin uint64
	End   uint64
}
type PacketNumTracker struct {
	m             sync.Mutex
	unackedRanges []Range
	// largestAt is when the highest packet number still waiting to be
	// acknowledged was processed, and largestPN which one that is. The pair
	// exists so the ACK can report how long this receiver held it — RFC 9002's
	// ack_delay, which the peer subtracts from its RTT sample.
	//
	// It is a LOWER BOUND on the true delay: the clock starts when the run loop
	// takes the packet off the receive queue, so any dwell before that is not
	// counted. Understating it is the safe direction — the peer's guard only
	// subtracts a delay that leaves the sample above its own min_rtt.
	largestPN   uint64
	largestAt   time.Time
	haveLargest bool
	recvAdded   chan struct{}
}

func NewPacketNumTracker() *PacketNumTracker {
	return &PacketNumTracker{
		unackedRanges: make([]Range, 0),
		recvAdded:     make(chan struct{}, 1),
	}
}

func (rc *PacketNumTracker) NotifyReceive() <-chan struct{} {
	return rc.recvAdded
}

func (rc *PacketNumTracker) notify() {
	select {
	case rc.recvAdded <- struct{}{}:
	default:
	}
}

func (rc *PacketNumTracker) InsertUnacked(seqNum uint64) {
	rc.m.Lock()
	defer rc.m.Unlock()
	if !rc.haveLargest || seqNum > rc.largestPN {
		rc.largestPN, rc.largestAt, rc.haveLargest = seqNum, time.Now(), true
	}
	if len(rc.unackedRanges) == 0 {
		rc.unackedRanges = append(rc.unackedRanges, Range{Begin: seqNum, End: seqNum + 1})
		rc.notify()
		return
	}
	for i, r := range rc.unackedRanges {
		if seqNum >= r.Begin && seqNum < r.End {
			// already present
			return
		}
		if seqNum == r.End {
			// extend range
			rc.unackedRanges[i].End++
			// check if can merge with next range
			if i+1 < len(rc.unackedRanges) && rc.unackedRanges[i].End == rc.unackedRanges[i+1].Begin {
				rc.unackedRanges[i].End = rc.unackedRanges[i+1].End
				rc.unackedRanges = append(rc.unackedRanges[:i+1], rc.unackedRanges[i+2:]...)
			}
			rc.notify()
			return
		}
		if seqNum+1 == r.Begin {
			// extend range
			rc.unackedRanges[i].Begin--
			// check if can merge with previous range
			if i >= 1 && rc.unackedRanges[i-1].End == rc.unackedRanges[i].Begin {
				rc.unackedRanges[i-1].End = rc.unackedRanges[i].End
				rc.unackedRanges = append(rc.unackedRanges[:i], rc.unackedRanges[i+1:]...)
			}
			rc.notify()
			return
		}
		if seqNum < r.Begin {
			// insert new range
			newRange := Range{Begin: seqNum, End: seqNum + 1}
			rc.unackedRanges = append(rc.unackedRanges[:i], append([]Range{newRange}, rc.unackedRanges[i:]...)...)
			rc.notify()
			return
		}
	}
}

// GenerateACK takes the pending ranges and, with them, how long the largest of
// them has been held. The caller turns that into the wire's ack_delay.
//
// Both are cleared: the next ACK covers packets that arrive after this one, and
// a stale largestAt would report a delay for a packet already acknowledged.
func (rc *PacketNumTracker) GenerateACK() ([]Range, time.Duration) {
	rc.m.Lock()
	defer rc.m.Unlock()
	ranges := rc.unackedRanges
	rc.unackedRanges = nil
	var held time.Duration
	if rc.haveLargest {
		held = time.Since(rc.largestAt)
		rc.haveLargest = false
	}
	return ranges, held
}

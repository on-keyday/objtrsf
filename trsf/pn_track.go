package trsf

import (
	"sync"
)

type Range struct {
	Begin uint64
	End   uint64
}
type PacketNumTracker struct {
	m             sync.Mutex
	unackedRanges []Range
	recvAdded     chan struct{}
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

func (rc *PacketNumTracker) GenerateACK() []Range {
	rc.m.Lock()
	defer rc.m.Unlock()
	ranges := rc.unackedRanges
	rc.unackedRanges = nil
	return ranges
}

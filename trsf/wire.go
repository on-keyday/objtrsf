package trsf

import (
	"errors"
	"slices"
	"time"

	"github.com/on-keyday/objtrsf/trsf/wire"
)

func decodeRanges(end uint64, firstDelta uint64, ranges []wire.ACKRange) []Range {
	begin := end - firstDelta - 1
	result := []Range{
		{Begin: begin, End: end},
	}
	currentBegin := begin
	for _, r := range ranges {
		end := currentBegin - r.Offset.Value() - 2
		begin := end - r.Delta.Value() - 1
		result = append(result, Range{
			Begin: begin,
			End:   end,
		})
		currentBegin = begin
	}
	slices.Reverse(result)
	return result
}

// ParseTransferACK also returns the delay the peer reported holding the
// largest acked packet for. Microseconds on the wire; a duration here.
func ParseTransferACK(pkt *wire.StreamACKPacket) (ranges []Range, ackDelay time.Duration, err error) {
	/*
		rawMin, ok := obj[MaxAckedKey]
		if !ok {
			return 0, nil, errors.New("missing min acked")
		}
		maxAcked, ok := objproto.ParseUint(rawMin)
		if !ok {
			return 0, nil, errors.New("invalid min acked")
		}
		rawFirstDelta, ok := obj[FirstDeltaKey]
		if !ok {
			return 0, nil, errors.New("missing first delta")
		}
		firstDelta, ok := objproto.ParseUint(rawFirstDelta)
		if !ok {
			return 0, nil, errors.New("invalid first delta")
		}
		var offsets []uint64
		var deltas []uint64
		rawOffsets, hasOffsets := obj[OffsetsKey]
		if hasOffsets {
			offsets, ok = objproto.ParseUintArray(rawOffsets)
			if !ok {
				return 0, nil, errors.New("invalid offsets")
			}
		}
		rawDeltas, hasDeltas := obj[DeltasKey]
		if hasDeltas {
			deltas, ok = objproto.ParseUintArray(rawDeltas)
			if !ok {
				return 0, nil, errors.New("invalid deltas")
			}
		}
		if len(offsets) != len(deltas) {
			return 0, nil, errors.New("mismatched offsets and deltas")
		}
		id, _ = objproto.GetUint(obj, IDKey)
	*/
	return decodeRanges(pkt.LargestAck.Value(), pkt.FirstDelta.Value(), pkt.Ranges),
		time.Duration(pkt.AckDelay.Value()) * time.Microsecond, nil
}

// TransferACK builds the ACK. ackDelay is how long this receiver has held the
// largest packet it is acking; it goes on the wire in microseconds so the peer
// can take its own scheduling out of the RTT sample (RFC 9002 5.3).
func TransferACK(ranges []Range, ackDelay time.Duration) (*wire.StreamACKPacket, error) {
	if len(ranges) == 0 {
		return nil, errors.New("empty delta")
	}
	var begin uint64 = ranges[len(ranges)-1].Begin
	var end uint64 = ranges[len(ranges)-1].End
	if end <= begin {
		return nil, errors.New("invalid range in delta")
	}
	firstDelta := end - begin - 1
	var maxDelta uint64 = 0
	var maxOffset uint64 = 0
	var wire_ranges []wire.ACKRange
	for i := 1; i < len(ranges); i++ {
		r := ranges[len(ranges)-1-i]
		if begin <= r.End {
			return nil, errors.New("invalid range in delta")
		}
		if r.End <= r.Begin {
			return nil, errors.New("invalid range in delta")
		}
		o := begin - r.End - 2
		d := r.End - r.Begin - 1
		begin = r.Begin
		offset, ok := wire.EncodeVarint(o)
		if !ok {
			return nil, errors.New("invalid offset")
		}
		delta, ok := wire.EncodeVarint(d)
		if !ok {
			return nil, errors.New("invalid delta")
		}
		wire_ranges = append(wire_ranges, wire.ACKRange{
			Offset: offset,
			Delta:  delta,
		})
		maxDelta = max(maxDelta, d)
		maxOffset = max(maxOffset, o)
	}
	endWire, ok := wire.EncodeVarint(end)
	if !ok {
		return nil, errors.New("invalid end")
	}
	firstDeltaWire, ok := wire.EncodeVarint(firstDelta)
	if !ok {
		return nil, errors.New("invalid first delta")
	}
	if ackDelay < 0 {
		ackDelay = 0
	}
	delayWire, ok := wire.EncodeVarint(uint64(ackDelay.Microseconds()))
	if !ok {
		return nil, errors.New("invalid ack delay")
	}
	return &wire.StreamACKPacket{
		LargestAck: endWire,
		AckDelay:   delayWire,
		FirstDelta: firstDeltaWire,
		Ranges:     wire_ranges,
	}, nil
}

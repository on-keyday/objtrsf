package trsf

import (
	"fmt"
	"io"
)

type RecvData struct {
	Offset uint64
	Data   []byte
	Eof    bool
}

type OrderingQueue struct {
	queue      []*RecvData
	nextOffset uint64
	eofRecved  bool
	eofReached bool
	*trigger
}

func NewOrderingQueue() *OrderingQueue {
	return &OrderingQueue{
		trigger: newTrigger(),
	}
}

func (oq *OrderingQueue) maybeNotify() {
	oq.trigger.Notify()
}

func (oq *OrderingQueue) Push(data *RecvData) error {
	// A zero-length non-EOF frame carries no payload byte range
	// (covers [Offset, Offset)) and no terminator. Its only purpose on
	// the wire is to advertise stream creation — by the time we reach
	// here, Streams.handlePacket has already materialised the recv
	// stream entry via getRecvStream, so the OrderingQueue has nothing
	// to track. Dropping these here also prevents a subsequent real
	// payload at the same offset from being mis-detected as a duplicate
	// by the data.Offset == d.Offset branch below (which assumes
	// "same offset ⇒ same byte range", an assumption that 0-length
	// markers violate).
	if len(data.Data) == 0 && !data.Eof {
		return nil
	}
	if oq.nextOffset > data.Offset {
		return nil // duplicate or old data, ignore
	}
	for i, d := range oq.queue {
		if data.Offset == d.Offset {
			return nil // duplicate data, ignore
		}
		if data.Offset < d.Offset {
			// check overlap with next
			if data.Offset+uint64(len(data.Data)) > d.Offset {
				return fmt.Errorf("overlapping data at offset %d", data.Offset)
			}
			// check overlap with previous
			if i > 0 {
				prev := oq.queue[i-1]
				if prev.Offset+uint64(len(prev.Data)) > data.Offset {
					return fmt.Errorf("overlapping data at offset %d", data.Offset)
				}
			}
			oq.queue = append(oq.queue[:i], append([]*RecvData{data}, oq.queue[i:]...)...)
			oq.maybeNotify()
			return nil
		}
	}
	if len(oq.queue) > 0 {
		last := oq.queue[len(oq.queue)-1]
		if last.Offset+uint64(len(last.Data)) > data.Offset {
			return fmt.Errorf("overlapping data at offset %d", data.Offset)
		}
		if last.Eof {
			return fmt.Errorf("cannot add data after EOF at offset %d", data.Offset)
		}
	}
	if oq.eofReached {
		return fmt.Errorf("cannot add data after EOF at offset %d", data.Offset)
	}
	oq.queue = append(oq.queue, data)
	if data.Eof {
		oq.eofRecved = true
	}
	oq.maybeNotify()
	return nil
}

func (oq *OrderingQueue) HasData() bool {
	return len(oq.queue) > 0 && oq.queue[0].Offset == oq.nextOffset
}

func (oq *OrderingQueue) EOF() bool {
	return oq.eofReached
}

func (oq *OrderingQueue) ReadDirect(n uint64) ([]byte, bool) {
	if oq.eofReached {
		return nil, true
	}
	if len(oq.queue) == 0 {
		return nil, false
	}
	if oq.queue[0].Offset != oq.nextOffset {
		return nil, false
	}
	var result []byte
	eof := false
	for len(oq.queue) > 0 && oq.queue[0].Offset == oq.nextOffset && n > 0 {
		data := oq.queue[0]
		toRead := uint64(len(data.Data))
		if toRead > n {
			toRead = n
		}
		result = append(result, data.Data[:toRead]...)
		oq.nextOffset += toRead
		n -= toRead
		if toRead < uint64(len(data.Data)) {
			// partial read
			data.Data = data.Data[toRead:]
			data.Offset += toRead
			break
		} else {
			// full read
			eof = data.Eof
			oq.queue = oq.queue[1:]
		}
	}
	oq.eofReached = eof
	return result, eof
}

func (oq *OrderingQueue) Read(p []byte) (n int, err error) {
	data, eof := oq.ReadDirect(uint64(len(p)))
	if len(data) == 0 && eof {
		return 0, io.EOF
	}
	copy(p, data)
	return len(data), nil
}

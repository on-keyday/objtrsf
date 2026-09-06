package trsf

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/on-keyday/objtrsf/trsf/mtu"
	"github.com/on-keyday/objtrsf/trsf/wire"
)

type SentRange struct {
	ID       StreamID
	Offset   uint64
	Data     []byte
	SentSize int
	Eof      bool
	OnACK    func(now time.Time)
	OnLost   func(now time.Time)
}

type FlowController interface {
	SendableSize() int
	RecordSend(size int)
	UpdateWindow(size int) bool
}

type sendStream struct {
	m            sync.Mutex
	inputBuffer  [][]byte
	dataInBuffer uint64
	bufferLimit  uint64
	writeFence   chan struct{}
	ctx          context.Context

	offset          uint64
	signalEOF       atomic.Bool
	cancelRequested atomic.Bool
	eofSent         bool

	// pendingOpen is set on streams created locally (CreateBidirectionalStream
	// / CreateSendStream) so that triggerPacket emits a 0-byte STREAM frame
	// once even if the user never calls AppendData. Without this, the peer
	// only learns about the stream when the first non-empty STREAM frame
	// arrives — a freshly created stream that nobody writes to is invisible,
	// and GetBidirectionalStream(id) on the peer returns nil. The marker
	// frame goes through the normal retransmit path, so a lost open is
	// resent like any other frame.
	pendingOpen atomic.Bool

	sentRanges []*SentRange
	flow       FlowController
	id         StreamID

	logger          *slog.Logger
	retransmitQueue *withTriggerQueue[SentRange]

	sendTrigger  *withTriggerQueue[sendStream] // shared with conn
	writerSignal chan struct{}

	mtu *mtu.MTUTracker
}

var _ SendStream = (*sendStream)(nil)

func newSendStream(ctx context.Context, mtu *mtu.MTUTracker, id StreamID, flow FlowController, logger *slog.Logger, sendTrigger *withTriggerQueue[sendStream]) *sendStream {
	return &sendStream{
		writeFence:      make(chan struct{}, 1),
		id:              id,
		mtu:             mtu,
		ctx:             ctx,
		flow:            flow,
		logger:          logger,
		retransmitQueue: newWithTriggerQueue[SentRange](),
		sendTrigger:     sendTrigger,
		writerSignal:    make(chan struct{}, 1),
		bufferLimit:     1024 * 1024, // 1MB
	}
}

func (r *sendStream) updateFlowWindow(size int) {
	r.m.Lock()
	defer r.m.Unlock()
	if r.flow.UpdateWindow(size) {
		r.logger.Debug("updated send stream window", "stream_id", r.id, "new_window", size)
		r.sendTrigger.PushBecause(r, pushOther)
	}
}

func (r *sendStream) onCancel() {
	r.cancelRequested.Store(true)
	r.sendTrigger.PushBecause(r, pushOther)
}

func (r *sendStream) signalWriter() {
	select {
	case r.writerSignal <- struct{}{}:
	default:
	}
}

func (r *sendStream) ID() StreamID {
	return r.id
}

func (r *sendStream) Write(p []byte) (n int, err error) {
	return r.WriteContext(context.Background(), p)
}

func (r *sendStream) WriteContext(ctx context.Context, data []byte) (n int, err error) {
	select {
	case <-r.ctx.Done():
		return 0, r.ctx.Err()
	default:
	}
	if r.signalEOF.Load() || r.cancelRequested.Load() {
		return 0, io.EOF
	}
	copyBuf := make([]byte, len(data))
	copy(copyBuf, data)
	err = r.AppendData(false, copyBuf)
	if err != nil {
		return 0, err
	}
	return len(data), nil
}

func (r *sendStream) Close() error {
	// instead of Calling AppendData(true), just set EOF flag directly
	// if concurrent Write is happening,
	// if happly, Write will written data and eof is flagged
	// if Write is happending, triggerPacket is blocked on lock so inconsistent state won't happen
	// otherwise, even if triggerPacket cannot get signalEOF at this moment,
	// r.sendTrigger.Push(r) will wake up triggerPacket later
	// if not, Write will return EOF error
	r.signalEOF.Store(true)               // flag EOF
	r.sendTrigger.PushBecause(r, pushApp) // force trigger to send EOF
	r.signalWriter()                      // wake up writer if waiting
	return nil
}

func (r *sendStream) HasSendData() bool {
	r.m.Lock()
	defer r.m.Unlock()
	return len(r.inputBuffer) > 0
}

func (r *sendStream) Completed() bool {
	r.m.Lock()
	defer r.m.Unlock()
	return r.eofSent && len(r.sentRanges) == 0
}

func (r *sendStream) AppendData(eof bool, data ...[]byte) error {
	return r.AppendDataContext(context.Background(), eof, data...)
}

// data must be copied before calling AppendData
// AppendData appends data to the send stream buffer and holds until there is space in the buffer
func (r *sendStream) AppendDataContext(ctx context.Context, eof bool, data ...[]byte) error {
	r.writeFence <- struct{}{}
	defer func() { <-r.writeFence }()
	r.m.Lock()
	defer r.m.Unlock()
	for {
		select {
		case <-r.ctx.Done():
			return r.ctx.Err()
		default:
		}
		if r.signalEOF.Load() || r.cancelRequested.Load() {
			r.logger.Debug("attempted to write to closed or canceled stream", "stream_id", r.id, "len", len(data), "eof", eof)
			return io.EOF
		}
		if r.dataInBuffer-r.offset >= r.bufferLimit { // wait for buffer to drain
			buffered := r.dataInBuffer - r.offset // capture under lock before releasing
			r.m.Unlock()
			r.logger.Debug("send stream buffer full, waiting for drain", "stream_id", r.id, "buffered", buffered, "buffer_limit", r.bufferLimit)
			select {
			case <-ctx.Done():
				// Re-acquire before returning so the top-level `defer
				// r.m.Unlock()` is balanced — without this the deferred unlock
				// hits an already-unlocked mutex (fatal "unlock of unlocked
				// mutex").
				r.m.Lock()
				return ctx.Err()
			case <-r.writerSignal:
			case <-r.ctx.Done():
				r.m.Lock()
				return r.ctx.Err()
			}
			r.m.Lock()
			continue
		}
		changed := false
		if len(data) > 0 {
			r.inputBuffer = append(r.inputBuffer, data...)
			for _, d := range data {
				r.dataInBuffer += uint64(len(d))
			}
			r.logger.Debug("appended data to send stream buffer", "stream_id", r.id, "len", len(data), "total_buffered", r.dataInBuffer)
			changed = true
		}
		if eof {
			r.logger.Debug("stream EOF reached", "stream_id", r.id)
			r.signalEOF.Store(true)
			changed = true
		}
		if changed {
			r.sendTrigger.PushBecause(r, pushApp)
		}
		break
	}
	return nil
}

func (r *sendStream) readChunk(maxSize int) (_ []byte, isEOF bool) {
	// if cancel is requested and eof is not yet sent, return eof immediately
	if r.cancelRequested.Load() && !r.eofSent {
		r.logger.Debug("stream canceled, sending EOF immediately", "stream_id", r.id)
		return nil, true
	}
	var buffer []byte
	for len(r.inputBuffer) > 0 {
		chunk := r.inputBuffer[0]
		if len(buffer)+len(chunk) > maxSize {
			chunkToSend := chunk[:maxSize-len(buffer)]
			r.inputBuffer[0] = chunk[maxSize-len(buffer):]
			if buffer == nil {
				return chunkToSend, false
			}
			return append(buffer, chunkToSend...), false
		} else {
			if buffer == nil {
				buffer = chunk
			} else {
				buffer = append(buffer, chunk...)
			}
			r.inputBuffer = r.inputBuffer[1:]
		}
	}
	return buffer, r.signalEOF.Load()
}

const fixedOverhead = 6 /*header*/ + 8 /*seq num*/ + 16 /*aead overhead*/

const payloadOverhead = 2 /*name len + name*/ + 1 /*type*/ + 1 /*flags*/
func StreamPacketHeaderSize(id StreamID, offset uint64, dataLen int) int {
	size := payloadOverhead
	if id != 0 {
		size += int(wire.VarintLen(uint64(id)))
	}
	if offset != 0 {
		size += int(wire.VarintLen(offset))
	}
	if dataLen != 0 {
		size += int(wire.VarintLen(uint64(dataLen)))
	}
	return size
}

func (r *sendStream) triggerPacket(maxPayload int) *SentRange {
	r.m.Lock()
	defer r.m.Unlock()
	if popped := r.retransmitQueue.Pop(); popped != nil {
		headerSize := StreamPacketHeaderSize(popped.ID, popped.Offset, len(popped.Data))
		if maxPayload <= headerSize {
			// cannot send now, push back
			r.retransmitQueue.Push(popped)
		} else {
			r.logger.Debug("retransmitting stream data", "id", popped.ID, "offset", popped.Offset, "size", len(popped.Data))
			return popped
		}
	}
	if r.eofSent {
		return nil
	}
	// Reserve header space using maxPayload as the data-length upper bound:
	// the actual chunk is at most (maxPayload - headerSize), so VarintLen of
	// maxPayload upper-bounds the length-field width — never an under-estimate.
	// The previous fixed 1500 under-reserved once the MTU (hence a chunk)
	// crossed a varint boundary (e.g. a chunk >16383 needs a 4-byte length
	// where 1500 assumed 2), pushing the packet past maxPayload — latent until
	// MTU is raised above ~1500 for stream transports.
	headerSize := StreamPacketHeaderSize(r.id, r.offset, maxPayload)
	if maxPayload <= headerSize {
		return nil
	}
	maxPayload -= headerSize
	sendable := r.flow.SendableSize()
	var data []byte
	var eof bool
	if sendable > 0 {
		chunkSize := min(maxPayload, sendable)
		data, eof = r.readChunk(chunkSize)
	}
	if len(data) == 0 && !eof {
		// No buffered data and no EOF: only emit a frame to advertise stream
		// creation, and only once per stream. The peer's recv path creates
		// its stream entry on the first frame for an unknown id; without this
		// open marker, an idle freshly-created stream stays invisible.
		if !r.pendingOpen.CompareAndSwap(true, false) {
			return nil
		}
	}
	sentRange := &SentRange{
		ID:       r.id,
		Offset:   r.offset,
		Data:     data,
		SentSize: len(data),
		Eof:      eof,
	}
	onLost := func(now time.Time) {
		r.retransmitQueue.Push(sentRange)
		r.sendTrigger.PushBecause(r, pushLoss)
	}
	onACK := func(now time.Time) {
		r.onACK(sentRange, now)
	}
	sentRange.OnACK = onACK
	sentRange.OnLost = onLost
	r.logger.Debug("prepared stream data for sending", "id", r.id, "offset", sentRange.Offset, "size", sentRange.SentSize, "eof", sentRange.Eof, "offset+size", sentRange.Offset+uint64(sentRange.SentSize))
	r.offset += uint64(len(data))
	r.sentRanges = append(r.sentRanges, sentRange)
	if eof {
		r.eofSent = true
	}
	r.flow.RecordSend(sentRange.SentSize)
	if r.flow.SendableSize() > 0 && len(r.inputBuffer) > 0 || (!r.eofSent && r.signalEOF.Load()) {
		r.sendTrigger.PushBecause(r, pushSelf) // trigger next send
	}
	r.signalWriter()
	return sentRange
}

func (r *sendStream) onACK(pkt *SentRange, now time.Time) {
	r.m.Lock()
	defer r.m.Unlock()
	// Filtered in place. This built a fresh zero-capacity slice and grew it
	// back up on every ACK, so the cost scaled with the number of ranges in
	// flight -- one allocation per ACK at a small congestion window, and
	// repeated growslice per ACK once the window opened. It was 16.7% of the
	// process's allocations and 43% of all time in runtime.growslice. Same
	// shape as the O(n^2) bytes-in-flight audit removed in cc75e99: harmless
	// while the window is small, dominant once it is not.
	//
	// Writes trail reads over the same backing array, so this is safe; the
	// tail is nil'd so the dropped ranges are not kept alive by it.
	// ACKs arrive in send order and sentRanges is appended in send order, so
	// the match is nearly always the head. Dropping it is O(1) and moves
	// nothing; the filter below is the fallback for out-of-order ACKs.
	if len(r.sentRanges) > 0 && r.sentRanges[0] == pkt {
		r.sentRanges[0] = nil
		r.sentRanges = r.sentRanges[1:]
		r.sendTrigger.PushBecause(r, pushACK)
		return
	}
	kept := r.sentRanges[:0]
	for _, sr := range r.sentRanges {
		if sr != pkt {
			kept = append(kept, sr)
		}
	}
	for i := len(kept); i < len(r.sentRanges); i++ {
		r.sentRanges[i] = nil
	}
	r.sentRanges = kept
	r.sendTrigger.PushBecause(r, pushACK)
}

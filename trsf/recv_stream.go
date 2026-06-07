package trsf

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
)

type RecvRange struct {
	Offset uint64
	Data   []byte
	Eof    bool
}

const InitialFlowWindow = 16 * 1024 * 1024 // 16MB

type recvStream struct {
	m             sync.Mutex
	id            StreamID
	logger        *slog.Logger
	oq            *OrderingQueue
	updateWindow  *withTriggerQueue[recvStream] // shared with conn
	cancelStream  *withTriggerQueue[recvStream] // shared with conn
	cancelSent    bool
	initialWindow uint64
	recvWindow    uint64
	ackedWindow   uint64
	ctx           context.Context
}

func newReceiveStream(ctx context.Context, logger *slog.Logger, id StreamID, initialWindow uint64, updateWindow *withTriggerQueue[recvStream], cancelStream *withTriggerQueue[recvStream]) *recvStream {
	return &recvStream{
		id:            id,
		oq:            NewOrderingQueue(),
		initialWindow: initialWindow,
		recvWindow:    initialWindow,
		logger:        logger,
		ackedWindow:   initialWindow,
		updateWindow:  updateWindow,
		cancelStream:  cancelStream,
		ctx:           ctx,
	}
}

func (rc *recvStream) HasRecvData() bool {
	rc.m.Lock()
	defer rc.m.Unlock()
	return rc.oq.HasData()
}

func (rc *recvStream) EOF() bool {
	rc.m.Lock()
	defer rc.m.Unlock()
	return rc.oq.EOF()
}

func (rc *recvStream) ProcessChunk(offset uint64, data []byte, eof bool) (bool, error) {
	rc.m.Lock()
	defer rc.m.Unlock()
	if rc.oq.EOF() {
		rc.logger.Debug("received data after EOF", "stream_id", rc.id, "offset", offset)
		return false, nil
	}
	rc.logger.Debug("received data chunk", "stream_id", rc.id, "offset", offset, "length", len(data), "eof", eof)
	err := rc.oq.Push(&RecvData{
		Offset: offset,
		Data:   data,
		Eof:    eof,
	})
	if err != nil {
		return false, fmt.Errorf("failed to push received data for id %d at offset %d: %w", rc.id, offset, err)
	}
	return true, nil
}

func (r *recvStream) Window() uint64 {
	r.m.Lock()
	defer r.m.Unlock()
	return r.recvWindow
}

func (r *recvStream) onWindowAck(size uint64) {
	r.m.Lock()
	defer r.m.Unlock()
	r.ackedWindow = size
}

func (r *recvStream) Cancel() {
	r.m.Lock()
	defer r.m.Unlock()
	if r.cancelSent {
		return
	}
	// clear oq
	r.cancelStream.Push(r)
	r.cancelSent = true
	r.oq.Notify()
}

func (r *recvStream) ReadDirectContext(ctx context.Context, maxN uint64) ([]byte, bool, error) {
	for {
		r.m.Lock()
		data, eof := r.oq.ReadDirect(maxN)
		if len(data) > 0 {
			r.recvWindow += uint64(len(data))
			// if window is less than half, notify to update
			if r.recvWindow-r.ackedWindow >= r.initialWindow/2 {
				r.updateWindow.Push(r)
			}
		}
		if len(data) == 0 && !eof { // nothing to read,
			if r.cancelSent {
				r.m.Unlock()
				return nil, false, context.Canceled
			}
			r.m.Unlock()
			select {
			case <-ctx.Done():
				return nil, false, ctx.Err()
			case <-r.ctx.Done():
				return nil, false, r.ctx.Err()
			case <-r.oq.Notification():
				continue // try to read again
			}
		}
		r.m.Unlock()
		return data, eof, nil
	}
}

func (r *recvStream) ReadDirect(maxN uint64) ([]byte, bool, error) {
	return r.ReadDirectContext(context.Background(), maxN)
}

func (r *recvStream) ReadContext(ctx context.Context, p []byte) (n int, err error) {
	for {
		r.m.Lock()
		n, err = r.oq.Read(p)
		if n > 0 {
			r.recvWindow += uint64(n)
			// if window is less than half, notify to update
			if r.recvWindow-r.ackedWindow >= r.initialWindow/2 {
				r.updateWindow.Push(r)
			}
		}
		if n == 0 && err == nil { // nothing to read,
			if r.cancelSent {
				r.m.Unlock()
				return 0, context.Canceled
			}
			r.m.Unlock()
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-r.ctx.Done():
				return 0, r.ctx.Err()
			case <-r.oq.Notification():
				continue // try to read again
			}
		}
		r.m.Unlock()
		return n, err
	}
}

func (r *recvStream) Read(b []byte) (n int, err error) {
	return r.ReadContext(context.Background(), b)
}

package trsf

import (
	"context"
	"errors"
	"io"
)

// ErrStreamFinished is returned when bytes are written to a direction that has
// already ended. It is not a connection error: the stream did its job and was
// reaped, and the caller is holding a view assembled after the fact.
var ErrStreamFinished = errors.New("trsf: stream direction already finished")

// finishedSendStream stands in for a send half that completed and was removed
// from the table, so a bidirectional view can still be assembled from the recv
// half that outlives it. See GetBidirectionalStream.
//
// It never emits a frame. Re-creating the real half instead would be wrong:
// removal means EOF was sent and acknowledged, and a resurrected stream would
// continue the offset sequence past an EOF the peer has already seen.
type finishedSendStream struct{ id StreamID }

func (f finishedSendStream) ID() StreamID { return f.id }

func (f finishedSendStream) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	return 0, ErrStreamFinished
}

func (f finishedSendStream) WriteContext(ctx context.Context, data []byte) (int, error) {
	return f.Write(data)
}

// Close succeeds: closing what is already closed is not an error, and callers
// routinely defer it.
func (f finishedSendStream) Close() error { return nil }

func (f finishedSendStream) HasSendData() bool { return false }

func (f finishedSendStream) Completed() bool { return true }

// AppendData accepts a bare EOF for the same reason Close succeeds, and refuses
// bytes rather than dropping them silently.
func (f finishedSendStream) AppendData(eof bool, data ...[]byte) error {
	for _, d := range data {
		if len(d) > 0 {
			return ErrStreamFinished
		}
	}
	return nil
}

func (f finishedSendStream) AppendDataContext(ctx context.Context, eof bool, data ...[]byte) error {
	return f.AppendData(eof, data...)
}

// finishedRecvStream is the receiving counterpart: a half whose EOF was
// received and whose entry has since aged out. It reports the direction as
// drained, which is what it is — a reader that gets EOF here has not lost data,
// because the entry is only dropped after the grace period following EOF.
type finishedRecvStream struct{ id StreamID }

func (f finishedRecvStream) ID() StreamID { return f.id }

func (f finishedRecvStream) Read(p []byte) (int, error) { return 0, io.EOF }

func (f finishedRecvStream) ReadContext(ctx context.Context, p []byte) (int, error) {
	return 0, io.EOF
}

func (f finishedRecvStream) ReadDirect(maxN uint64) ([]byte, bool, error) {
	return nil, true, nil
}

func (f finishedRecvStream) ReadDirectContext(ctx context.Context, maxN uint64) ([]byte, bool, error) {
	return nil, true, nil
}

func (f finishedRecvStream) HasRecvData() bool { return false }

func (f finishedRecvStream) EOF() bool { return true }

func (f finishedRecvStream) Cancel() {}

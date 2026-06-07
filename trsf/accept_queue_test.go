package trsf

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/on-keyday/objtrsf/objproto"
)

// stubPNIssuer is a trivial PacketNumberIssuer for constructing a Streams in
// tests; getRecvStream does not consume packet numbers, so a counter suffices.
type stubPNIssuer struct{ n uint64 }

func (s *stubPNIssuer) ConsumePacketNumber() objproto.PacketNumber { s.n++; return s.n }

// TestStreams_PeerInitiatedStreamAcceptQueueDoesNotWedge reproduces the
// production freeze: every peer-initiated stream's first frame runs through
// getRecvStream, which enqueues the new stream onto newBidiStreamQueue /
// newRecvStreamQueue (cap 100) for the Accept* API. No production code calls
// AcceptBidirectionalStream/AcceptReceiveStream, so the queue is never drained.
// Once 100 peer-initiated streams accumulate on a long-lived connection (e.g. a
// WebUI snapshot poll opens a server send-stream every 5s -> ~8 min to fill),
// the 101st enqueue blocks getRecvStream WHILE IT HOLDS streamsLock, wedging the
// entire streams layer: all inbound demux and stream creation stall, while
// connection-level pings keep flowing so the peer still looks connected.
func TestStreams_PeerInitiatedStreamAcceptQueueDoesNotWedge(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tr := NewStreams(ctx, false /* client */, DefaultInitialMTU, DefaultMaxMTU,
		&stubPNIssuer{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	s := tr.(*Streams)

	// Drive more peer-initiated (server-initiated, from the client's view)
	// bidi streams through getRecvStream than the accept queue can hold.
	const n = 150
	done := make(chan struct{})
	go func() {
		id := ServerBidirectionalStart // 0, 4, 8, ... — server-initiated + bidi
		for i := 0; i < n; i++ {
			if s.getRecvStream(id) == nil {
				t.Errorf("getRecvStream(%d) returned nil", id)
				return
			}
			id = id.Next()
		}
		close(done)
	}()

	select {
	case <-done:
		// All streams materialized without wedging — good.
	case <-time.After(3 * time.Second):
		t.Fatalf("getRecvStream wedged after ~%d streams: the cap-%d accept queue "+
			"filled and the blocking enqueue stalled the demux while holding streamsLock",
			cap(s.newBidiStreamQueue), cap(s.newBidiStreamQueue))
	}
}

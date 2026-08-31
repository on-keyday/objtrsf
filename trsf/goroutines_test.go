package trsf_test

import (
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

// A bulk transfer must not need hundreds of goroutines. It once did: with
// objproto's message channel 10 deep, 59.5% of received messages fell back to
// SendMessage's goroutine-per-message path and the process peaked at 523 for
// a connection that structurally needs about nine -- two trsf run loops, two
// AutoSend, two AutoReceive, and a sender and reader per endpoint.
//
// This guards the depth constant that fixed it (objproto.messageChannelDepth)
// against being lowered back, and any future change that reintroduces a
// per-message goroutine. It samples rather than instruments, so it is a floor
// on the peak, not the peak.
func TestPeakGoroutines(t *testing.T) {
	const limit = 100

	ctx := t.Context()
	source, sink := udpPair(t)
	send, recv := connectedStream(t, ctx, source, sink)

	var peak atomic.Int64
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
			}
			if n := int64(runtime.NumGoroutine()); n > peak.Load() {
				peak.Store(n)
			}
			time.Sleep(200 * time.Microsecond)
		}
	}()

	const size = 16 << 20
	if err := bulkTransfer(ctx, send, recv, size, relayChunk); err != nil {
		t.Fatalf("transfer: %v", err)
	}
	close(stop)
	<-done

	t.Logf("peak goroutines = %d", peak.Load())
	if peak.Load() > limit {
		t.Fatalf("peak of %d goroutines for one bulk transfer, over the limit of %d: "+
			"something is spawning per message again", peak.Load(), limit)
	}
}

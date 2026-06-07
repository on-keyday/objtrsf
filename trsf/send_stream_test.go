package trsf

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"
)

// TestSendStream_AppendDataContext_CancelWhileBufferFull reproduces the
// double-unlock crash in AppendDataContext: the buffer-full branch releases
// r.m explicitly, waits, and on r.ctx cancellation returns WITHOUT re-acquiring
// the lock — so the function's top-level `defer r.m.Unlock()` unlocks an already
// unlocked mutex, which is a fatal (unrecoverable) runtime error that crashes
// the whole process. In production this fired in runnerPump's tui.AppendData
// when the tui stream was cancelled (e.g. reattach takeover -> old.CloseBoth())
// while the send buffer was full.
//
// Before the fix this test crashes the test binary ("unlock of unlocked
// mutex"); after the fix AppendData returns the cancellation error cleanly.
func TestSendStream_AppendDataContext_CancelWhileBufferFull(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tr := NewStreams(ctx, false, DefaultInitialMTU, DefaultMaxMTU,
		&stubPNIssuer{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	s := tr.(*Streams)
	ss := s.CreateSendStream().(*sendStream)

	// bufferLimit 0 makes the very first append take the buffer-full wait
	// branch (dataInBuffer-offset >= 0 is always true), deterministically
	// parking the call in the select that the bug mishandles.
	ss.m.Lock()
	ss.bufferLimit = 0
	ss.m.Unlock()

	errCh := make(chan error, 1)
	go func() { errCh <- ss.AppendData(false, []byte("x")) }()

	// Give the goroutine time to reach the buffer-full wait, then cancel the
	// stream's ctx (via the Streams ctx) so r.ctx.Done() fires.
	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatalf("expected a cancellation error, got nil")
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("AppendData did not return after ctx cancel")
	}
}

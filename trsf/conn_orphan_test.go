package trsf_test

import (
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/on-keyday/objtrsf/trsf/mock"
)

// TestManyConcurrentSendStreamsAllDelivered reproduces the cwnd-orphan bug.
//
// A send stream that reaches the head of the send queue while the connection
// is ALREADY congestion-blocked — before it has transmitted even its first
// packet — has no in-flight packet of its own. Every revival path is keyed on
// the stream's own sent packets (onACK / onLost) or on the peer having already
// received it (window update), so none ever fires for such a stream. The run
// loop's "do not re-push the congestion-blocked stream" optimization then drops
// it and it is never re-queued: it orphans forever.
//
// With many concurrent send streams whose combined size exceeds the initial
// congestion window (2*MTU = 2400 bytes), the first stream or two fill cwnd and
// every later stream is popped-while-blocked before its first send → orphaned →
// never delivered to the peer. Before the fix this times out; after it, all
// streams arrive.
func TestManyConcurrentSendStreamsAllDelivered(t *testing.T) {
	ctx := t.Context()
	client, server := mock.SetupClientServer(t)
	mock.BackgroundIO(t, client, server)

	const n = 8
	const sz = 2048 // each ~2KB; sum (16KB) >> initial cwnd (2*1200 = 2400)

	for i := 0; i < n; i++ {
		ss := client.CreateSendStream()
		if ss == nil {
			t.Fatalf("CreateSendStream %d returned nil", i)
		}
		if err := ss.AppendData(true, bytes.Repeat([]byte{byte('A' + i)}, sz)); err != nil {
			t.Fatalf("AppendData %d: %v", i, err)
		}
	}

	// Accept + fully drain all n streams concurrently; report each completion.
	done := make(chan int, n)
	for i := 0; i < n; i++ {
		go func() {
			rs, err := server.AcceptReceiveStream(ctx)
			if err != nil {
				return
			}
			data, err := io.ReadAll(rs)
			if err != nil {
				return
			}
			done <- len(data)
		}()
	}

	received, total := 0, 0
	timeout := time.After(15 * time.Second)
	for received < n {
		select {
		case nbytes := <-done:
			received++
			total += nbytes
		case <-timeout:
			t.Fatalf("only %d/%d concurrent send streams delivered (%d bytes) before "+
				"timeout: streams congestion-blocked before their first send were "+
				"orphaned and never re-queued", received, n, total)
		}
	}
	if want := n * sz; total != want {
		t.Fatalf("delivered byte total = %d, want %d", total, want)
	}
}

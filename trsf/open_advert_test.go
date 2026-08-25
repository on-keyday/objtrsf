package trsf_test

import (
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/on-keyday/objtrsf/trsf"
	"github.com/on-keyday/objtrsf/trsf/mock"
)

// waitVisible polls the peer's stream table the way the harness's
// peer.WaitForBidirectionalStream does, and reports how long it took.
func waitVisible(peer trsf.Transport, id trsf.StreamID, within time.Duration) (bool, time.Duration) {
	start := time.Now()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if s := peer.GetBidirectionalStream(id); s != nil {
			return true, time.Since(start)
		}
		time.Sleep(5 * time.Millisecond)
	}
	return false, time.Since(start)
}

// A freshly created stream that nobody writes to must still become resolvable
// on the peer: CreateBidirectionalStream queues a 0-byte STREAM frame exactly
// so that GetBidirectionalStream(id) can find an idle stream.
func TestIdleStreamIsAdvertised(t *testing.T) {
	client, server := mock.SetupClientServerEx(t, slog.LevelWarn)
	mock.BackgroundIO(t, client, server)

	s := server.CreateBidirectionalStream()
	if s == nil {
		t.Fatal("server failed to create a stream")
	}
	ok, took := waitVisible(client, s.ID(), 3*time.Second)
	if !ok {
		t.Fatalf("idle stream %d never became visible to the peer (waited %v)", s.ID(), took)
	}
}

// The same, under the churn an exec produces: create a pair, close them, create
// the next pair. This is the shape that intermittently loses the advertisement
// when run through the harness.
func TestIdleStreamIsAdvertisedUnderChurn(t *testing.T) {
	client, server := mock.SetupClientServerEx(t, slog.LevelWarn)
	mock.BackgroundIO(t, client, server)

	for round := 0; round < 8; round++ {
		a := server.CreateBidirectionalStream()
		b := server.CreateBidirectionalStream()
		if a == nil || b == nil {
			t.Fatalf("round %d: creation returned nil", round)
		}
		okA, tookA := waitVisible(client, a.ID(), 3*time.Second)
		okB, tookB := waitVisible(client, b.ID(), 3*time.Second)
		if !okA || !okB {
			t.Fatalf("round %d: stream %d visible=%v (%v), stream %d visible=%v (%v)",
				round, a.ID(), okA, tookA, b.ID(), okB, tookB)
		}
		t.Logf("round %d: %d in %v, %d in %v", round, a.ID(), tookA, b.ID(), tookB)
		_ = a.CloseBoth()
		_ = b.CloseBoth()
	}
}

// A stream the creator writes to and then CloseBoth()s stays resolvable by id
// on the peer while its bytes are unread.
//
// CloseBoth cancels the recv half, which sends a StreamCancel for that id; the
// peer's send half of the same id then EOFs itself (readChunk returns EOF
// immediately once cancelRequested is set) and, once that range is acked,
// Completed() goes true and the run loop drops it from sendStreams. That used
// to end the lookup, because GetBidirectionalStream required BOTH halves —
// within ~5ms of the close, while ReadDirect on a held pointer would still hand
// back the payload. It now composes the surviving recv half with a finished
// send stub instead.
func TestCloseBothKeepsTheStreamResolvableByID(t *testing.T) {
	client, server := mock.SetupClientServerEx(t, slog.LevelWarn)
	mock.BackgroundIO(t, client, server)

	s := server.CreateBidirectionalStream()
	if s == nil {
		t.Fatal("server failed to create a stream")
	}

	// Let the creation advertisement land FIRST, so the cancel that follows
	// finds a materialized send half to reap. Arriving in the other order the
	// peer logs "cancel for unknown stream" and drops it, which hid the defect
	// and made the harness symptom intermittent.
	ok, took := waitVisible(client, s.ID(), 3*time.Second)
	if !ok {
		t.Fatalf("stream %d never became visible at all (waited %v)", s.ID(), took)
	}

	if _, err := s.Write([]byte("outcome")); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = s.CloseBoth()

	start := time.Now()
	for time.Since(start) < 500*time.Millisecond {
		if client.GetBidirectionalStream(s.ID()) == nil {
			t.Fatalf("stream %d stopped resolving by id %v after CloseBoth, with its bytes unread",
				s.ID(), time.Since(start))
		}
		time.Sleep(5 * time.Millisecond)
	}

	// And the payload is still readable through the composed view.
	got := client.GetBidirectionalStream(s.ID())
	if got == nil {
		t.Fatal("lookup went nil at the read")
	}
	var raw []byte
	for {
		chunk, eof, err := got.ReadDirect(64 * 1024)
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		raw = append(raw, chunk...)
		if eof {
			break
		}
	}
	if string(raw) != "outcome" {
		t.Errorf("read %q, want the bytes written before CloseBoth", raw)
	}
}

// The reaped half is reported as finished rather than as a fresh stream, and a
// unidirectional id is not composable into a bidirectional view at all.
func TestFinishedHalfAndUnidirectionalIDs(t *testing.T) {
	client, server := mock.SetupClientServerEx(t, slog.LevelWarn)
	mock.BackgroundIO(t, client, server)

	uni := server.CreateSendStream()
	if uni == nil {
		t.Fatal("server failed to create a send stream")
	}
	if err := uni.AppendData(true, []byte("row")); err != nil {
		t.Fatalf("append: %v", err)
	}
	// It must arrive as a RECEIVE stream...
	deadline := time.Now().Add(3 * time.Second)
	for client.GetReceiveStream(uni.ID()) == nil {
		if time.Now().After(deadline) {
			t.Fatalf("unidirectional stream %d never arrived", uni.ID())
		}
		time.Sleep(5 * time.Millisecond)
	}
	// ...and never as a bidirectional one, stub or not.
	if got := client.GetBidirectionalStream(uni.ID()); got != nil {
		t.Errorf("unidirectional stream %d resolved as bidirectional", uni.ID())
	}

	bidi := server.CreateBidirectionalStream()
	if ok, took := waitVisible(client, bidi.ID(), 3*time.Second); !ok {
		t.Fatalf("stream %d never became visible (waited %v)", bidi.ID(), took)
	}
	if _, err := bidi.Write([]byte("x")); err != nil {
		t.Fatalf("write: %v", err)
	}
	_ = bidi.CloseBoth()

	// Once the peer's send half is reaped, writing must fail loudly instead of
	// being dropped, and Completed must say the direction is over.
	deadline = time.Now().Add(3 * time.Second)
	for {
		view := client.GetBidirectionalStream(bidi.ID())
		if view == nil {
			t.Fatalf("stream %d stopped resolving", bidi.ID())
		}
		if view.Completed() {
			if _, err := view.Write([]byte("late")); !errors.Is(err, trsf.ErrStreamFinished) {
				t.Errorf("write to a finished half = %v, want ErrStreamFinished", err)
			}
			if err := view.Close(); err != nil {
				t.Errorf("Close on a finished half = %v, want nil", err)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the peer's send half never completed")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

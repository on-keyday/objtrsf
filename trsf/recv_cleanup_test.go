package trsf_test

import (
	"io"
	"testing"
	"time"

	"github.com/on-keyday/objtrsf/trsf"
	"github.com/on-keyday/objtrsf/trsf/mock"
)

// setRemovalGrace shrinks the recv-stream removal grace period (production
// default 1 minute) so the tests can observe the post-grace state quickly.
func setRemovalGrace(t *testing.T, tr trsf.Transport, d time.Duration) {
	t.Helper()
	s, ok := tr.(interface{ SetRecvStreamRemovalGrace(time.Duration) })
	if !ok {
		t.Fatalf("transport %T does not expose SetRecvStreamRemovalGrace", tr)
	}
	s.SetRecvStreamRemovalGrace(d)
}

// waitForRecvStreams polls until the transport reports want active receive
// streams, failing after deadline. Removal is timer-driven (grace period), so
// polling is the honest way to observe it.
func waitForRecvStreams(t *testing.T, tr trsf.Transport, want int, what string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		got := tr.GetInternalState().ActiveReceiveStreams
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: ActiveReceiveStreams = %d, want %d (recv stream entry leaked)", what, got, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Regression test for the fleet memory leak: a request/response exchange on a
// bidirectional stream (the /metrics-over-transport scrape shape) must not
// permanently retain the recvStreams map entry on either side once EOF has
// been received and consumed.
func TestRecvStreamRemovedAfterEOFConsumed(t *testing.T) {
	ctx := t.Context()
	client, server := mock.SetupClientServer(t)
	setRemovalGrace(t, client, 50*time.Millisecond)
	setRemovalGrace(t, server, 50*time.Millisecond)
	mock.BackgroundIO(t, client, server)

	bidi := client.CreateBidirectionalStream()
	if bidi == nil {
		t.Fatal("failed to create bidirectional stream")
	}
	if err := bidi.AppendData(true, []byte("GET /metrics")); err != nil {
		t.Fatalf("failed to send request: %v", err)
	}
	accepted, err := server.AcceptBidirectionalStream(ctx)
	if err != nil {
		t.Fatalf("failed to accept bidirectional stream: %v", err)
	}
	req, err := io.ReadAll(accepted)
	if err != nil {
		t.Fatalf("failed to read request to EOF: %v", err)
	}
	if string(req) != "GET /metrics" {
		t.Fatalf("request mismatch: %q", req)
	}
	if err := accepted.AppendData(true, []byte("metrics body")); err != nil {
		t.Fatalf("failed to send response: %v", err)
	}
	resp, err := io.ReadAll(bidi)
	if err != nil {
		t.Fatalf("failed to read response to EOF: %v", err)
	}
	if string(resp) != "metrics body" {
		t.Fatalf("response mismatch: %q", resp)
	}

	waitForRecvStreams(t, server, 0, "server after exchange")
	waitForRecvStreams(t, client, 0, "client after exchange")
}

// A receive stream the reader cancels (no EOF ever arrives) must also leave
// the recvStreams map once the cancel has been issued.
func TestRecvStreamRemovedAfterCancel(t *testing.T) {
	ctx := t.Context()
	client, server := mock.SetupClientServer(t)
	setRemovalGrace(t, client, 50*time.Millisecond)
	setRemovalGrace(t, server, 50*time.Millisecond)
	mock.BackgroundIO(t, client, server)

	sendStream := client.CreateSendStream()
	if sendStream == nil {
		t.Fatal("failed to create send stream")
	}
	sendStream.AppendData(false, []byte("data before cancel"))
	recvStream, err := server.AcceptReceiveStream(ctx)
	if err != nil {
		t.Fatalf("failed to accept receive stream: %v", err)
	}
	buf := make([]byte, 64)
	if _, err := recvStream.Read(buf); err != nil {
		t.Fatalf("failed to read before cancel: %v", err)
	}
	recvStream.Cancel()

	waitForRecvStreams(t, server, 0, "server after cancel")
}

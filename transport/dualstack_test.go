package transport

import (
	"net/netip"
	"sync/atomic"
	"testing"
	"time"

	"github.com/on-keyday/objtrsf/objproto"
)

// fakeCancelSink records CannotSend invocations so the test can verify
// unsupported-transport packets are correctly funneled into the
// "undeliverable" path instead of being silently dropped.
type fakeCancelSink struct {
	calls atomic.Int32
}

func (f *fakeCancelSink) CannotSend(*objproto.PacketData) {
	f.calls.Add(1)
}

func mkPacket(transport string) *objproto.PacketData {
	return &objproto.PacketData{
		To: objproto.ConnectionID{
			Transport: transport,
			Addr:      netip.MustParseAddrPort("127.0.0.1:9000"),
		},
		Data: []byte{0x42},
	}
}

func recvWithTimeout(t *testing.T, ch <-chan *objproto.PacketData, want int, timeout time.Duration) []*objproto.PacketData {
	t.Helper()
	got := make([]*objproto.PacketData, 0, want)
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for len(got) < want {
		select {
		case pkt := <-ch:
			got = append(got, pkt)
		case <-deadline.C:
			t.Fatalf("timeout: got %d/%d packets in %s", len(got), want, timeout)
		}
	}
	return got
}

func TestFanOutByTransport_RoutesUDPAndWS(t *testing.T) {
	src := make(chan *objproto.PacketData, 8)
	udpDst := make(chan *objproto.PacketData, 8)
	wsDst := make(chan *objproto.PacketData, 8)
	sink := &fakeCancelSink{}

	go fanOutByTransport(sink, src, udpDst, wsDst, nil)

	src <- mkPacket("udp")
	src <- mkPacket("ws")
	src <- mkPacket("wss")
	src <- mkPacket("udp")

	udpGot := recvWithTimeout(t, udpDst, 2, 1*time.Second)
	wsGot := recvWithTimeout(t, wsDst, 2, 1*time.Second)

	if len(udpGot) != 2 {
		t.Errorf("udp got %d, want 2", len(udpGot))
	}
	if len(wsGot) != 2 {
		t.Errorf("ws got %d, want 2", len(wsGot))
	}
	if sink.calls.Load() != 0 {
		t.Errorf("CannotSend calls = %d, want 0 (no unsupported transport)", sink.calls.Load())
	}

	// Drain by closing the source so the goroutine can exit.
	close(src)
}

func TestFanOutByTransport_UnsupportedRoutesToCannotSend(t *testing.T) {
	src := make(chan *objproto.PacketData, 4)
	udpDst := make(chan *objproto.PacketData, 4)
	wsDst := make(chan *objproto.PacketData, 4)
	sink := &fakeCancelSink{}

	go fanOutByTransport(sink, src, udpDst, wsDst, nil)

	src <- mkPacket("bogus")
	src <- mkPacket("smtp") // also unsupported

	deadline := time.Now().Add(1 * time.Second)
	for time.Now().Before(deadline) {
		if sink.calls.Load() == 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := sink.calls.Load(); got != 2 {
		t.Errorf("CannotSend calls = %d, want 2", got)
	}
	if len(udpDst) != 0 {
		t.Errorf("udpDst received unexpected packets: %d", len(udpDst))
	}
	if len(wsDst) != 0 {
		t.Errorf("wsDst received unexpected packets: %d", len(wsDst))
	}
	close(src)
}

// TestFanOutByTransport_NoLossUnderConcurrency ensures every packet
// pushed onto the source channel emerges on exactly one of the two
// destination channels — i.e., neither dropped nor duplicated. This is
// the regression test for the "BROKEN AS-IS" race that the dualstack
// fanout had before transport/websocket.go got its sendTo arg back.
func TestFanOutByTransport_NoLossUnderConcurrency(t *testing.T) {
	const n = 1000
	src := make(chan *objproto.PacketData, 16)
	udpDst := make(chan *objproto.PacketData, n)
	wsDst := make(chan *objproto.PacketData, n)
	sink := &fakeCancelSink{}

	go fanOutByTransport(sink, src, udpDst, wsDst, nil)

	go func() {
		for i := 0; i < n; i++ {
			if i%2 == 0 {
				src <- mkPacket("udp")
			} else {
				src <- mkPacket("ws")
			}
		}
		close(src)
	}()

	// Each side should see exactly n/2.
	udpCount := 0
	wsCount := 0
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for udpCount+wsCount < n {
		select {
		case <-udpDst:
			udpCount++
		case <-wsDst:
			wsCount++
		case <-deadline.C:
			t.Fatalf("timeout: routed %d/%d (udp=%d, ws=%d)", udpCount+wsCount, n, udpCount, wsCount)
		}
	}
	if udpCount != n/2 {
		t.Errorf("udp count = %d, want %d", udpCount, n/2)
	}
	if wsCount != n/2 {
		t.Errorf("ws count = %d, want %d", wsCount, n/2)
	}
	if sink.calls.Load() != 0 {
		t.Errorf("CannotSend calls = %d, want 0", sink.calls.Load())
	}
}

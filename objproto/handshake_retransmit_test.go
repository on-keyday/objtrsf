package objproto

import (
	"bytes"
	"context"
	"crypto/ecdh"
	"io"
	"log/slog"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/on-keyday/objtrsf/objproto/packet"
)

// A handshake runs before trsf exists, so nothing under objproto retransmits it
// -- prevKeyRetention's comment defers to trsf for exactly that reason. One
// dropped datagram therefore used to cost the whole dial, and a dropped ACK
// used to cost the connection id permanently: the dialer's repeat was refused
// with "connection already exists" and the responder never re-acked.
//
// These wire two endpoints through a pipe that drops chosen packets, which is
// the only way to tell the two loss cases apart.

type dropper func(*PacketData) bool

// pipeDropping joins two endpoints and drops whatever the filters say to. The
// filters see each packet once, in flight, so "the first ack" is expressible.
func pipeDropping(t *testing.T, aToB, bToA dropper) (*endpoint, *endpoint) {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	a := NewEndpoint(logger, EndpointModeMutual).(*endpoint)
	b := NewEndpoint(logger, EndpointModeMutual).(*endpoint)

	done := make(chan struct{})
	var wg sync.WaitGroup
	pump := func(from, to *endpoint, drop dropper) {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			case pkt := <-from.GetSenderChannel():
				if drop != nil && drop(pkt) {
					continue
				}
				// The mock hands the DESTINATION address to Receive as the
				// source, as transport.InMemoryPipeSession does: it keeps one
				// address on both sides so the cids line up.
				_ = to.Receive(pkt.To.Transport, pkt.To.Addr, pkt.Data)
			}
		}
	}
	wg.Add(2)
	go pump(a, b, aToB)
	go pump(b, a, bToA)
	t.Cleanup(func() { close(done); wg.Wait() })
	return a, b
}

func dropFirst(kind packet.PacketKind, n int64) (dropper, *atomic.Int64) {
	left := &atomic.Int64{}
	left.Store(n)
	seen := &atomic.Int64{}
	return func(p *PacketData) bool {
		if p.Kind != kind {
			return false
		}
		seen.Add(1)
		return left.Add(-1) >= 0
	}, seen
}

func dialTo(t *testing.T, from *endpoint, addr string) (Connection, error) {
	t.Helper()
	cid := NewConnectionID("udp", netip.MustParseAddrPort(addr), 4242)
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	return DoECDHHandshake(ctx, from, cid, ecdh.X25519(), packet.CommonKeyKind_Aes128Gcm)
}

// The case that used to be unrecoverable. The responder has the connection, the
// dialer does not, and every repeat was refused.
func TestHandshakeCompletesAfterItsAckIsLost(t *testing.T) {
	dropAck, acksSeen := dropFirst(packet.PacketKind_HandshakeAck, 1)
	a, _ := pipeDropping(t, nil, dropAck)

	conn, err := dialTo(t, a, "10.0.0.2:5000")
	if err != nil {
		t.Fatalf("a dropped ack must be recoverable, got: %v", err)
	}
	if conn == nil {
		t.Fatal("no connection")
	}
	if n := acksSeen.Load(); n < 2 {
		t.Errorf("the responder emitted %d acks; the second one is the replay, so it must have been asked for", n)
	}
}

// The other loss case, which a dialer-side retransmission alone can fix: the
// responder never saw the hello, so it has no state and answers normally.
func TestHandshakeCompletesAfterTheHelloIsLost(t *testing.T) {
	dropHello, _ := dropFirst(packet.PacketKind_Handshake, 1)
	a, _ := pipeDropping(t, dropHello, nil)

	if _, err := dialTo(t, a, "10.0.0.3:5000"); err != nil {
		t.Fatalf("a dropped hello must be recoverable, got: %v", err)
	}
}

// A retransmission that is not byte-identical is not a retransmission: the
// stored ack is bound to the hello that produced it, so a rotated ephemeral key
// would make the replay underivable.
func TestRetransmittedHelloIsByteIdentical(t *testing.T) {
	var mu sync.Mutex
	var hellos [][]byte
	capture := func(p *PacketData) bool {
		if p.Kind == packet.PacketKind_Handshake {
			mu.Lock()
			hellos = append(hellos, append([]byte(nil), p.Data...))
			mu.Unlock()
		}
		return false
	}
	// Drop every ack so the dialer keeps retransmitting until it gives up, which
	// is the only way to observe more than one hello.
	dropEveryAck := func(p *PacketData) bool { return p.Kind == packet.PacketKind_HandshakeAck }
	a, _ := pipeDropping(t, capture, dropEveryAck)

	cid := NewConnectionID("udp", netip.MustParseAddrPort("10.0.0.4:5000"), 7)
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	if _, err := DoECDHHandshake(ctx, a, cid, ecdh.X25519(), packet.CommonKeyKind_Aes128Gcm); err == nil {
		t.Fatal("with every ack dropped the dial must fail")
	}
	mu.Lock()
	defer mu.Unlock()
	if len(hellos) < 2 {
		t.Fatalf("saw %d hellos in 1.5s with a 333ms first delay; the dialer is not retransmitting", len(hellos))
	}
	for i := 1; i < len(hellos); i++ {
		if !bytes.Equal(hellos[0], hellos[i]) {
			t.Fatalf("hello %d differs from the first: a rotated key exchange, not a retransmission", i)
		}
	}
}

// Replaying the ack is for the SAME hello. A different one at a live cid is key
// confusion, and would also swap the transcript an application may have bound
// an identity to.
func TestDifferentHelloAtALiveCidIsStillRefused(t *testing.T) {
	ep := testEndpoint(t)
	cid := cidAt(11)
	first := helloOn(t, ecdh.X25519(), packet.CommonKeyKind_Aes128Gcm)
	if err := ep.receiveHandshake(cid, first, []byte("hello-one")); err != nil {
		t.Fatalf("first handshake: %v", err)
	}
	second := helloOn(t, ecdh.X25519(), packet.CommonKeyKind_Aes128Gcm)
	if err := ep.receiveHandshake(cid, second, []byte("hello-two")); err == nil {
		t.Fatal("a different hello took over a live cid")
	}
}

// The replay is time-bounded, so a captured hello does not stay a "make this
// peer emit a packet" primitive for the life of the connection.
func TestAckReplayStopsAfterTheWindow(t *testing.T) {
	ep := testEndpoint(t)
	cid := cidAt(12)
	hs := helloOn(t, ecdh.X25519(), packet.CommonKeyKind_Aes128Gcm)
	hello := []byte("the same datagram")
	if err := ep.receiveHandshake(cid, hs, hello); err != nil {
		t.Fatalf("first handshake: %v", err)
	}
	if err := ep.receiveHandshake(cid, hs, hello); err != nil {
		t.Fatalf("inside the window a repeat must be replayed, got: %v", err)
	}
	// Age the connection past the window.
	ep.endpointLock.RLock()
	active := ep.activeConnections[cid]
	ep.endpointLock.RUnlock()
	active.mu.Lock()
	active.connTime = time.Now().Add(-handshakeReackWindow - time.Second)
	active.mu.Unlock()

	if err := ep.receiveHandshake(cid, hs, hello); err == nil {
		t.Error("the same hello was still replayed after the window closed")
	}
}

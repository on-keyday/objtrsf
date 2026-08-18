package objproto

import (
	"crypto/ecdh"
	"io"
	"log/slog"
	"net/netip"
	"testing"
	"time"

	"github.com/on-keyday/objtrsf/objproto/packet"
)

type testPair struct {
	client, server *activeConnection
	// pump delivers everything queued on both endpoints until both are quiet.
	pump func()
	// toServer and toClient inject one raw datagram from the correct source
	// address, for tests that hand-modify bytes.
	toServer func([]byte) error
	toClient func([]byte) error
}

func newConnectedPair(t *testing.T) *testPair {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cliEP := NewEndpoint(logger, EndpointModeClient).(*endpoint)
	srvEP := NewEndpoint(logger, EndpointModeServer).(*endpoint)

	srvAddr := netip.MustParseAddrPort("127.0.0.1:9001")
	cliAddr := netip.MustParseAddrPort("127.0.0.1:9002")
	srvCID := NewConnectionID("mock", srvAddr, 0x1111)

	// A packet the client queued arrives at the server FROM the client's
	// address, and vice versa. Getting this backwards silently creates two
	// unrelated connection ids and the handshake never completes.
	toServer := func(b []byte) error { return srvEP.Receive("mock", cliAddr, b) }
	toClient := func(b []byte) error { return cliEP.Receive("mock", srvAddr, b) }

	pump := func() {
		for {
			select {
			case pkt := <-cliEP.GetSenderChannel():
				if err := toServer(pkt.Data); err != nil {
					t.Logf("server dropped a packet: %v", err)
				}
			case pkt := <-srvEP.GetSenderChannel():
				if err := toClient(pkt.Data); err != nil {
					t.Logf("client dropped a packet: %v", err)
				}
			default:
				return
			}
		}
	}

	priv, hs, err := NewECDHHandshake(ecdh.X25519(), packet.CommonKeyKind_Aes128Gcm)
	if err != nil {
		t.Fatal(err)
	}
	ch, err := cliEP.SendHandshake(srvCID, priv, hs)
	if err != nil {
		t.Fatal(err)
	}
	pump() // handshake to the server, ack back to the client

	clientConn, err := ch.WaitWithTimeout(t.Context(), time.Second)
	if err != nil {
		t.Fatalf("client handshake did not complete: %v", err)
	}
	serverConn, err := srvEP.WaitNewActiveConnection(time.Second)
	if err != nil {
		t.Fatalf("server handshake did not complete: %v", err)
	}
	return &testPair{
		client:   clientConn.(*activeConnection),
		server:   serverConn.(*activeConnection),
		pump:     pump,
		toServer: toServer,
		toClient: toClient,
	}
}

// captureNextPacket sends a packet and returns its raw bytes WITHOUT
// delivering it, so a test can reorder or corrupt it.
func captureNextPacket(t *testing.T, a *activeConnection, payload []byte) []byte {
	t.Helper()
	if _, _, err := a.endpoint.sendApplication(a.cid, payload, a, nil); err != nil {
		t.Fatal(err)
	}
	select {
	case pkt := <-a.endpoint.GetSenderChannel():
		return append([]byte(nil), pkt.Data...)
	case <-time.After(time.Second):
		t.Fatal("no packet was queued")
		return nil
	}
}

// ratchet advances the send side the way the triggers do, for tests that need
// a phase change at an exact point.
func ratchet(t *testing.T, a *activeConnection) {
	t.Helper()
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := a.ratchetSendLocked(); err != nil {
		t.Fatal(err)
	}
}

func TestHarnessRoundTrip(t *testing.T) {
	p := newConnectedPair(t)
	if _, _, err := p.client.endpoint.sendApplication(p.client.cid, []byte("hi"), p.client, nil); err != nil {
		t.Fatal(err)
	}
	p.pump()
	msg, err := p.server.ReceiveMessageTimeout(t.Context(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if string(msg.Data) != "hi" {
		t.Fatalf("got %q", msg.Data)
	}
}

func TestPeerFollowsPhaseAdvance(t *testing.T) {
	p := newConnectedPair(t)
	if _, _, err := p.client.endpoint.sendApplication(p.client.cid, []byte("phase0"), p.client, nil); err != nil {
		t.Fatal(err)
	}
	p.pump()
	ratchet(t, p.client)
	if _, _, err := p.client.endpoint.sendApplication(p.client.cid, []byte("phase1"), p.client, nil); err != nil {
		t.Fatal(err)
	}
	p.pump()

	for _, want := range []string{"phase0", "phase1"} {
		msg, err := p.server.ReceiveMessageTimeout(t.Context(), time.Second)
		if err != nil {
			t.Fatalf("%s: %v", want, err)
		}
		if string(msg.Data) != want {
			t.Fatalf("got %q want %q", msg.Data, want)
		}
	}
	p.server.mu.Lock()
	defer p.server.mu.Unlock()
	if p.server.recvPhase != 1 {
		t.Fatalf("receiver stayed on phase %d", p.server.recvPhase)
	}
	if p.server.prevPeerSecret == nil {
		t.Fatal("receiver dropped the previous key immediately")
	}
}

func TestPacketNumberStaysMonotonicAcrossUpdate(t *testing.T) {
	p := newConnectedPair(t)
	var last PacketNumber
	for i := 0; i < 4; i++ {
		if i == 2 {
			ratchet(t, p.client)
		}
		_, pn, err := p.client.endpoint.sendApplication(p.client.cid, []byte("x"), p.client, nil)
		if err != nil {
			t.Fatal(err)
		}
		if pn <= last {
			t.Fatalf("packet number went backwards across an update: %d after %d", pn, last)
		}
		last = pn
		p.pump()
	}
}

func TestFlippedPhaseBitIsDroppedWithoutStateChange(t *testing.T) {
	p := newConnectedPair(t)
	raw := captureNextPacket(t, p.client, []byte("hello"))
	raw[0] ^= 0x80 // the phase bit is inside the AAD

	p.server.mu.Lock()
	before := p.server.recvPhase
	p.server.mu.Unlock()

	if err := p.toServer(raw); err == nil {
		t.Fatal("a packet with a flipped phase bit must not decrypt")
	}
	p.server.mu.Lock()
	defer p.server.mu.Unlock()
	if p.server.recvPhase != before {
		t.Fatalf("receive phase advanced on a failed packet: %d -> %d", before, p.server.recvPhase)
	}
	if !p.server.IsActive() {
		t.Fatal("connection was torn down by one bad packet")
	}
}

func TestPreviousPhasePacketDecryptsInsideRetention(t *testing.T) {
	p := newConnectedPair(t)
	// Both phase-0 packets must be captured BEFORE the ratchet, because
	// captureNextPacket always seals under the client's current phase.
	old := captureNextPacket(t, p.client, []byte("late"))
	old2 := captureNextPacket(t, p.client, []byte("too late"))

	ratchet(t, p.client)
	if _, _, err := p.client.endpoint.sendApplication(p.client.cid, []byte("new"), p.client, nil); err != nil {
		t.Fatal(err)
	}
	p.pump()
	if _, err := p.server.ReceiveMessageTimeout(t.Context(), time.Second); err != nil {
		t.Fatal(err)
	}

	// The phase-0 packet that got overtaken must still open.
	if err := p.toServer(old); err != nil {
		t.Fatalf("in-retention previous-phase packet was rejected: %v", err)
	}

	// Expire the previous key, then replay the second phase-0 packet.
	p.server.mu.Lock()
	p.server.prevExpiry = time.Now().Add(-time.Second)
	p.server.mu.Unlock()
	if err := p.toServer(old2); err == nil {
		t.Fatal("previous-phase packet must be dropped after the retention window")
	}
}

func TestPacketCountTriggerAdvancesThePhase(t *testing.T) {
	// The count trigger also respects the anti-double-advance floors, which in
	// production are far below the threshold (1024 packets and 1s, against
	// 2^22 packets that take minutes to send). A test has to lower all three.
	origPackets, origFloor, origTime := keyUpdatePackets, minPacketsBetweenUpdates, minTimeBetweenUpdates
	keyUpdatePackets, minPacketsBetweenUpdates, minTimeBetweenUpdates = 8, 4, 0
	t.Cleanup(func() {
		keyUpdatePackets, minPacketsBetweenUpdates, minTimeBetweenUpdates = origPackets, origFloor, origTime
	})

	p := newConnectedPair(t)
	const total = 24
	for i := 0; i < total; i++ {
		if _, _, err := p.client.endpoint.sendApplication(p.client.cid, []byte("x"), p.client, nil); err != nil {
			t.Fatal(err)
		}
		p.pump()
	}
	// Every packet must arrive. Asserting only on sendPhase would let a packet
	// sealed under the wrong phase be silently dropped by the receiver.
	for i := 0; i < total; i++ {
		if _, err := p.server.ReceiveMessageTimeout(t.Context(), time.Second); err != nil {
			t.Fatalf("packet %d of %d was lost across a key update: %v", i+1, total, err)
		}
	}
	p.client.mu.Lock()
	defer p.client.mu.Unlock()
	if p.client.sendPhase < 2 {
		t.Fatalf("send phase advanced only %d times past the packet threshold", p.client.sendPhase)
	}
}

func TestUpdateKeyRespectsTheFloor(t *testing.T) {
	p := newConnectedPair(t)
	if err := p.client.UpdateKey(); err != nil {
		t.Fatalf("first update rejected: %v", err)
	}
	if err := p.client.UpdateKey(); err == nil {
		t.Fatal("a second update inside the floor must be refused")
	}
}

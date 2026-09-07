package objproto

import (
	"crypto/ecdh"
	"crypto/rand"
	"io"
	"log/slog"
	"net/netip"
	"testing"

	"github.com/on-keyday/objtrsf/objproto/packet"
)

// A responder on the direct data-plane path answers arbitrary addresses, so
// every way of being refused is reachable by a datagram anyone can send for
// free. These tests pin the ORDER: a refusal must not cost a key exchange.
//
// The yardstick is one keygen, measured in the same run rather than hardcoded,
// because it is the thing the reordering removed from the refusal path. If a
// future edit moves NewECDHHandshake back above the cheap checks, the refusal
// paths cross that line and these fail.

func testEndpoint(t *testing.T) *endpoint {
	t.Helper()
	return NewEndpoint(slog.New(slog.NewTextHandler(io.Discard, nil)), EndpointModeServer).(*endpoint)
}

// helloOn builds the ClientHello a peer naming this curve would send. P521 on
// purpose: it is the most expensive curve the responder can be told to use, and
// the responder generates its key on whichever curve the CALLER named.
func helloOn(t *testing.T, curve ecdh.Curve, aead packet.CommonKeyKind) *packet.Handshake {
	t.Helper()
	priv, err := curve.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate peer key: %v", err)
	}
	kind := packet.KeyKind_P521
	switch curve {
	case ecdh.X25519():
		kind = packet.KeyKind_X25519
	case ecdh.P256():
		kind = packet.KeyKind_P256
	case ecdh.P384():
		kind = packet.KeyKind_P384
	}
	hs, err := NewHandshake(priv.PublicKey().Bytes(), kind, aead, 0)
	if err != nil {
		t.Fatalf("build hello: %v", err)
	}
	return hs
}

func cidAt(id uint16) ConnectionID {
	return NewConnectionID("udp", netip.MustParseAddrPort("10.0.0.1:9999"), id)
}

// skippedWorkAllocs is the work a refusal is supposed to skip, measured in the
// same run rather than hardcoded: the keygen, the ECDH and the key schedule, on
// the most expensive curve a hostile hello can name. A refusal that costs this
// much is a refusal that ran it.
// oneKeygenAllocs is the tighter yardstick, for a refusal whose pre-fix cost was
// a keygen and nothing more -- a malformed share used to fail inside
// ECDHFromHandshake, which is after our keygen but before the key schedule, so
// the wider budget below cannot see it.
func oneKeygenAllocs(t *testing.T) float64 {
	t.Helper()
	return testing.AllocsPerRun(20, func() {
		if _, _, err := NewECDHHandshake(ecdh.P521(), packet.CommonKeyKind_Aes128Gcm); err != nil {
			t.Fatalf("keygen: %v", err)
		}
	})
}

func skippedWorkAllocs(t *testing.T) float64 {
	t.Helper()
	hs := helloOn(t, ecdh.P521(), packet.CommonKeyKind_Aes128Gcm)
	cid := cidAt(999)
	return testing.AllocsPerRun(20, func() {
		priv, _, err := NewECDHHandshake(ecdh.P521(), packet.CommonKeyKind_Aes128Gcm)
		if err != nil {
			t.Fatalf("keygen: %v", err)
		}
		shared, _, err := ECDHFromHandshake(priv, hs)
		if err != nil {
			t.Fatalf("ecdh: %v", err)
		}
		if _, err := keySchedule(shared, integrityInfo(cid, hs)); err != nil {
			t.Fatalf("key schedule: %v", err)
		}
	})
}

func TestUnsupportedAeadIsRefusedBeforeAnyKeyExchange(t *testing.T) {
	ep := testEndpoint(t)
	hs := helloOn(t, ecdh.P521(), packet.CommonKeyKind(200)) // not one of 115/67/34/62
	pkt := []byte("datagram")

	if err := ep.receiveHandshake(cidAt(1), hs, pkt); err == nil {
		t.Fatal("an unsupported aead was accepted; addActiveConnection's switch is not a gate, it is a backstop")
	}
	got := testing.AllocsPerRun(20, func() {
		_ = ep.receiveHandshake(cidAt(1), hs, pkt)
	})
	if budget := skippedWorkAllocs(t); got >= budget {
		t.Errorf("refusing an unsupported aead allocated %.0f, at or above the work it should skip (%.0f): the check is running AFTER the key exchange", got, budget)
	}
}

func TestInvalidKeyShareIsRefusedBeforeAnyKeyExchange(t *testing.T) {
	ep := testEndpoint(t)
	hs := helloOn(t, ecdh.P521(), packet.CommonKeyKind_Aes128Gcm)
	hs.KeyShare = []byte{1, 2, 3} // not a point on P521, and the wrong length
	hs.Len = uint16(len(hs.KeyShare))
	pkt := []byte("datagram")

	if err := ep.receiveHandshake(cidAt(2), hs, pkt); err == nil {
		t.Fatal("a malformed key share was accepted")
	}
	got := testing.AllocsPerRun(20, func() {
		_ = ep.receiveHandshake(cidAt(2), hs, pkt)
	})
	if budget := oneKeygenAllocs(t); got >= budget {
		t.Errorf("refusing a malformed key share allocated %.0f, at or above one keygen (%.0f): the share is being parsed AFTER our own keygen", got, budget)
	}
}

// The replay case, and the one that stops being an attack the moment a dialer
// retransmits: a duplicate used to be the LAST thing checked.
func TestDuplicateHandshakeIsRefusedBeforeAnyKeyExchange(t *testing.T) {
	ep := testEndpoint(t)
	cid := cidAt(3)
	hs := helloOn(t, ecdh.P521(), packet.CommonKeyKind_Aes128Gcm)

	if err := ep.receiveHandshake(cid, hs, []byte("first")); err != nil {
		t.Fatalf("the first handshake must succeed: %v", err)
	}
	if err := ep.receiveHandshake(cid, hs, []byte("first")); err == nil {
		t.Fatal("a duplicate handshake at a live cid was accepted; that is key confusion")
	}
	got := testing.AllocsPerRun(20, func() {
		_ = ep.receiveHandshake(cid, hs, []byte("first"))
	})
	if budget := skippedWorkAllocs(t); got >= budget {
		t.Errorf("refusing a duplicate allocated %.0f, at or above the work it should skip (%.0f): the duplicate check is back below the key exchange", got, budget)
	}
}

// The reorder must not have changed what a valid handshake does.
func TestValidHandshakeStillCompletesAndKeepsTheTranscript(t *testing.T) {
	ep := testEndpoint(t)
	cid := cidAt(4)
	hs := helloOn(t, ecdh.X25519(), packet.CommonKeyKind_Chacha20Poly1305)
	original := []byte("the exact datagram")

	if err := ep.receiveHandshake(cid, hs, original); err != nil {
		t.Fatalf("valid handshake refused: %v", err)
	}
	ep.endpointLock.RLock()
	active, ok := ep.activeConnections[cid]
	ep.endpointLock.RUnlock()
	if !ok {
		t.Fatal("no active connection after a valid handshake")
	}
	// The transcript is the received datagram followed by the ack, and the ack
	// half is what a future re-ack would replay. Both ends must agree on these
	// bytes, so the prefix has to survive verbatim.
	tr := active.GetTranscript()
	if len(tr) <= len(original) {
		t.Fatalf("transcript is %d bytes, not longer than the %d-byte hello: the ack half is missing", len(tr), len(original))
	}
	if string(tr[:len(original)]) != string(original) {
		t.Errorf("transcript does not start with the received datagram verbatim")
	}
	// One ack was queued for the peer.
	select {
	case p := <-ep.pktQueue:
		if p.Kind != packet.PacketKind_HandshakeAck {
			t.Errorf("queued packet kind = %v, want HandshakeAck", p.Kind)
		}
		if string(p.Data) != string(tr[len(original):]) {
			t.Errorf("the queued ack is not the ack half of the transcript; a re-ack could not replay it")
		}
	default:
		t.Error("no handshake ack was queued")
	}
}

func TestCommonKeyKindSupportedAcceptsExactlyTheFour(t *testing.T) {
	for _, k := range []packet.CommonKeyKind{
		packet.CommonKeyKind_Aes128Gcm,
		packet.CommonKeyKind_Aes192Gcm,
		packet.CommonKeyKind_Aes256Gcm,
		packet.CommonKeyKind_Chacha20Poly1305,
	} {
		if err := CommonKeyKindSupported(k); err != nil {
			t.Errorf("CommonKeyKindSupported(%v) = %v, want nil", k, err)
		}
	}
	// The values are 115/67/34/62, so neighbours of a valid one are invalid --
	// which is why this cannot be a range test.
	for _, k := range []packet.CommonKeyKind{0, 33, 35, 61, 63, 66, 68, 114, 116, 255} {
		if err := CommonKeyKindSupported(k); err == nil {
			t.Errorf("CommonKeyKindSupported(%d) = nil, want an error", k)
		}
	}
}

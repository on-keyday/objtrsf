package objproto

import (
	"bytes"
	"testing"

	"github.com/on-keyday/objtrsf/objproto/packet"
)

func TestMaskRoundTrip(t *testing.T) {
	kinds := []packet.PacketKind{
		packet.PacketKind_Handshake,
		packet.PacketKind_HandshakeAck,
		packet.PacketKind_Application,
		packet.PacketKind_Probe,
	}
	ids := []uint16{0, 1, 0xEBBC, 0xFFFF}
	for b0 := 0; b0 < 256; b0++ {
		for _, k := range kinds {
			if got := unmaskKind(maskKind(k, byte(b0)), byte(b0)); got != k {
				t.Fatalf("kind b0=%#x: got %v want %v", b0, got, k)
			}
		}
		for _, id := range ids {
			if got := unmaskConnID(maskConnID(id, byte(b0)), byte(b0)); got != id {
				t.Fatalf("connid b0=%#x: got %#x want %#x", b0, got, id)
			}
		}
	}
}

func TestMaskIsNotIdentity(t *testing.T) {
	// A mask that leaves the value untouched for every seed would silently
	// defeat the whole point, so require that some seed changes each field.
	changedKind, changedID := false, false
	for b0 := 0; b0 < 256; b0++ {
		if maskKind(packet.PacketKind_Application, byte(b0)) != uint8(packet.PacketKind_Application) {
			changedKind = true
		}
		if maskConnID(0xEBBC, byte(b0)) != 0xEBBC {
			changedID = true
		}
	}
	if !changedKind || !changedID {
		t.Fatalf("mask never alters a field: kind=%v id=%v", changedKind, changedID)
	}
}

func TestMaskColumnsArePermutations(t *testing.T) {
	for i := 0; i < 3; i++ {
		var seen [256]bool
		for b0 := 0; b0 < 256; b0++ {
			v := maskFor(byte(b0))[i]
			if seen[v] {
				t.Fatalf("column %d is not a permutation: %#x repeats", i, v)
			}
			seen[v] = true
		}
	}
}

func TestNewMaskSeedCarriesPhase(t *testing.T) {
	for phase := uint64(0); phase < 4; phase++ {
		b0 := newMaskSeed(phase, true)
		if want := byte(phase&1) << 7; b0&0x80 != want {
			t.Fatalf("phase %d: bit7 = %#x want %#x", phase, b0&0x80, want)
		}
	}
	// Non-application packets have no phase, so all 8 bits must be free to
	// vary; a constant top bit would be the fixed byte we are removing.
	varied := false
	for i := 0; i < 512; i++ {
		if newMaskSeed(0, false)&0x80 != 0 {
			varied = true
			break
		}
	}
	if !varied {
		t.Fatal("non-application seed never sets bit 7")
	}
}

func TestHeaderWireLayout(t *testing.T) {
	h := buildHeader(packet.PacketKind_Application, 0xEBBC, 0x0102, 1)
	wire, err := h.Append(nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(wire) != 6 {
		t.Fatalf("header is %d bytes, want 6", len(wire))
	}
	b0 := wire[0]
	if b0&0x80 == 0 {
		t.Fatalf("phase 1 must set bit 7, got %#x", b0)
	}
	if wire[1] != maskKind(packet.PacketKind_Application, b0) {
		t.Fatalf("kind byte not masked: %#x", wire[1])
	}
	wantID := maskConnID(0xEBBC, b0)
	if got := uint16(wire[2])<<8 | uint16(wire[3]); got != wantID {
		t.Fatalf("connid not masked: got %#x want %#x", got, wantID)
	}
	if got := uint16(wire[4])<<8 | uint16(wire[5]); got != 0x0102 {
		t.Fatalf("len must stay cleartext, got %#x", got)
	}

	var back packet.PacketHeader
	off := 0
	if err := back.DecodeSlice(wire, &off); err != nil {
		t.Fatal(err)
	}
	if headerKind(&back) != packet.PacketKind_Application {
		t.Fatalf("kind round-trip failed: %v", headerKind(&back))
	}
	if headerConnID(&back) != 0xEBBC {
		t.Fatalf("connid round-trip failed: %#x", headerConnID(&back))
	}
	if headerByte0(&back) != b0 {
		t.Fatalf("byte0 reassembly failed: %#x want %#x", headerByte0(&back), b0)
	}
}

func TestHeaderKindVariesOnTheWire(t *testing.T) {
	// The whole point: the kind byte must not be a constant at a fixed offset.
	seen := map[byte]bool{}
	for i := 0; i < 4096; i++ {
		h := buildHeader(packet.PacketKind_Application, 0xEBBC, 16, 0)
		wire, err := h.Append(nil)
		if err != nil {
			t.Fatal(err)
		}
		seen[wire[1]] = true
	}
	if len(seen) < 32 {
		t.Fatalf("kind byte took only %d distinct values over 4096 packets", len(seen))
	}
}

func TestDecodeDoesNotMutateTheBuffer(t *testing.T) {
	// Transcripts are raw wire bytes and feed the harness PSK binder. If
	// decoding or unmasking ever writes back, the two ends diverge and
	// authentication fails.
	h := buildHeader(packet.PacketKind_Handshake, 0x1234, 4, 0)
	pkt := &packet.Packet{Header: h, Data: []byte{1, 2, 3, 4}}
	wire := pkt.MustAppend(nil)
	before := append([]byte(nil), wire...)

	var got packet.Packet
	if err := got.DecodeExact(wire); err != nil {
		t.Fatal(err)
	}
	_ = headerKind(&got.Header)
	_ = headerConnID(&got.Header)

	if !bytes.Equal(before, wire) {
		t.Fatalf("decode mutated the buffer:\nbefore % x\nafter  % x", before, wire)
	}
	if got.Header.MustAppend(nil)[0] != wire[0] {
		t.Fatal("re-serialising the decoded header does not reproduce the wire bytes")
	}
}

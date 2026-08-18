package objproto

import (
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

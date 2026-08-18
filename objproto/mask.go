package objproto

import (
	"math/rand/v2"

	"github.com/on-keyday/objtrsf/objproto/packet"
)

// The first header byte is key_phase:u1 || mask_seed:u7. It travels in
// cleartext and is the index into maskTable, which XOR-masks `kind` and
// `connection_id` on the wire.
//
// This is obfuscation, not confidentiality: the seed is right there in the
// packet, so anyone who knows the scheme reverses it. What it buys is that no
// header byte holds a constant value at a constant offset. See the Non-goals
// section of docs/superpowers/specs/2026-08-18-objproto-key-phase-design.md in
// the harness repo before claiming anything more for it.
var maskTable [256][3]byte

func init() {
	// Odd multipliers, so each column is a permutation of 0..255.
	c := [3]byte{0x1d, 0x8b, 0x37}
	d := [3]byte{0x5a, 0xa5, 0x3c}
	for b := 0; b < 256; b++ {
		for i := 0; i < 3; i++ {
			maskTable[b][i] = byte(b)*c[i] ^ d[i]
		}
	}
}

func maskFor(b0 byte) [3]byte { return maskTable[b0] }

func maskKind(kind packet.PacketKind, b0 byte) uint8 {
	return uint8(kind) ^ maskTable[b0][0]
}

func unmaskKind(masked uint8, b0 byte) packet.PacketKind {
	return packet.PacketKind(masked ^ maskTable[b0][0])
}

func maskConnID(id uint16, b0 byte) uint16 {
	m := maskTable[b0]
	return id ^ (uint16(m[1])<<8 | uint16(m[2]))
}

func unmaskConnID(masked uint16, b0 byte) uint16 {
	return maskConnID(masked, b0) // XOR is its own inverse
}

// newMaskSeed builds the first header byte. For application packets bit 7 is
// the key phase and the low 7 bits are random. For every other kind there is
// no phase, so all 8 bits are random -- leaving bit 7 at zero would keep a
// constant bit at a constant offset.
func newMaskSeed(phase uint64, application bool) byte {
	b := byte(rand.UintN(256))
	if application {
		b &^= 0x80
		b |= byte(phase&1) << 7
	}
	return b
}

func headerByte0(h *packet.PacketHeader) byte {
	var b byte
	if h.KeyPhase() {
		b = 0x80
	}
	return b | h.MaskSeed()
}

func headerKind(h *packet.PacketHeader) packet.PacketKind {
	return unmaskKind(h.MaskedKind, headerByte0(h))
}

func headerConnID(h *packet.PacketHeader) uint16 {
	return unmaskConnID(h.MaskedConnectionId, headerByte0(h))
}

// buildHeader produces a header already in wire (masked) form. phase is
// ignored for non-application kinds, which carry no key phase.
func buildHeader(kind packet.PacketKind, connID uint16, length uint16, phase uint64) packet.PacketHeader {
	b0 := newMaskSeed(phase, kind == packet.PacketKind_Application)
	var h packet.PacketHeader
	h.SetKeyPhase(b0&0x80 != 0)
	h.SetMaskSeed(b0 & 0x7f)
	h.MaskedKind = maskKind(kind, b0)
	h.MaskedConnectionId = maskConnID(connID, b0)
	h.Len = length
	return h
}

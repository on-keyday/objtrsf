package packet_test

import (
	"bytes"
	"testing"

	"github.com/on-keyday/objtrsf/objproto/packet"
)

func FuzzPacket(f *testing.F) {
	// The mask helpers live in package objproto, so this test cannot unmask.
	// Seed the corpus with the wire-form field instead.
	validPacket := &packet.Packet{
		Header: packet.PacketHeader{
			MaskedKind: uint8(packet.PacketKind_Handshake),
			Len:        30,
		},
		Data: bytes.Repeat([]byte{0x32}, 30),
	}
	corpus := validPacket.MustAppend(nil)
	f.Add(corpus)
	invalidPacket := &packet.PacketHeader{
		MaskedKind: uint8(packet.PacketKind_Application),
		Len:        0xffff,
	}
	invalidCorpus := invalidPacket.MustAppend(nil)
	f.Add(invalidCorpus)
	f.Fuzz(func(t *testing.T, data []byte) {
		pkt := &packet.Packet{}
		if err := pkt.DecodeExact(data); err != nil {
			return
		}
		// Decode unconditionally: the kind byte is masked here, and this keeps
		// the same crash surface as the old kind-gated call.
		hs := &packet.Handshake{}
		hs.DecodeExact(pkt.Data)
	})
}

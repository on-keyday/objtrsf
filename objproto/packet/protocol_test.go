package packet_test

import (
	"bytes"
	"testing"

	"github.com/on-keyday/objtrsf/objproto/packet"
)

func FuzzPacket(f *testing.F) {
	validPacket := &packet.Packet{
		Header: packet.PacketHeader{
			Kind: packet.PacketKind_Handshake,
			Len:  30,
		},
		Data: bytes.Repeat([]byte{0x32}, 30),
	}
	corpus := validPacket.MustAppend(nil)
	f.Add(corpus)
	invalidPacket := &packet.PacketHeader{
		Kind: packet.PacketKind_Application,
		Len:  0xffff,
	}
	invalidCorpus := invalidPacket.MustAppend(nil)
	f.Add(invalidCorpus)
	f.Fuzz(func(t *testing.T, data []byte) {
		pkt := &packet.Packet{}
		err := pkt.DecodeExact(data)
		if err != nil {
			return
		}
		if pkt.Header.Kind == packet.PacketKind_Handshake {
			hs := &packet.Handshake{}
			hs.DecodeExact(pkt.Data)
		}
	})
}

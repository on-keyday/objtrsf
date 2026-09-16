package wire

import (
	"bytes"
	"testing"
)

func TestDatagramPacketRoundTrip(t *testing.T) {
	payload := []byte("the quick brown fox")

	var out StreamAppPacket
	out.Header.Kind = ApplicationPayloadKind_Datagram
	if !out.SetDatagram(DatagramPacket{Data: payload}) {
		t.Fatal("SetDatagram refused the payload although the header names the datagram kind")
	}

	encoded, err := out.EncodeCopy(nil)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	// One kind byte, then the payload verbatim: there is no length field
	// because objproto never splits an application message, so rest-of-packet
	// is exact.
	if len(encoded) != 1+len(payload) {
		t.Fatalf("encoded %d bytes for a %d-byte payload, want %d", len(encoded), len(payload), 1+len(payload))
	}

	var in StreamAppPacket
	if err := in.DecodeExact(encoded); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if in.Header.Kind != ApplicationPayloadKind_Datagram {
		t.Fatalf("decoded kind = %v, want datagram", in.Header.Kind)
	}
	got := in.Datagram()
	if got == nil {
		t.Fatal("decoded packet has no datagram arm")
	}
	if !bytes.Equal(got.Data, payload) {
		t.Fatalf("payload = %q, want %q", got.Data, payload)
	}
}

// An empty datagram is a legal payload, not a decode error: the consumer's own
// framing decides what a zero-length body means.
func TestEmptyDatagramRoundTrips(t *testing.T) {
	var out StreamAppPacket
	out.Header.Kind = ApplicationPayloadKind_Datagram
	if !out.SetDatagram(DatagramPacket{}) {
		t.Fatal("SetDatagram refused an empty payload")
	}
	encoded, err := out.EncodeCopy(nil)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	var in StreamAppPacket
	if err := in.DecodeExact(encoded); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got := in.Datagram(); got == nil || len(got.Data) != 0 {
		t.Fatalf("decoded datagram = %v, want a present but empty payload", got)
	}
}

// The predicate's membership and the union's arms are the same set written
// twice in stream.bgn, and is_defined cannot derive one from the other. A kind
// the union decodes but the predicate rejects never reaches the run loop and
// disappears with no error anywhere, which is the direction that fails quietly.
func TestDatagramIsRoutedToTheRunLoop(t *testing.T) {
	if !IsStreamRelated(ApplicationPayloadKind_Datagram) {
		t.Fatal("StreamAppPacket decodes the datagram kind but IsStreamRelated rejects it: AutoReceive would hand it to onEvent and it would vanish")
	}
}

// The kinds AutoReceive handles itself must stay OUT of the predicate. Routing
// them into the run loop would put them through StreamAppPacket.DecodeExact and
// straight into its "Unexpected packet" arm.
func TestConnectionKindsAreNotRoutedToTheRunLoop(t *testing.T) {
	for _, k := range []ApplicationPayloadKind{
		ApplicationPayloadKind_Ping,
		ApplicationPayloadKind_Pong,
		ApplicationPayloadKind_Close,
	} {
		if IsStreamRelated(k) {
			t.Errorf("%v is handled inside AutoReceive but the predicate routes it to the run loop, where it can only fail to decode", k)
		}
	}
}

package wire

import "testing"

// A datagram has no kind of its own here, and these two tests are what keeps it
// that way.
//
// The kind byte in the header position belongs to whoever owns its range:
// 0x00..0x3F is the transport's, USER_DEFINED_START and above is the consumer's.
// A datagram's byte is the CONSUMER's, and AutoReceive asks the consumer's own
// predicate whether to route it. Giving the transport a datagram kind as well
// would put two kind bytes on the wire to answer one question the range already
// answers.

// No consumer kind may be stream-related. If one were, the run loop would put
// it through StreamAppPacket.DecodeExact, whose union has no arm for it, and it
// would land in the "Unexpected packet" error — while the consumer's predicate,
// the thing that is actually supposed to decide, was never asked.
func TestNoConsumerKindIsStreamRelated(t *testing.T) {
	for k := int(USER_DEFINED_START); k <= 0xFF; k++ {
		if IsStreamRelated(ApplicationPayloadKind(k)) {
			t.Fatalf("kind 0x%02X is in the consumer range but IsStreamRelated claims it", k)
		}
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

// Every kind the predicate claims must have an arm in the union. The two lists
// are the same set written twice in stream.bgn and is_defined cannot derive one
// from the other; a kind in the predicate with no arm reaches DecodeExact and
// errors, which at least says so, while the reverse vanishes silently.
func TestEveryStreamRelatedKindDecodes(t *testing.T) {
	for k := 0; k < int(USER_DEFINED_START); k++ {
		kind := ApplicationPayloadKind(k)
		if !IsStreamRelated(kind) {
			continue
		}
		var pkt StreamAppPacket
		pkt.Header.Kind = kind
		if _, err := pkt.EncodeCopy(nil); err != nil {
			t.Errorf("%v is stream-related but StreamAppPacket cannot carry it: %v", kind, err)
		}
	}
}

package trsf

import (
	"time"

	"github.com/on-keyday/objtrsf/trsf/wire"
)

// ping/pong/close carry an OPTIONAL rest-of-packet body after the kind byte.
// A 0-length body is valid (bare keepalive / teardown). These helpers build and
// parse the bodied variants; they live in the shared transport layer so every
// consumer encodes/decodes them identically. RTT computation (time.Since) is an
// application concern and stays out of the transport core.

// EncodePing builds a ping packet carrying an RTT timestamp body. A bare ping
// ([]byte{<ping kind>}) is equally valid; this is the timestamped variant the
// responder echoes verbatim so the initiator can measure RTT.
func EncodePing(t time.Time) []byte {
	return (&wire.PingBody{Nanos: uint64(t.UnixNano())}).MustAppend([]byte{byte(wire.ApplicationPayloadKind_Ping)})
}

// EncodePong echoes the body received on a ping (the bytes after the ping kind
// byte) back under the pong kind, so the ping initiator can compute RTT. An
// empty pingBody yields a bare pong.
func EncodePong(pingBody []byte) []byte {
	return append([]byte{byte(wire.ApplicationPayloadKind_Pong)}, pingBody...)
}

// EncodeClose builds a close packet with an optional diagnostic body.
func EncodeClose(status wire.CloseStatus, message []byte) []byte {
	cb := &wire.CloseBody{Status: status}
	cb.SetMessage(message)
	return cb.MustAppend([]byte{byte(wire.ApplicationPayloadKind_Close)})
}

// DecodePingPong extracts the RTT timestamp from a ping/pong packet body. ok is
// false for a bare (bodyless) ping/pong. data is the full packet including the
// leading kind byte.
func DecodePingPong(data []byte) (t time.Time, ok bool) {
	if len(data) < 1 {
		return time.Time{}, false
	}
	var pb wire.PingBody
	if err := pb.DecodeExact(data[1:]); err != nil {
		return time.Time{}, false
	}
	return time.Unix(0, int64(pb.Nanos)), true
}

// DecodeClose extracts the status and message from a close packet body. ok is
// false for a bare (bodyless) close. data is the full packet including the kind
// byte; CloseStatus is logging-only (peers must not branch logic on it).
func DecodeClose(data []byte) (status wire.CloseStatus, message string, ok bool) {
	if len(data) < 1 {
		return 0, "", false
	}
	var cb wire.CloseBody
	if err := cb.DecodeExact(data[1:]); err != nil {
		return 0, "", false
	}
	return cb.Status, string(cb.Message), true
}

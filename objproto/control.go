package objproto

import (
	"fmt"

	"github.com/on-keyday/objtrsf/objproto/packet"
)

// sendControl emits an objproto-internal frame. It is an ordinary application
// packet with the protected header's control bit set, so it is encrypted,
// carries the current key phase, and consumes a packet number like any other
// packet. It is never surfaced to the application.
//
// Its one caller today is the time-based key update: an idle connection has no
// data packet to carry a new phase bit.
func (s *endpoint) sendControl(cid ConnectionID, a *activeConnection, kind packet.ControlKind) error {
	_, _, err := s.sendApplicationFrame(cid, []byte{byte(kind)}, a, nil, true)
	return err
}

func (s *endpoint) handleControl(a *activeConnection, plaintext []byte) error {
	if len(plaintext) < 1 {
		return fmt.Errorf("empty control frame")
	}
	switch packet.ControlKind(plaintext[0]) {
	case packet.ControlKind_Ping:
		// The packet itself was the payload: receiving it already moved the
		// peer's phase in receiveApplication. Nothing further to do.
		return nil
	default:
		// Unknown control kinds are ignored rather than fatal, so a future
		// kind does not tear down a connection with an older peer.
		s.logger.Debug("ignoring unknown control frame", "cid", a.cid.String(), "kind", plaintext[0])
		return nil
	}
}

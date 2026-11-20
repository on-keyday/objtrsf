package objproto

import (
	"encoding/hex"
	"fmt"
	"net/netip"
)

// A named address stands for a dial target that is not an IP address: a
// WebSocket URL whose host name and path a name-routed front (a CDN edge, a
// virtual host) routes by. Many such targets share one IP:port, so the IP
// cannot key them apart.
//
// Like a vsock address it rides in the low 64 bits of 100::/64 with port 0,
// which keeps ConnectionID's shape and every wire format that carries one.
// The 64 bits are a key into the table of the WebSocket leg that registered
// the target (transport.WebSocketTargets); the address means nothing to any
// other process, so a named ConnectionID must never be handed elsewhere to be
// dialed. vsock and named addresses share the prefix and are told apart by
// the transport: a named address is only ever ws or wss.

// NamedAddrPort returns the address a named target with key is keyed by.
func NamedAddrPort(key [8]byte) netip.AddrPort {
	var b [16]byte
	copy(b[:8], vsockPrefix[:])
	copy(b[8:], key[:])
	return netip.AddrPortFrom(netip.AddrFrom16(b), 0)
}

// NamedFromAddrPort is the inverse of NamedAddrPort. It cannot tell a named
// address from a vsock one; check the transport first.
func NamedFromAddrPort(ap netip.AddrPort) (key [8]byte, ok bool) {
	if !ap.Addr().Is6() || ap.Port() != 0 {
		return key, false
	}
	b := ap.Addr().As16()
	if [8]byte(b[:8]) != vsockPrefix {
		return key, false
	}
	return [8]byte(b[8:]), true
}

// IsNamed reports whether c is a named address: ws or wss, with an address
// NamedAddrPort produced. Such a ConnectionID is a key into the table of the
// leg that registered it, so nothing but that process can dial it; anything
// that takes an address to dial from text should refuse one.
func (c ConnectionID) IsNamed() bool {
	if !isNamedTransport(c.Transport) {
		return false
	}
	_, ok := NamedFromAddrPort(c.Addr)
	return ok
}

// isNamedTransport reports whether transport can carry a named address.
func isNamedTransport(transport string) bool {
	return transport == "ws" || transport == "wss"
}

// parseNamedAddr parses the "@<16 hex>" a named ConnectionID is written with.
func parseNamedAddr(transport, s string) (netip.AddrPort, error) {
	if !isNamedTransport(transport) {
		return netip.AddrPort{}, fmt.Errorf("named address %q: only ws and wss carry one, not %q", s, transport)
	}
	digits := s[1:]
	var key [8]byte
	if len(digits) != 2*len(key) {
		return netip.AddrPort{}, fmt.Errorf("named address %q: want @ and 16 hex digits", s)
	}
	if _, err := hex.Decode(key[:], []byte(digits)); err != nil {
		return netip.AddrPort{}, fmt.Errorf("named address %q: %w", s, err)
	}
	return NamedAddrPort(key), nil
}

package objproto

import (
	"encoding/binary"
	"fmt"
	"net/netip"
	"strconv"
	"strings"
)

// TransportVsock is the ConnectionID transport of an AF_VSOCK peer.
const TransportVsock = "vsock"

// vsockPrefix is the /64 a vsock address is carried under.
//
// A vsock address is (CID u32, port u32), which a netip.AddrPort cannot hold as
// itself, so it rides in the low 64 bits of an IPv6 address. That keeps
// ConnectionID's shape — and every wire format that carries one as
// transport + 16-byte address + u16 port — unchanged. The Transport field is
// what says these bytes are a vsock address; the prefix only keeps them from
// meaning anything if some path ever treats them as IP: 100::/64 is the
// discard-only prefix (RFC 6666), so a stray send goes nowhere rather than to
// a real host. The AddrPort's port is always 0.
var vsockPrefix = [8]byte{0x01, 0x00}

// VsockAddrPort returns the address a vsock peer (cid, port) is keyed by.
func VsockAddrPort(cid, port uint32) netip.AddrPort {
	var b [16]byte
	copy(b[:8], vsockPrefix[:])
	binary.BigEndian.PutUint32(b[8:12], cid)
	binary.BigEndian.PutUint32(b[12:16], port)
	return netip.AddrPortFrom(netip.AddrFrom16(b), 0)
}

// VsockFromAddrPort is the inverse of VsockAddrPort. ok is false for an
// address VsockAddrPort did not produce.
func VsockFromAddrPort(ap netip.AddrPort) (cid, port uint32, ok bool) {
	if !ap.Addr().Is6() || ap.Port() != 0 {
		return 0, 0, false
	}
	b := ap.Addr().As16()
	if [8]byte(b[:8]) != vsockPrefix {
		return 0, 0, false
	}
	return binary.BigEndian.Uint32(b[8:12]), binary.BigEndian.Uint32(b[12:16]), true
}

// parseVsockAddr parses the "<cid>:<port>" a vsock ConnectionID is written with.
func parseVsockAddr(s string) (netip.AddrPort, error) {
	cidStr, portStr, found := strings.Cut(s, ":")
	if !found {
		return netip.AddrPort{}, fmt.Errorf("vsock address %q: want <cid>:<port>", s)
	}
	cid, err := strconv.ParseUint(cidStr, 10, 32)
	if err != nil {
		return netip.AddrPort{}, fmt.Errorf("vsock cid %q: %w", cidStr, err)
	}
	port, err := strconv.ParseUint(portStr, 10, 32)
	if err != nil {
		return netip.AddrPort{}, fmt.Errorf("vsock port %q: %w", portStr, err)
	}
	return VsockAddrPort(uint32(cid), uint32(port)), nil
}

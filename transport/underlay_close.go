package transport

import (
	"log/slog"
	"net/netip"

	"github.com/on-keyday/objtrsf/objproto"
)

// CloseConnectionsAt ends every active objproto connection that rode an underlay
// socket the transport has just found dead.
//
// Without it the endpoint learns of the death only from
// objproto.AutoGarbageCollect, whose connectionTimeout callers typically set to
// a minute. That timeout is not arbitrary and is not the thing to shorten:
// a connection's lastTime advances on RECEIVE only, so a peer that has stopped
// talking is indistinguishable from one that is merely quiet until several
// keepalive intervals have gone by — a minute is four of trsf.AutoPing's
// default fifteen seconds.
//
// A closed socket is strictly better information than that timeout: it is
// certain, and it arrives in milliseconds. Measured against a harness server
// before this existed: a SIGKILLed client's WebSocket closed within 3s and the
// endpoint declared the connection dead 66s later — and everything keyed to the
// connection's lifetime waited that long with it.
//
// Deliberately an ADDITION rather than a replacement. A UDP underlay has no
// close signal at all, so the sweep stays the general mechanism; this is for
// underlays that can tell.
//
// Every connection on the address is closed, not one. A WebSocket server gives
// each accepted socket its own remote addr:port, so the match is one to one in
// practice, and anything else sharing the address rode the same dead socket.
//
// The address is unmapped before comparing, because the endpoint stores
// connection ids that way (objproto.unmapAddrPort) while a transport carries
// whatever its listener reported — an IPv4-mapped IPv6 form on some stacks,
// which would match nothing.
func CloseConnectionsAt(ep objproto.Endpoint, addr netip.AddrPort, logger *slog.Logger) {
	want := netip.AddrPortFrom(addr.Addr().Unmap(), addr.Port())
	for _, conn := range ep.ListActiveConnections() {
		cid := conn.ConnectionID()
		if netip.AddrPortFrom(cid.Addr.Addr().Unmap(), cid.Addr.Port()) != want {
			continue
		}
		if logger != nil {
			logger.Info("closing connection: its underlay is gone", "cid", cid.String())
		}
		_ = conn.Close()
	}
}

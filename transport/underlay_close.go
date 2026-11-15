package transport

import (
	"log/slog"
	"net/netip"
	"sync"

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

// CloseUnderlayAt drops the socket ep's transports hold to addr and closes the
// connections that rode it. It reports whether any socket was found.
//
// This is for a caller that has learned a link is dead from something the
// transport cannot see — an application-level probe that went unanswered. A
// browser after sleep keeps a half-open WebSocket in readyState OPEN with no
// close event for minutes; closing the objproto connection alone does not help
// there, because the next handshake to the same address rides the socket still
// in the transport's map, i.e. the dead one. After this call the next handshake
// dials a fresh socket.
//
// A new handshake does NOT replace an existing socket on its own: a Mutual-mode
// endpoint accepts a peer's handshake at a new connection id over the socket it
// already has, so "dead" has to be said by the caller.
//
// Only transports that hold a socket per peer register here (WebSocket). For
// any other endpoint, and for an address with no socket, this returns false and
// does nothing; UDP has no per-peer socket to drop.
func CloseUnderlayAt(ep objproto.Endpoint, addr netip.AddrPort, logger *slog.Logger) bool {
	want := netip.AddrPortFrom(addr.Addr().Unmap(), addr.Port())
	underlayClosersMu.Lock()
	closers := append([]func(netip.AddrPort) bool(nil), underlayClosers[ep]...)
	underlayClosersMu.Unlock()
	found := false
	for _, f := range closers {
		if f(want) {
			found = true
		}
	}
	if found {
		if logger != nil {
			logger.Info("dropped the underlay socket on request", "addr", want.String())
		}
		CloseConnectionsAt(ep, want, logger)
	}
	return found
}

// underlayClosers holds, per endpoint, each socket transport's function that
// drops its socket to an (unmapped) address and reports whether it had one.
// Endpoints are never closed, so neither are their entries.
var (
	underlayClosersMu sync.Mutex
	underlayClosers   = map[objproto.Endpoint][]func(netip.AddrPort) bool{}
)

func registerUnderlayCloser(ep objproto.Endpoint, f func(netip.AddrPort) bool) {
	underlayClosersMu.Lock()
	defer underlayClosersMu.Unlock()
	underlayClosers[ep] = append(underlayClosers[ep], f)
}

func unmapped(a netip.AddrPort) netip.AddrPort {
	return netip.AddrPortFrom(a.Addr().Unmap(), a.Port())
}

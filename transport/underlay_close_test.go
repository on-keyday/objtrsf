package transport

import (
	"crypto/ecdh"
	"io"
	"log/slog"
	"net/netip"
	"testing"
	"time"

	"github.com/on-keyday/objtrsf/objproto"
	"github.com/on-keyday/objtrsf/objproto/packet"
)

// connectTo brings up one client endpoint against srvEP from the given address
// and returns the server-side connection, using only the public API — the
// in-package helper this mirrors (objproto's newConnectedPair) reaches
// unexported types.
func connectTo(t *testing.T, srvEP objproto.RawEndpoint, cliAddr netip.AddrPort, id uint16) objproto.Connection {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	cliEP := objproto.NewEndpoint(logger, objproto.EndpointModeClient)

	srvAddr := netip.MustParseAddrPort("127.0.0.1:9001")
	srvCID := objproto.NewConnectionID("mock", srvAddr, id)

	// A packet the client queued arrives at the server FROM the client's
	// address, and vice versa. Backwards, the handshake silently never
	// completes because two unrelated connection ids get created.
	pump := func() {
		for {
			select {
			case pkt := <-cliEP.GetSenderChannel():
				_ = srvEP.Receive("mock", cliAddr, pkt.Data)
			case pkt := <-srvEP.GetSenderChannel():
				_ = cliEP.Receive("mock", srvAddr, pkt.Data)
			default:
				return
			}
		}
	}

	priv, hs, err := objproto.NewECDHHandshake(ecdh.X25519(), packet.CommonKeyKind_Aes128Gcm)
	if err != nil {
		t.Fatal(err)
	}
	ch, err := cliEP.SendHandshake(srvCID, priv, hs)
	if err != nil {
		t.Fatal(err)
	}
	pump()
	if _, err := ch.WaitWithTimeout(t.Context(), time.Second); err != nil {
		t.Fatalf("handshake did not complete: %v", err)
	}

	for _, c := range srvEP.ListActiveConnections() {
		if c.ConnectionID().Addr == cliAddr {
			return c
		}
	}
	t.Fatalf("no server-side connection for %v after the handshake", cliAddr)
	return nil
}

// A dead underlay closes the connections that rode it — immediately, rather
// than at AutoGarbageCollect's connectionTimeout — and closes ONLY those.
//
// The negative half is the one worth having: a helper that closed everything
// would pass an "it closed" assertion and take out every other peer with it.
func TestCloseConnectionsAtClosesOnlyThatAddress(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	srvEP := objproto.NewEndpoint(logger, objproto.EndpointModeServer)

	doomedAddr := netip.MustParseAddrPort("127.0.0.1:9002")
	otherAddr := netip.MustParseAddrPort("127.0.0.1:9003")
	doomed := connectTo(t, srvEP, doomedAddr, 0x1111)
	other := connectTo(t, srvEP, otherAddr, 0x2222)

	CloseConnectionsAt(srvEP, doomedAddr, nil)

	select {
	case <-doomed.Done():
	default:
		t.Error("the connection on the dead underlay is still open")
	}
	if doomed.IsActive() {
		t.Error("the connection on the dead underlay still reports active")
	}
	select {
	case <-other.Done():
		t.Fatal("a connection on a DIFFERENT address was closed too")
	default:
	}
	if !other.IsActive() {
		t.Error("a connection on a different address stopped being active")
	}

	for _, c := range srvEP.ListActiveConnections() {
		if c.ConnectionID().Addr == doomedAddr {
			t.Error("the closed connection is still listed as active")
		}
	}
}

// The endpoint stores connection ids with the address UNMAPPED, while a
// transport reports whatever its listener gave it — an IPv4-mapped IPv6 form on
// some stacks. Comparing the two raw matches nothing, and the failure is silent:
// the connection is simply never closed and the timeout is back.
func TestCloseConnectionsAtUnmapsTheAddress(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	srvEP := objproto.NewEndpoint(logger, objproto.EndpointModeServer)

	stored := netip.MustParseAddrPort("127.0.0.1:9002")
	conn := connectTo(t, srvEP, stored, 0x3333)

	mapped := netip.MustParseAddrPort("[::ffff:127.0.0.1]:9002")
	if mapped == stored {
		t.Fatal("the mapped and unmapped forms compared equal — this test proves nothing")
	}
	CloseConnectionsAt(srvEP, mapped, nil)

	select {
	case <-conn.Done():
	default:
		t.Error("an IPv4-mapped address did not match the connection stored unmapped")
	}
}

// Nothing at that address is not an error: two receive loops can notice the
// same socket die, and a transport may outlive the connections it carried.
func TestCloseConnectionsAtUnknownAddressIsANoOp(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	srvEP := objproto.NewEndpoint(logger, objproto.EndpointModeServer)
	live := connectTo(t, srvEP, netip.MustParseAddrPort("127.0.0.1:9002"), 0x4444)

	CloseConnectionsAt(srvEP, netip.MustParseAddrPort("127.0.0.1:9999"), nil)
	CloseConnectionsAt(srvEP, netip.MustParseAddrPort("127.0.0.1:9002"), nil)
	CloseConnectionsAt(srvEP, netip.MustParseAddrPort("127.0.0.1:9002"), nil)

	if live.IsActive() {
		t.Error("the second call should have been a no-op, not a resurrection")
	}
}

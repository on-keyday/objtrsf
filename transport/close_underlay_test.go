//go:build !js

package transport

import (
	"net/netip"
	"testing"
	"time"

	"github.com/on-keyday/objtrsf/objproto"
)

// The case CloseUnderlayAt exists for: the caller has decided the link to a
// peer is dead while the socket still looks open (a browser after sleep keeps
// a half-open WebSocket in readyState OPEN). Closing the objproto connection is
// not enough — the next handshake to the same address rides the socket already
// in the map, i.e. the dead one, and never completes. After CloseUnderlayAt the
// next handshake must dial a NEW socket, and the connections on the old one
// must be closed with it.
func TestCloseUnderlayAtMakesTheNextHandshakeDialAFreshSocket(t *testing.T) {
	_, srvAddr, upgrades := wsServer(t, false)
	cli := wsClient(t, nil)
	cid := objproto.NewConnectionID("ws", srvAddr, 0x5101)

	first, err := handshake(t, cli, cid, 2*time.Second)
	if err != nil {
		t.Fatalf("first handshake: %v", err)
	}
	if n := upgrades.Load(); n != 1 {
		t.Fatalf("upgrades after the first handshake = %d, want 1", n)
	}

	if !CloseUnderlayAt(cli, srvAddr, nil) {
		t.Fatal("CloseUnderlayAt reported no socket to an address the client had dialed")
	}
	select {
	case <-first.Done():
	case <-time.After(time.Second):
		t.Fatal("the connection that rode the dropped socket is still open")
	}

	second, err := handshake(t, cli, objproto.NewConnectionID("ws", srvAddr, 0x5102), 2*time.Second)
	if err != nil {
		t.Fatalf("handshake after CloseUnderlayAt: %v", err)
	}
	if n := upgrades.Load(); n != 2 {
		t.Errorf("upgrades after CloseUnderlayAt + handshake = %d, want 2 (a fresh socket)", n)
	}
	// The old socket's receive loop ends after the new socket is in the map.
	// It must not take the new socket's entry or its connection down with it.
	time.Sleep(200 * time.Millisecond)
	if !second.IsActive() {
		t.Error("the connection on the fresh socket was closed by the old socket's teardown")
	}
}

// Without CloseUnderlayAt the socket is reused — the behaviour a Mutual-mode
// runner depends on (a server handshake at a new connection id over the
// existing socket), and the reason a new handshake alone must not replace it.
func TestWithoutCloseUnderlayAtTheSocketIsReused(t *testing.T) {
	_, srvAddr, upgrades := wsServer(t, false)
	cli := wsClient(t, nil)

	first, err := handshake(t, cli, objproto.NewConnectionID("ws", srvAddr, 0x5201), 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_ = first.Close()
	if _, err := handshake(t, cli, objproto.NewConnectionID("ws", srvAddr, 0x5202), 2*time.Second); err != nil {
		t.Fatal(err)
	}
	if n := upgrades.Load(); n != 1 {
		t.Errorf("upgrades = %d, want 1 (the socket is reused)", n)
	}
}

// An address with no socket, and an endpoint no socket transport serves,
// report false rather than failing: the caller cannot know which underlay an
// endpoint uses, and UDP has no socket per peer to drop.
func TestCloseUnderlayAtReportsFalseWhenThereIsNothingToClose(t *testing.T) {
	_, srvAddr, _ := wsServer(t, false)
	cli := wsClient(t, nil)
	if _, err := handshake(t, cli, objproto.NewConnectionID("ws", srvAddr, 0x5301), 2*time.Second); err != nil {
		t.Fatal(err)
	}
	if CloseUnderlayAt(cli, netip.MustParseAddrPort("127.0.0.1:1"), nil) {
		t.Error("reported a socket to an address never dialed")
	}
	bare := objproto.NewEndpoint(quietLogger(), objproto.EndpointModeClient)
	if CloseUnderlayAt(bare, srvAddr, nil) {
		t.Error("reported a socket on an endpoint no socket transport serves")
	}
}

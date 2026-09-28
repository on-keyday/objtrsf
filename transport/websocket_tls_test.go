package transport

import (
	"crypto/ecdh"
	"crypto/tls"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"sync/atomic"
	"testing"
	"time"

	"github.com/on-keyday/objtrsf/objproto"
	"github.com/on-keyday/objtrsf/objproto/packet"
)

const wsTestPath = "/ws"

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// wsServer starts a Server-mode WS endpoint behind httptest, over TLS when
// tlsOn is set. upgrades counts requests that reached the mux, so a test can
// prove a dial never happened.
func wsServer(t *testing.T, tlsOn bool) (objproto.Endpoint, netip.AddrPort, *atomic.Int32) {
	t.Helper()
	mux := http.NewServeMux()
	ep, err := WebSocketEndpoint(mux, WebSocketConfig{Logger: quietLogger(), Path: wsTestPath, Mode: objproto.EndpointModeServer})
	if err != nil {
		t.Fatal(err)
	}
	var upgrades atomic.Int32
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrades.Add(1)
		mux.ServeHTTP(w, r)
	})
	var ts *httptest.Server
	if tlsOn {
		ts = httptest.NewTLSServer(h)
	} else {
		ts = httptest.NewServer(h)
	}
	t.Cleanup(ts.Close)
	return ep, netip.MustParseAddrPort(ts.Listener.Addr().String()), &upgrades
}

func wsClient(t *testing.T, tlsConf *tls.Config) objproto.Endpoint {
	t.Helper()
	ep, err := WebSocketEndpoint(nil, WebSocketConfig{Logger: quietLogger(), Path: wsTestPath, Mode: objproto.EndpointModeClient, TLS: tlsConf})
	if err != nil {
		t.Fatal(err)
	}
	return ep
}

func handshake(t *testing.T, cli objproto.Endpoint, cid objproto.ConnectionID, wait time.Duration) (objproto.Connection, error) {
	t.Helper()
	priv, hs, err := objproto.NewECDHHandshake(ecdh.X25519(), packet.CommonKeyKind_Aes128Gcm)
	if err != nil {
		t.Fatal(err)
	}
	ch, err := cli.SendHandshake(cid, priv, hs)
	if err != nil {
		t.Fatal(err)
	}
	return ch.WaitWithTimeout(t.Context(), wait)
}

func serverSideTransports(ep objproto.Endpoint) []string {
	var out []string
	for _, c := range ep.(objproto.RawEndpoint).ListActiveConnections() {
		out = append(out, c.ConnectionID().Transport)
	}
	return out
}

func skipVerify() *tls.Config { return &tls.Config{InsecureSkipVerify: true} }

// A wss round trip, with both ends seeing a wss: CID. Before the change the
// server labelled every accepted connection "ws" because its own cfg.TLS was
// nil, so its CID disagreed with the client's.
func TestWSSRoundTripLabelsBothEnds(t *testing.T) {
	srv, addr, _ := wsServer(t, true)
	cli := wsClient(t, skipVerify())

	conn, err := handshake(t, cli, objproto.NewConnectionID("wss", addr, 0x1001), 3*time.Second)
	if err != nil {
		t.Fatalf("wss handshake did not complete: %v", err)
	}
	if got := conn.ConnectionID().Transport; got != "wss" {
		t.Errorf("client-side transport = %q, want wss", got)
	}
	if got := serverSideTransports(srv); len(got) != 1 || got[0] != "wss" {
		t.Errorf("server-side transports = %v, want [wss]", got)
	}
}

// One endpoint dials a ws peer and a wss peer. Before the change the scheme
// was fixed per endpoint, so one of the two was dialed with the wrong scheme.
func TestOneEndpointDialsWSAndWSS(t *testing.T) {
	plainSrv, plainAddr, _ := wsServer(t, false)
	tlsSrv, tlsAddr, _ := wsServer(t, true)
	cli := wsClient(t, skipVerify())

	if _, err := handshake(t, cli, objproto.NewConnectionID("ws", plainAddr, 0x2001), 3*time.Second); err != nil {
		t.Fatalf("ws handshake: %v", err)
	}
	if _, err := handshake(t, cli, objproto.NewConnectionID("wss", tlsAddr, 0x2002), 3*time.Second); err != nil {
		t.Fatalf("wss handshake: %v", err)
	}
	if got := serverSideTransports(plainSrv); len(got) != 1 || got[0] != "ws" {
		t.Errorf("plain server transports = %v, want [ws]", got)
	}
	if got := serverSideTransports(tlsSrv); len(got) != 1 || got[0] != "wss" {
		t.Errorf("tls server transports = %v, want [wss]", got)
	}
}

// A wss: dial with no TLS config must not reach the network in plaintext.
// Before the change it dialed ws:// silently (and then failed to match the
// HandshakeAck, which arrived labelled "ws").
func TestWSSWithoutTLSConfigNeverDialsPlaintext(t *testing.T) {
	_, addr, upgrades := wsServer(t, false)
	cli := wsClient(t, nil)

	if _, err := handshake(t, cli, objproto.NewConnectionID("wss", addr, 0x3001), 500*time.Millisecond); err == nil {
		t.Fatal("wss handshake with no TLS config completed")
	}
	if n := upgrades.Load(); n != 0 {
		t.Errorf("server saw %d request(s); a wss dial without TLS must not dial at all", n)
	}
}

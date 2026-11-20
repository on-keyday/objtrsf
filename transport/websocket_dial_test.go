package transport

import (
	"bufio"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/on-keyday/objtrsf/objproto"
)

// serverConns lists a server endpoint's live connections.
func serverConns(ep objproto.Endpoint) []objproto.Connection {
	return ep.(objproto.RawEndpoint).ListActiveConnections()
}

// exchange sends ping from cli, expects srv's only connection to receive it,
// answers pong and expects cli to receive that.
func exchange(t *testing.T, cli objproto.Connection, srv objproto.Endpoint) {
	t.Helper()
	conns := serverConns(srv)
	if len(conns) != 1 {
		t.Fatalf("server holds %d connections, want 1", len(conns))
	}
	if _, _, err := cli.SendMessage([]byte("ping")); err != nil {
		t.Fatalf("client send: %v", err)
	}
	if _, err := conns[0].ReceiveMessageTimeout(t.Context(), time.Second); err != nil {
		t.Fatalf("server did not receive ping: %v", err)
	}
	if _, _, err := conns[0].SendMessage([]byte("pong")); err != nil {
		t.Fatalf("server send: %v", err)
	}
	if _, err := cli.ReceiveMessageTimeout(t.Context(), 2*time.Second); err != nil {
		t.Fatalf("client did not receive pong: %v", err)
	}
}

// heldServer is wsServer with each upgrade request passed through hold first.
func heldServer(t *testing.T, tlsOn bool, hold func(n int32, r *http.Request)) (objproto.Endpoint, netip.AddrPort, *atomic.Int32) {
	t.Helper()
	mux := http.NewServeMux()
	ep, err := WebSocketEndpoint(mux, WebSocketConfig{Logger: quietLogger(), Path: wsTestPath, Mode: objproto.EndpointModeServer})
	if err != nil {
		t.Fatal(err)
	}
	var n atomic.Int32
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hold(n.Add(1), r)
		mux.ServeHTTP(w, r)
	})
	var ts *httptest.Server
	if tlsOn {
		ts = httptest.NewTLSServer(h)
	} else {
		ts = httptest.NewServer(h)
	}
	t.Cleanup(ts.Close)
	return ep, netip.MustParseAddrPort(ts.Listener.Addr().String()), &n
}

// P6: a dial slower than the handshake retransmit used to open one socket per
// retransmit, and the peer answered each as a separate connection.
func TestSlowFrontKeepsOneConnection(t *testing.T) {
	for _, tr := range []string{"ws", "wss"} {
		t.Run(tr, func(t *testing.T) {
			srv, addr, _ := heldServer(t, tr == "wss", func(int32, *http.Request) { time.Sleep(1500 * time.Millisecond) })
			cli := wsClient(t, skipVerify())
			conn, err := handshake(t, cli, objproto.NewConnectionID(tr, addr, 0x6001), 8*time.Second)
			if err != nil {
				t.Fatalf("handshake: %v", err)
			}
			time.Sleep(2500 * time.Millisecond) // every retransmit's dial has landed
			exchange(t, conn, srv)
		})
	}
}

// The behaviour first-socket-wins must keep: a retransmit's second dial
// rescues a first upgrade that never answers.
func TestHungFirstUpgradeStillConnects(t *testing.T) {
	release := make(chan struct{})
	srv, addr, _ := heldServer(t, false, func(n int32, r *http.Request) {
		if n == 1 {
			select {
			case <-release:
			case <-r.Context().Done():
			}
		}
	})
	t.Cleanup(func() { close(release) })
	cli := wsClient(t, skipVerify())
	conn, err := handshake(t, cli, objproto.NewConnectionID("ws", addr, 0x7001), time.Second)
	if err != nil {
		t.Fatalf("handshake: %v", err)
	}
	exchange(t, conn, srv)
}

// seen is what a front records about each upgrade request.
type seen struct{ host, sni, path string }

// hijackRecorder remembers the connection the websocket server hijacks, so a
// test can cut one peer's socket.
type hijackRecorder struct {
	http.ResponseWriter
	got chan net.Conn
}

func (h hijackRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	c, rw, err := h.ResponseWriter.(http.Hijacker).Hijack()
	if err == nil {
		h.got <- c
	}
	return c, rw, err
}

// front is one TLS listener routing /a/ws and /b/ws to two separate
// server endpoints, recording what each request carried.
type front struct {
	addr   netip.AddrPort
	a, b   objproto.Endpoint
	aConns chan net.Conn
	mu     sync.Mutex
	reqs   []seen
}

func newFront(t *testing.T) *front {
	t.Helper()
	f := &front{aConns: make(chan net.Conn, 4)}
	muxA, muxB := http.NewServeMux(), http.NewServeMux()
	var err error
	if f.a, err = WebSocketEndpoint(muxA, WebSocketConfig{Logger: quietLogger(), Path: wsTestPath, Mode: objproto.EndpointModeServer}); err != nil {
		t.Fatal(err)
	}
	if f.b, err = WebSocketEndpoint(muxB, WebSocketConfig{Logger: quietLogger(), Path: wsTestPath, Mode: objproto.EndpointModeServer}); err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.reqs = append(f.reqs, seen{r.Host, r.TLS.ServerName, r.URL.Path})
		f.mu.Unlock()
		switch {
		case strings.HasPrefix(r.URL.Path, "/a/"):
			r.URL.Path = strings.TrimPrefix(r.URL.Path, "/a")
			muxA.ServeHTTP(hijackRecorder{w, f.aConns}, r)
		case strings.HasPrefix(r.URL.Path, "/b/"):
			r.URL.Path = strings.TrimPrefix(r.URL.Path, "/b")
			muxB.ServeHTTP(w, r)
		default:
			muxA.ServeHTTP(w, r)
		}
	}))
	t.Cleanup(ts.Close)
	f.addr = netip.MustParseAddrPort(ts.Listener.Addr().String())
	return f
}

func (f *front) requests() []seen {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]seen(nil), f.reqs...)
}

func namedClient(t *testing.T, targets *WebSocketTargets) objproto.Endpoint {
	t.Helper()
	ep, err := WebSocketEndpoint(nil, WebSocketConfig{Logger: quietLogger(), Path: wsTestPath,
		Mode: objproto.EndpointModeClient, TLS: skipVerify(), Targets: targets})
	if err != nil {
		t.Fatal(err)
	}
	return ep
}

func mustTarget(t *testing.T, raw string) WebSocketTarget {
	t.Helper()
	tg, err := ParseWebSocketTarget(raw)
	if err != nil {
		t.Fatal(err)
	}
	return tg
}

func TestNamedTargetsShareAnAddress(t *testing.T) {
	f := newFront(t)
	port := f.addr.Port()
	targets := NewWebSocketTargets()
	ta := mustTarget(t, "wss://localhost:"+itoa(port)+"/a/ws")
	tb := mustTarget(t, "wss://localhost:"+itoa(port)+"/b/ws")
	cidA := objproto.NewConnectionID("wss", targets.Register(ta), 0xa001)
	cidB := objproto.NewConnectionID("wss", targets.Register(tb), 0xb001)
	cli := namedClient(t, targets)

	var wg sync.WaitGroup
	var connA, connB objproto.Connection
	var errA, errB error
	wg.Add(2)
	go func() { defer wg.Done(); connA, errA = handshake(t, cli, cidA, 5*time.Second) }()
	go func() { defer wg.Done(); connB, errB = handshake(t, cli, cidB, 5*time.Second) }()
	wg.Wait()
	if errA != nil || errB != nil {
		t.Fatalf("handshakes: a=%v b=%v", errA, errB)
	}

	reqs := f.requests()
	if len(reqs) != 2 {
		t.Fatalf("front saw %d requests, want 2: %+v", len(reqs), reqs)
	}
	paths := map[string]bool{}
	for _, r := range reqs {
		if r.host != "localhost:"+itoa(port) || r.sni != "localhost" {
			t.Errorf("request %+v: want Host localhost:%d and SNI localhost", r, port)
		}
		paths[r.path] = true
	}
	if !paths["/a/ws"] || !paths["/b/ws"] {
		t.Errorf("paths = %v, want /a/ws and /b/ws", paths)
	}
	exchange(t, connA, f.a)
	exchange(t, connB, f.b)

	// Cut A's socket: only A's connection ends.
	select {
	case c := <-f.aConns:
		c.Close()
	case <-time.After(time.Second):
		t.Fatal("front never handed over A's socket")
	}
	select {
	case <-connA.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("A's connection did not end after its socket was cut")
	}
	if !connB.IsActive() {
		t.Fatal("B's connection ended with A's socket")
	}
	if _, _, err := connB.SendMessage([]byte("still")); err != nil {
		t.Fatalf("B send after A's cut: %v", err)
	}
	if _, err := serverConns(f.b)[0].ReceiveMessageTimeout(t.Context(), time.Second); err != nil {
		t.Fatalf("B did not receive after A's cut: %v", err)
	}
}

// What the same two peers looked like before named targets: both
// ConnectionIDs are one IP:port, so they ride one socket and the front sees
// an IP for Host and no SNI.
func TestNamedTargetsNegativeControl(t *testing.T) {
	f := newFront(t)
	cli := wsClient(t, skipVerify())
	for _, id := range []uint16{1, 2} {
		if _, err := handshake(t, cli, objproto.NewConnectionID("wss", f.addr, id), 3*time.Second); err != nil {
			t.Fatalf("handshake %d: %v", id, err)
		}
	}
	reqs := f.requests()
	if len(reqs) != 1 {
		t.Fatalf("front saw %d requests, want 1 (the second handshake rides the first socket): %+v", len(reqs), reqs)
	}
	if reqs[0].host != f.addr.String() || reqs[0].sni != "" {
		t.Errorf("request %+v: want Host %s and no SNI", reqs[0], f.addr)
	}
}

func TestNamedAddressWithoutEntryCannotSend(t *testing.T) {
	f := newFront(t)
	targets := NewWebSocketTargets()
	plain := mustTarget(t, "ws://localhost:"+itoa(f.addr.Port())+"/a/ws")
	cli := namedClient(t, targets)
	for name, cid := range map[string]objproto.ConnectionID{
		"unregistered":    objproto.NewConnectionID("wss", objproto.NamedAddrPort([8]byte{9}), 1),
		"scheme mismatch": objproto.NewConnectionID("wss", targets.Register(plain), 2),
	} {
		if _, err := handshake(t, cli, cid, 2*time.Second); err == nil {
			t.Errorf("%s: handshake completed, want it to fail", name)
		}
	}
	if reqs := f.requests(); len(reqs) != 0 {
		t.Errorf("front saw %d requests, want none: %+v", len(reqs), reqs)
	}
}

func itoa(p uint16) string { return strconv.Itoa(int(p)) }

package transport

import (
	"crypto/sha256"
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"sync"

	"github.com/on-keyday/objtrsf/objproto"
)

// WebSocketTarget is a dial target a ConnectionID's address cannot hold: the
// URL a named address (objproto.NamedAddrPort) stands for. It exists so that
// several peers behind one name-routed front — a CDN edge, a virtual host, a
// path-routing proxy — are told apart by the host name, SNI and path the dial
// sends, not by an IP:port they all share.
//
// It is always canonical (see ParseWebSocketTarget), and its key is derived
// from the canonical text, so the same target has the same address in every
// process and across restarts.
type WebSocketTarget struct {
	text string
	u    *url.URL
}

// ParseWebSocketTarget validates raw as a ws:// or wss:// dial target and
// returns it in canonical form: scheme and host lower-cased, an empty or
// default port (80 for ws, 443 for wss) dropped, and the path as
// url.URL.EscapedPath — its own escapes kept, required ones added, dot
// segments and a trailing slash left alone, because only the front knows how
// it routes them. The path is required. Every error names the rule broken.
func ParseWebSocketTarget(raw string) (WebSocketTarget, error) {
	fail := func(rule string) (WebSocketTarget, error) {
		return WebSocketTarget{}, fmt.Errorf("ws target %q: %s", raw, rule)
	}
	for i := 0; i < len(raw); i++ {
		if raw[i] < 0x20 || raw[i] == 0x7f {
			return fail("control character")
		}
	}
	// Checked on the raw text: url.Parse reports a bare "#" as no fragment.
	if strings.ContainsRune(raw, '?') {
		return fail("query is not supported")
	}
	if strings.ContainsRune(raw, '#') {
		return fail("fragment is not supported")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return WebSocketTarget{}, fmt.Errorf("ws target %q: %w", raw, err)
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme != "ws" && scheme != "wss" {
		return fail("scheme must be ws or wss")
	}
	if u.User != nil {
		return fail("user info is not supported")
	}
	host := u.Hostname()
	switch {
	case u.Opaque != "" || host == "":
		return fail("host is required (write ws://host/path or wss://host/path)")
	case strings.ContainsRune(host, '%'):
		return fail("an IPv6 zone is not supported")
	case strings.HasSuffix(host, "."):
		return fail("a host with a trailing dot is not supported")
	}
	for i := 0; i < len(host); i++ {
		if host[i] >= 0x80 {
			return fail("a non-ASCII host is not supported (write it in punycode)")
		}
	}
	host = strings.ToLower(host)
	if strings.ContainsRune(host, ':') {
		host = "[" + host + "]"
	}
	if p := u.Port(); p != "" {
		n, err := strconv.Atoi(p)
		if err != nil || n < 1 || n > 65535 {
			return fail("port must be 1-65535")
		}
		if !(scheme == "ws" && n == 80) && !(scheme == "wss" && n == 443) {
			host += ":" + strconv.Itoa(n)
		}
	}
	path := u.EscapedPath()
	if path == "" {
		return fail("path is required (wss://host/ for the root)")
	}
	text := scheme + "://" + host + path
	cu, err := url.Parse(text)
	if err != nil {
		return WebSocketTarget{}, fmt.Errorf("ws target %q: canonical form %q: %w", raw, text, err)
	}
	return WebSocketTarget{text: text, u: cu}, nil
}

// String is the canonical text.
func (t WebSocketTarget) String() string { return t.text }

// Transport is the ConnectionID transport a dial to t uses: "ws" or "wss".
func (t WebSocketTarget) Transport() string { return t.u.Scheme }

// Host is the canonical host[:port], as Location and Origin carry it.
func (t WebSocketTarget) Host() string { return t.u.Host }

// URL returns a copy of the canonical URL.
func (t WebSocketTarget) URL() *url.URL {
	c := *t.u
	return &c
}

// dialLocation is the URL a dial to t uses: the canonical one, except that a
// bracketed IPv6 host on its scheme's default port gets the port written out.
// x/net/websocket adds a missing port by joining Host as given, which turns
// "[::1]" into "[[::1]]:443". The canonical text (and so the key) is unchanged.
func dialLocation(t WebSocketTarget) *url.URL {
	u := t.URL()
	if u.Port() == "" && strings.ContainsRune(u.Hostname(), ':') {
		port := "443"
		if u.Scheme == "ws" {
			port = "80"
		}
		u.Host = net.JoinHostPort(u.Hostname(), port)
	}
	return u
}

// Key is the first 8 bytes of SHA-256 over the canonical text.
func (t WebSocketTarget) Key() [8]byte {
	sum := sha256.Sum256([]byte(t.text))
	return [8]byte(sum[:8])
}

// WebSocketTargets is the table a WebSocket leg resolves named addresses
// through (WebSocketConfig.Targets). Entries are added and never removed.
type WebSocketTargets struct {
	mu sync.RWMutex
	m  map[[8]byte]WebSocketTarget
}

func NewWebSocketTargets() *WebSocketTargets {
	return &WebSocketTargets{m: map[[8]byte]WebSocketTarget{}}
}

// Register stores t and returns the address that stands for it.
func (s *WebSocketTargets) Register(t WebSocketTarget) netip.AddrPort {
	k := t.Key()
	s.mu.Lock()
	s.m[k] = t
	s.mu.Unlock()
	return objproto.NamedAddrPort(k)
}

func (s *WebSocketTargets) Lookup(key [8]byte) (WebSocketTarget, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, ok := s.m[key]
	return t, ok
}

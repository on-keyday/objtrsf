package transport

import (
	"crypto/sha256"
	"strings"
	"testing"

	"github.com/on-keyday/objtrsf/objproto"
)

func TestParseWebSocketTargetRejects(t *testing.T) {
	for _, c := range []struct{ raw, rule string }{
		{"wss://h", "path"},
		{"wss://h/ws?", "query is not supported"},
		{"wss://h/ws?x=1", "query is not supported"},
		{"wss://h/ws#", "fragment"},
		{"wss://u@h/ws", "user info"},
		{"wss://u:p@h/ws", "user info"},
		{"wss:h/ws", "host"},
		{"wss:///ws", "host"},
		{"wss://hé.example/ws", "non-ASCII"},
		{"wss://h./ws", "trailing dot"},
		{"wss://[fe80::1%25eth0]/ws", "zone"},
		{"wss://h:99999/ws", "port"},
		{"wss://h:0/ws", "port"},
		{"wss://h:x/ws", "port"},
		{"http://h/ws", "scheme"},
		{"wss://h/ws\x00", "control character"},
	} {
		_, err := ParseWebSocketTarget(c.raw)
		if err == nil {
			t.Errorf("ParseWebSocketTarget(%q) accepted, want an error naming %q", c.raw, c.rule)
			continue
		}
		if !strings.Contains(err.Error(), c.rule) {
			t.Errorf("ParseWebSocketTarget(%q) = %v, want it to name %q", c.raw, err, c.rule)
		}
	}
}

func TestParseWebSocketTargetCanonical(t *testing.T) {
	for _, c := range []struct{ raw, want, transport, host string }{
		{"wss://h/", "wss://h/", "wss", "h"},
		{"WSS://H.Example.COM:443/WS", "wss://h.example.com/WS", "wss", "h.example.com"},
		{"ws://h:80/ws", "ws://h/ws", "ws", "h"},
		{"wss://h:/ws", "wss://h/ws", "wss", "h"},
		{"wss://h:8443/ws", "wss://h:8443/ws", "wss", "h:8443"},
		{"wss://[::1]:8443/ws", "wss://[::1]:8443/ws", "wss", "[::1]:8443"},
		{"wss://h/a%2Fb", "wss://h/a%2Fb", "wss", "h"},
		{"wss://h/a%41", "wss://h/a%41", "wss", "h"},
		{"wss://h/a b", "wss://h/a%20b", "wss", "h"},
	} {
		tg, err := ParseWebSocketTarget(c.raw)
		if err != nil {
			t.Errorf("ParseWebSocketTarget(%q): %v", c.raw, err)
			continue
		}
		if got := tg.String(); got != c.want {
			t.Errorf("ParseWebSocketTarget(%q).String() = %q, want %q", c.raw, got, c.want)
		}
		if got := tg.Transport(); got != c.transport {
			t.Errorf("ParseWebSocketTarget(%q).Transport() = %q, want %q", c.raw, got, c.transport)
		}
		if got := tg.Host(); got != c.host {
			t.Errorf("ParseWebSocketTarget(%q).Host() = %q, want %q", c.raw, got, c.host)
		}
	}
	// Dot segments and a trailing slash are left alone: three candidates.
	seen := map[string]bool{}
	keys := map[[8]byte]bool{}
	for _, raw := range []string{"wss://h/ws", "wss://h/ws/", "wss://h/./ws"} {
		tg, err := ParseWebSocketTarget(raw)
		if err != nil {
			t.Fatal(err)
		}
		seen[tg.String()] = true
		keys[tg.Key()] = true
	}
	if len(seen) != 3 || len(keys) != 3 {
		t.Errorf("/ws, /ws/, /./ws gave %d texts and %d keys, want 3 and 3", len(seen), len(keys))
	}
}

func TestWebSocketTargetKeyStable(t *testing.T) {
	a, err := ParseWebSocketTarget("WSS://H:443/ws")
	if err != nil {
		t.Fatal(err)
	}
	b, err := ParseWebSocketTarget("wss://h/ws")
	if err != nil {
		t.Fatal(err)
	}
	if a.Key() != b.Key() {
		t.Errorf("keys differ for spellings of one target: %x vs %x", a.Key(), b.Key())
	}
	sum := sha256.Sum256([]byte(b.String()))
	if b.Key() != [8]byte(sum[:8]) {
		t.Errorf("Key() = %x, want the first 8 bytes of sha256(%q)", b.Key(), b.String())
	}
	if u := b.URL(); u.String() != b.String() {
		t.Errorf("URL() = %q, want %q", u, b.String())
	} else {
		u.Path = "/changed"
		if b.String() != "wss://h/ws" {
			t.Error("URL() returned the target's own URL, not a copy")
		}
	}
}

func TestWebSocketTargetsRegister(t *testing.T) {
	tg, err := ParseWebSocketTarget("wss://h/ws")
	if err != nil {
		t.Fatal(err)
	}
	s := NewWebSocketTargets()
	a1 := s.Register(tg)
	a2 := s.Register(tg)
	if a1 != a2 || a1 != objproto.NamedAddrPort(tg.Key()) {
		t.Fatalf("Register = %v then %v, want %v both times", a1, a2, objproto.NamedAddrPort(tg.Key()))
	}
	key, _ := objproto.NamedFromAddrPort(a1)
	got, ok := s.Lookup(key)
	if !ok || got.String() != tg.String() {
		t.Errorf("Lookup = %v, %v; want %v, true", got, ok, tg)
	}
	if _, ok := s.Lookup([8]byte{1}); ok {
		t.Error("Lookup of an unregistered key succeeded")
	}
}

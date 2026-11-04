package objproto

import (
	"net/netip"
	"strings"
	"testing"
)

// The id follows the LAST '-', so a host name that contains '-' reaches the
// resolver whole instead of being cut at its first hyphen.
func TestParseConnectionIDHyphenatedHost(t *testing.T) {
	cid, err := ParseConnectionID("ws:local-host.test:8540-7", ParseOption_ResolveAddr)
	if err == nil {
		t.Fatalf("an unresolvable name parsed: %v", cid)
	}
	// The error must come from looking up the whole name. A cut at the first
	// '-' fails before any lookup, as "invalid address format: local".
	if !strings.Contains(err.Error(), "local-host.test") {
		t.Fatalf("host was not looked up whole: %v", err)
	}

	cid, err = ParseConnectionID("ws:localhost:8540-7", ParseOption_ResolveAddr)
	if err != nil {
		t.Fatal(err)
	}
	if cid.Addr.Port() != 8540 || cid.ID != 7 || !cid.Addr.Addr().IsLoopback() {
		t.Fatalf("parsed %+v", cid)
	}
}

func TestParseConnectionIDLiteralsUnchanged(t *testing.T) {
	for _, c := range []struct {
		in   string
		addr string
		id   uint16
	}{
		{"ws:127.0.0.1:8539-12", "127.0.0.1:8539", 12},
		{"udp:[2001:db8::1]:8540-0", "[2001:db8::1]:8540", 0},
	} {
		cid, err := ParseConnectionID(c.in, 0)
		if err != nil {
			t.Fatalf("%s: %v", c.in, err)
		}
		if cid.Addr != netip.MustParseAddrPort(c.addr) || cid.ID != c.id {
			t.Errorf("%s parsed as %+v", c.in, cid)
		}
		if got := cid.String(); got != c.in {
			t.Errorf("String() = %q, want %q", got, c.in)
		}
	}
	for _, bad := range []string{"ws:127.0.0.1:8539", "ws:127.0.0.1:8539-", "ws:127.0.0.1:8539-x"} {
		if _, err := ParseConnectionID(bad, ParseOption_AllowRandomID); err == nil {
			t.Errorf("%q parsed", bad)
		}
	}
	if _, err := ParseConnectionID("ws:127.0.0.1:8539-*", ParseOption_AllowRandomID); err != nil {
		t.Errorf("random id: %v", err)
	}
}

package objproto

import (
	"fmt"
	"net/netip"
	"testing"
)

var testNamedKey = [8]byte{0x3f, 0x2a, 0x9c, 0x0d, 0x11, 0xb7, 0xe6, 0x04}

func TestNamedAddrRoundTrip(t *testing.T) {
	ap := NamedAddrPort(testNamedKey)
	if !ap.Addr().Is6() || ap.Port() != 0 {
		t.Fatalf("NamedAddrPort = %v, want an IPv6 address with port 0", ap)
	}
	if !netip.MustParsePrefix("100::/64").Contains(ap.Addr()) {
		t.Fatalf("NamedAddrPort = %v, want it inside 100::/64", ap)
	}
	got, ok := NamedFromAddrPort(ap)
	if !ok || got != testNamedKey {
		t.Fatalf("NamedFromAddrPort = %x, %v; want %x, true", got, ok, testNamedKey)
	}
}

func TestNamedFromAddrPortRejects(t *testing.T) {
	for _, s := range []string{"1.2.3.4:0", "[::1]:0", "[100::1]:443"} {
		if _, ok := NamedFromAddrPort(netip.MustParseAddrPort(s)); ok {
			t.Errorf("NamedFromAddrPort(%s) ok, want not a named address", s)
		}
	}
	// vsock and named addresses share 100::/64 with port 0; only the
	// transport tells them apart, so every caller checks it first.
	if _, ok := NamedFromAddrPort(VsockAddrPort(3, 5)); !ok {
		t.Error("NamedFromAddrPort(vsock) not ok; the shared prefix is expected to read as named")
	}
}

func TestNamedConnectionIDText(t *testing.T) {
	for _, tr := range []string{"ws", "wss"} {
		cid := NewConnectionID(tr, NamedAddrPort(testNamedKey), 4711)
		want := fmt.Sprintf("%s:@%x-4711", tr, testNamedKey[:])
		if got := cid.String(); got != want {
			t.Errorf("String() = %q, want %q", got, want)
		}
		for _, opt := range []ParseOption{0, ParseOption_ResolveAddr} {
			back, err := ParseConnectionID(want, opt)
			if err != nil {
				t.Fatalf("ParseConnectionID(%q, %d): %v", want, opt, err)
			}
			if back != cid {
				t.Errorf("ParseConnectionID(%q) = %v, want %v", want, back, cid)
			}
		}
	}
}

func TestNamedConnectionIDTextRejects(t *testing.T) {
	for _, s := range []string{
		"udp:@3f2a9c0d11b7e604-1",
		"vsock:@3f2a9c0d11b7e604-1",
		"wss:@3f2a9c0d11b7e60-1",
		"wss:@3f2a9c0d11b7e6040-1",
		"wss:@3f2a9c0d11b7e6zz-1",
		"wss:@-1",
	} {
		if cid, err := ParseConnectionID(s, ParseOption_ResolveAddr); err == nil {
			t.Errorf("ParseConnectionID(%q) = %v, want an error", s, cid)
		}
	}
	v := NewConnectionID(TransportVsock, VsockAddrPort(3, 5), 7)
	if got := v.String(); got != "vsock:3:5-7" {
		t.Errorf("vsock String() = %q, want vsock:3:5-7", got)
	}
	if back, err := ParseConnectionID("vsock:3:5-7", 0); err != nil || back != v {
		t.Errorf("ParseConnectionID(vsock:3:5-7) = %v, %v; want %v", back, err, v)
	}
}

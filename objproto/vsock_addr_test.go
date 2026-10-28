package objproto

import (
	"net/netip"
	"testing"
)

func TestVsockAddrPortRoundTrip(t *testing.T) {
	for _, c := range []struct{ cid, port uint32 }{{2, 5000}, {3, 1}, {0xffffffff, 0xffffffff}, {0, 0}} {
		ap := VsockAddrPort(c.cid, c.port)
		cid, port, ok := VsockFromAddrPort(ap)
		if !ok || cid != c.cid || port != c.port {
			t.Errorf("VsockFromAddrPort(VsockAddrPort(%d, %d)) = %d, %d, %v", c.cid, c.port, cid, port, ok)
		}
	}
}

func TestVsockFromAddrPortRejectsIP(t *testing.T) {
	for _, s := range []string{"127.0.0.1:80", "[::1]:0", "[2001:db8::2:0:1388]:0", "[100::2:0:1388]:1"} {
		if _, _, ok := VsockFromAddrPort(netip.MustParseAddrPort(s)); ok {
			t.Errorf("%s read as a vsock address", s)
		}
	}
}

func TestVsockConnectionIDText(t *testing.T) {
	cid, err := ParseConnectionID("vsock:2:5000-7", 0)
	if err != nil {
		t.Fatal(err)
	}
	if cid.Addr != VsockAddrPort(2, 5000) || cid.ID != 7 {
		t.Fatalf("parsed %+v", cid)
	}
	if got := cid.String(); got != "vsock:2:5000-7" {
		t.Errorf("String() = %q, want vsock:2:5000-7", got)
	}
	if _, err := ParseConnectionID("vsock:2-7", 0); err == nil {
		t.Error("vsock CID without a port parsed")
	}
	if _, err := ParseConnectionID("vsock:2:70000000000-7", 0); err == nil {
		t.Error("vsock port beyond u32 parsed")
	}
	// Under another transport the same address is printed as the IPv6 it is.
	other := NewConnectionID("udp", VsockAddrPort(2, 5000), 7)
	if got := other.String(); got != "udp:[100::2:0:1388]:0-7" {
		t.Errorf("String() = %q", got)
	}
}

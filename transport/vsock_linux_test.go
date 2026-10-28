//go:build linux

package transport

import (
	"context"
	"math/rand/v2"
	"strconv"
	"testing"
	"time"

	"golang.org/x/sys/unix"

	"github.com/on-keyday/objtrsf/objproto"
)

// listenVsockLocal listens on a free port and proves the local CID (the
// vsock_loopback transport) can reach it, skipping when this kernel cannot.
func listenVsockLocal(t *testing.T) uint32 {
	t.Helper()
	for range 20 {
		port := 20000 + rand.Uint32N(40000)
		l, err := ListenVsock(port)
		if err != nil {
			t.Skipf("no AF_VSOCK here: %v", err)
		}
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		c, err := DialVsock(ctx, unix.VMADDR_CID_LOCAL, port)
		cancel()
		l.Close()
		if err != nil {
			t.Skipf("vsock loopback (CID 1) unavailable: %v", err)
		}
		c.Close()
		return port
	}
	t.Fatal("no free vsock port")
	return 0
}

func TestDialVsockRefusedPort(t *testing.T) {
	port := listenVsockLocal(t) // closed again: nothing listens on it now
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	if c, err := DialVsock(ctx, unix.VMADDR_CID_LOCAL, port); err == nil {
		c.Close()
		t.Fatal("dial to a port nobody listens on succeeded")
	}
}

func TestVsockLegLoopbackRoundTrip(t *testing.T) {
	port := listenVsockLocal(t)
	srv, err := MultiLegEndpoint(quietLogger(), objproto.EndpointModeServer,
		VsockLeg(VsockConfig{Logger: quietLogger(), ListenPort: port}))
	if err != nil {
		t.Fatal(err)
	}
	cli, err := MultiLegEndpoint(quietLogger(), objproto.EndpointModeClient,
		VsockLeg(VsockConfig{Logger: quietLogger()}))
	if err != nil {
		t.Fatal(err)
	}
	dst, err := objproto.ParseConnectionID(
		"vsock:1:"+strconv.FormatUint(uint64(port), 10)+"-*", objproto.ParseOption_AllowRandomID)
	if err != nil {
		t.Fatal(err)
	}
	_, srvConn := roundTrip(t, srv, cli, dst)

	// The server keys the client by its own CID and ephemeral port, printed
	// in vsock form.
	got := srvConn.ConnectionID()
	cid, _, ok := objproto.VsockFromAddrPort(got.Addr)
	if got.Transport != objproto.TransportVsock || !ok || cid != unix.VMADDR_CID_LOCAL {
		t.Errorf("server-side CID = %s", got)
	}
	back, err := objproto.ParseConnectionID(got.String(), 0)
	if err != nil || back != got {
		t.Errorf("server-side CID %s does not parse back: %v, %v", got, back, err)
	}
}

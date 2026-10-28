package transport

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/netip"

	"github.com/on-keyday/objtrsf/objproto"
)

// VsockAddr is an AF_VSOCK address.
type VsockAddr struct {
	CID, Port uint32
}

func (a VsockAddr) Network() string { return objproto.TransportVsock }
func (a VsockAddr) String() string  { return fmt.Sprintf("%d:%d", a.CID, a.Port) }

// VsockConfig configures a vsock leg: a StreamLeg over AF_VSOCK, keyed by the
// address objproto.VsockAddrPort gives a peer.
type VsockConfig struct {
	Logger *slog.Logger
	// ListenPort, when non-zero, accepts on that port from any CID. Zero means
	// the leg only dials.
	ListenPort uint32
}

// VsockLeg is a MultiLegEndpoint leg for transport "vsock". Linux only: on
// other systems starting it fails when it listens, and every dial fails.
func VsockLeg(cfg VsockConfig) Leg {
	return Leg{
		transports: []string{objproto.TransportVsock},
		start: func(rawSess objproto.RawEndpoint, sendTo <-chan *objproto.PacketData) error {
			sc := StreamConfig{
				Logger:    cfg.Logger,
				Transport: objproto.TransportVsock,
				Dial:      dialVsockAddrPort,
				AddrOf:    vsockPeerAddrPort,
			}
			if cfg.ListenPort != 0 {
				l, err := ListenVsock(cfg.ListenPort)
				if err != nil {
					return fmt.Errorf("vsock listen on port %d: %w", cfg.ListenPort, err)
				}
				sc.Listener = l
			}
			return StreamEndpointEx(rawSess, sc, sendTo)
		},
	}
}

func dialVsockAddrPort(ctx context.Context, addr netip.AddrPort) (net.Conn, error) {
	cid, port, ok := objproto.VsockFromAddrPort(addr)
	if !ok {
		return nil, fmt.Errorf("%s is not a vsock address", addr)
	}
	return DialVsock(ctx, cid, port)
}

func vsockPeerAddrPort(c net.Conn) (netip.AddrPort, error) {
	a, ok := c.RemoteAddr().(VsockAddr)
	if !ok {
		return netip.AddrPort{}, fmt.Errorf("remote address %v is not vsock", c.RemoteAddr())
	}
	return objproto.VsockAddrPort(a.CID, a.Port), nil
}

//go:build !linux

package transport

import (
	"log/slog"
	"net"
	"net/netip"

	"github.com/on-keyday/objtrsf/objproto"
)

// readLoop hands each datagram to sess.Receive as it arrives, one recvfrom
// per datagram.
//
// The Linux build batches this with recvmmsg (udp_recv_linux.go). Every other
// platform keeps the plain read on purpose rather than going through
// x/net's ReadBatch, which degrades to a single RecvMsg off Linux anyway: it
// would buy nothing and would put a different syscall under the receive path
// on platforms this project cannot test, Windows in particular, where a
// regression reads as "the runner receives nothing".
func readLoop(udpConn *net.UDPConn, sess objproto.RawEndpoint, logger *slog.Logger) {
	buf := make([]byte, 65535)
	for {
		n, from, err := udpConn.ReadFromUDP(buf)
		if err != nil {
			logger.Error("failed to read udp packet", slog.String("error", err.Error()))
			continue
		}
		fromSlice, ok := netip.AddrFromSlice(from.IP[:])
		if !ok {
			logger.Error("invalid udp address", slog.String("from", from.String()))
			continue
		}
		netipAddr := netip.AddrPortFrom(fromSlice.Unmap(), uint16(from.Port))
		newBuf := make([]byte, n)
		copy(newBuf, buf[:n])
		sess.Receive("udp", netipAddr, newBuf)
	}
}

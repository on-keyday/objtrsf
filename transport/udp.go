package transport

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/netip"
	"syscall"

	"github.com/on-keyday/objtrsf/objproto"
)

func UDPEndpoint(logger *slog.Logger, port uint16, mode objproto.EndpointMode) (objproto.Endpoint, error) {
	sess := objproto.NewEndpoint(logger, mode)
	return UDPEndpointEx(sess, logger, port, sess.GetSenderChannel())
}

func UDPEndpointEx(sess objproto.RawEndpoint, logger *slog.Logger, port uint16, sendTo <-chan *objproto.PacketData) (objproto.Endpoint, error) {
	udpConn, err := net.ListenUDP("udp", &net.UDPAddr{
		IP:   net.IPv6unspecified,
		Port: int(port),
	})
	if err != nil {
		logger.Error("failed to listen on udp", slog.String("port", fmt.Sprintf("%d", port)), slog.String("error", err.Error()))
		return nil, err
	}
	if err := tunePMTUDProbe(udpConn); err != nil {
		logger.Debug("pmtud probe-mode tuning skipped", slog.String("error", err.Error()))
	}

	go func() {
		for pkt := range sendTo {
			if pkt.To.Transport != "udp" {
				logger.Error("unsupported transport for udp session", slog.String("transport", pkt.To.Transport))
				continue
			}
			_, err := udpConn.WriteToUDP(pkt.Data, &net.UDPAddr{
				IP:   pkt.To.Addr.Addr().AsSlice(),
				Port: int(pkt.To.Addr.Port()),
			})
			if err != nil {
				if errors.Is(err, syscall.EMSGSIZE) {
					logger.Debug("udp packet size too large, cannot send", slog.String("to", pkt.To.String()), slog.Int("size", len(pkt.Data)))
					continue // ignore too big error because upper layer implements PLPMTUD
				}
				logger.Error("failed to send udp packet", slog.String("to", pkt.To.String()), slog.String("error", err.Error()))
				sess.CannotSend(pkt)
			}
		}
	}()

	go func() {
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
	}()
	return sess, nil
}

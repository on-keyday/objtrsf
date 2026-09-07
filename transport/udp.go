package transport

import (
	"fmt"
	"log/slog"
	"net"

	"github.com/on-keyday/objtrsf/objproto"
)

// udpReadBufferBytes is what the receive buffer is asked for, against a kernel
// default (net.core.rmem_default) that is commonly 208 KB.
//
// 208 KB is not enough for a sustained transfer, and the reason is the reader
// loop below: it hands each datagram to sess.Receive SYNCHRONOUSLY, so while
// that call decrypts and demultiplexes, nothing is draining the socket and
// arrivals queue behind it. Full queue means the kernel drops them — and the
// drop is invisible to tc, because it happens past the qdisc, in the socket.
// The transport then sees a real gap and answers it by cutting the congestion
// window, so a buffer sizing question surfaces as a congestion-control one.
//
// Measured before this line existed: one 50 MB push across a link that dropped
// nothing at the qdisc produced 7245 RcvbufErrors on the server's socket
// (`ss -u -m` reported rb212992 and d18264 cumulative), 5396 packets declared
// lost by the transport, and 695 congestion responses — none of them spurious.
//
// A failure to grow it is logged and ignored: this is throughput, not
// correctness, and net.core.rmem_max caps what can be granted anyway — raising
// that ceiling is the operator's call, not this library's.
const udpReadBufferBytes = 4 << 20

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
	if err := udpConn.SetReadBuffer(udpReadBufferBytes); err != nil {
		logger.Debug("could not enlarge the udp receive buffer; the kernel default applies",
			slog.Int("wanted", udpReadBufferBytes), slog.String("error", err.Error()))
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
				if isMessageTooBig(err) {
					// Dropped, and NOT reported upward: this must not reach
					// CannotSend, which closes the connection. The upper layer
					// runs its own PLPMTUD and will conclude the same thing
					// from the probe going unacknowledged (trsf's PROBE_TIMER),
					// just a round trip later than the kernel already knows it.
					//
					// Handing the size verdict up instead is not the obvious
					// win it looks like, because "EMSGSIZE" is not one fact.
					// Probe-mode Linux (IP_PMTUDISC_PROBE) ignores the path-MTU
					// cache, so it raises this ONLY above the local interface
					// MTU; Windows and Darwin/FreeBSD have no probe mode and
					// set the bare DF bit, so their kernels' own path-MTU
					// knowledge is in play as well. The same probe size can
					// therefore fail locally on one host and go out on the
					// wire from another, which would make the estimate depend
					// on the sender's OS. And on the path this discovery
					// exists FOR -- a tunnel narrower than the interface, say
					// a 1280-byte one -- Linux raises nothing at all, so the
					// timeout path has to work regardless.
					logger.Debug("udp packet size too large, cannot send", slog.String("to", pkt.To.String()), slog.Int("size", len(pkt.Data)))
					continue
				}
				logger.Error("failed to send udp packet", slog.String("to", pkt.To.String()), slog.String("error", err.Error()))
				sess.CannotSend(pkt)
			}
		}
	}()

	go readLoop(udpConn, sess, logger)
	return sess, nil
}

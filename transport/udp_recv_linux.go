package transport

import (
	"log/slog"
	"net"
	"net/netip"

	"github.com/on-keyday/objtrsf/objproto"
	"golang.org/x/net/ipv6"
)

// recvBatchSize is how many datagrams one recvmmsg asks for.
//
// Measured over a 16 MB transfer, one datagram per read took 12,328 reads;
// asking for 8 took 3,225, a mean of 3.93 datagrams each. The distribution is
// bimodal -- a pile of single-datagram reads where the reader had caught up,
// and 1,222 reads that came back completely full -- so the bursts are real and
// they exceed this. Asking for 64 reached a mean of 8.09.
//
// 8 rather than 64 because each slot holds a 65535-byte buffer for the life of
// the endpoint: 8 costs 512 KB, against the 4 MB the socket's own receive
// buffer already takes, and captures most of the reduction. The buffers stay
// full-size because this transport does not get to assume the MTU of whatever
// sends to it.
const recvBatchSize = 8

// readLoop drains the socket with recvmmsg, which matters here beyond the
// syscall count: sess.Receive runs on this goroutine, so nothing reads the
// socket while a datagram is being decrypted and demultiplexed, and the
// overflow is dropped past the qdisc where tc cannot see it (see
// udpReadBufferBytes). Taking a burst in one call shortens that window.
//
// Throughput was +6.6% and +10.5% over two interleaved 20- and 40-run sets
// with the objproto-free control flat, which does not clear what a loaded box
// resolves. The syscall reduction is a count and is not in doubt; the
// percentage is not claimed.
func readLoop(udpConn *net.UDPConn, sess objproto.RawEndpoint, logger *slog.Logger) {
	pc := ipv6.NewPacketConn(udpConn)
	msgs := make([]ipv6.Message, recvBatchSize)
	for i := range msgs {
		msgs[i].Buffers = [][]byte{make([]byte, 65535)}
	}
	for {
		n, err := pc.ReadBatch(msgs, 0)
		if err != nil {
			logger.Error("failed to read udp packet", slog.String("error", err.Error()))
			continue
		}
		for i := range msgs[:n] {
			m := &msgs[i]
			from, ok := m.Addr.(*net.UDPAddr)
			if !ok {
				logger.Error("unexpected udp address type", slog.String("from", m.Addr.String()))
				continue
			}
			fromSlice, ok := netip.AddrFromSlice(from.IP[:])
			if !ok {
				logger.Error("invalid udp address", slog.String("from", from.String()))
				continue
			}
			netipAddr := netip.AddrPortFrom(fromSlice.Unmap(), uint16(from.Port))
			newBuf := make([]byte, m.N)
			copy(newBuf, m.Buffers[0][:m.N])
			sess.Receive("udp", netipAddr, newBuf)
		}
	}
}

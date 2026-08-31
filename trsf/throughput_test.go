package trsf_test

import (
	"context"
	"crypto/ecdh"
	"io"
	"log/slog"
	"net"
	"net/netip"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/on-keyday/objtrsf/objproto"
	"github.com/on-keyday/objtrsf/transport"
	"github.com/on-keyday/objtrsf/trsf"
	"github.com/on-keyday/objtrsf/trsf/mock"
)

// Throughput, attributed to a layer instead of to the whole stack.
//
// Every throughput figure this transport has ever had was taken by driving a
// three-process application through a relay: two connections, 19 pipeline
// stages, and a benchmark whose runs spanned 1.9x. That measures the sum and
// attributes nothing, which is how four consecutive local "fixes" were argued
// from a profile and then killed by measurement — the one real result of that
// round was a socket receive buffer (transport/udp.go), and it was found only
// after the loss detector had been wrongly accused.
//
// These are the same bulk transfer at three rungs, so a number belongs to a
// layer:
//
//	BenchmarkThroughput/mock   trsf alone. No crypto, no syscalls, no kernel;
//	                           packets move by channel send (trsf/mock). The
//	                           state machine's own ceiling.
//	BenchmarkThroughput/udp    the same, plus objproto (AES-GCM, packet
//	                           numbers) and a real loopback UDP socket.
//	BenchmarkThroughput/relay  the same again, but through a middle endpoint
//	                           that splices two connections the way a server
//	                           relaying between two peers does.
//
// mock/udp is what the packet layer costs. udp/relay is what relaying costs,
// measured with the process boundary and the application removed, so it
// cannot be confused with either.
//
// A single run of any of these is not a result — that is the lesson the
// spread above paid for. Establish a difference the way Go already knows how:
//
//	go test ./trsf -run '^$' -bench Throughput -benchtime 1x -count 10 > new.txt
//	benchstat old.txt new.txt
//
// TRSF_BENCH_MB overrides the per-iteration transfer size (default 32).

// benchSizeMB is the payload moved per benchmark iteration. Small enough that
// -count 10 stays interactive, large enough that connection setup and the
// slow-start ramp are a minority of it — the ramp is paid once, in the
// discarded warm-up below, not in the timed iterations.
func benchSizeMB(tb testing.TB) int {
	raw := os.Getenv("TRSF_BENCH_MB")
	if raw == "" {
		return 32
	}
	mb, err := strconv.Atoi(raw)
	if err != nil || mb <= 0 {
		tb.Fatalf("TRSF_BENCH_MB=%q: want a positive integer", raw)
	}
	return mb
}

// relayChunk is the read size a spliced relay typically uses, and the one the
// relay rung below uses, so the write pattern is the one a real relay
// produces rather than a shape chosen to flatter the transport.
const relayChunk = 64 << 10

func BenchmarkThroughput(b *testing.B) {
	size := benchSizeMB(b) << 20
	for _, tc := range []struct {
		name string
		pair func(testing.TB) (source, sink trsf.Transport)
	}{
		{"mock", mockPair},
		{"udp", udpPair},
		{"relay", relayPair(spliceOneWay)},
		{"relay-pipelined", relayPair(splicePipelined)},
	} {
		b.Run(tc.name, func(b *testing.B) {
			ctx := b.Context()
			source, sink := tc.pair(b)
			send, recv := connectedStream(b, ctx, source, sink)

			// Discarded warm-up: the first bulk transfer on a connection pays
			// the slow-start ramp and the first-touch page faults. Including
			// it would make run 1 differ from the rest for a reason that is
			// not the change under test.
			if err := bulkTransfer(ctx, send, recv, size, relayChunk); err != nil {
				b.Fatalf("warm-up transfer: %v", err)
			}

			b.SetBytes(int64(size))
			for b.Loop() {
				if err := bulkTransfer(ctx, send, recv, size, relayChunk); err != nil {
					b.Fatalf("transfer: %v", err)
				}
			}
		})
	}
}

// bulkTransfer moves size bytes over one already-accepted stream and returns
// once the last byte has been read on the far side. Reading concurrently is
// not a convenience: a send stream blocks at its 1 MB buffer limit
// (send_stream.go), so a transfer larger than that deadlocks if nothing is
// draining.
func bulkTransfer(ctx context.Context, send trsf.SendStream, recv trsf.ReceiveStream, size, chunk int) error {
	done := make(chan error, 1)
	go func() {
		for got := 0; got < size; {
			data, eof, err := recv.ReadDirectContext(ctx, uint64(size-got))
			if err != nil {
				done <- err
				return
			}
			got += len(data)
			if eof && got < size {
				done <- io.ErrUnexpectedEOF
				return
			}
		}
		done <- nil
	}()

	buf := make([]byte, chunk)
	for sent := 0; sent < size; sent += chunk {
		// WriteContext, not AppendData: AppendData retains the caller's slice
		// until the range is acked (its doc comment says the caller must copy
		// first), so a reused buffer would be rewritten underneath the
		// retransmit queue. WriteContext makes that copy, which is also the
		// copy the real send path pays.
		if _, err := send.WriteContext(ctx, buf[:min(chunk, size-sent)]); err != nil {
			return err
		}
	}
	return <-done
}

// connectedStream opens a bidirectional stream on source and returns it
// together with the far end accepted on sink. Creating the stream advertises
// it, so the accept completes without any payload having been written.
func connectedStream(tb testing.TB, ctx context.Context, source, sink trsf.Transport) (trsf.BidirectionalStream, trsf.BidirectionalStream) {
	tb.Helper()
	send := source.CreateBidirectionalStream()
	if send == nil {
		tb.Fatal("CreateBidirectionalStream returned nil")
	}
	type accepted struct {
		stream trsf.BidirectionalStream
		err    error
	}
	ch := make(chan accepted, 1)
	go func() {
		s, err := sink.AcceptBidirectionalStream(ctx)
		ch <- accepted{s, err}
	}()
	select {
	case a := <-ch:
		if a.err != nil {
			tb.Fatalf("accept: %v", a.err)
		}
		return send, a.stream
	case <-time.After(10 * time.Second):
		tb.Fatal("the far end never accepted the stream")
		return nil, nil
	}
}

// --- rung 1: trsf with everything below it removed ----------------------

func mockPair(tb testing.TB) (trsf.Transport, trsf.Transport) {
	// LevelError, not the helper's default LevelDebug: the send and receive
	// paths log per packet, and a debug handler on stderr would be most of
	// what this benchmark measures.
	source, sink := mock.SetupClientServerEx(tb, slog.LevelError)
	mock.BackgroundIO(tb, source, sink)
	return source, sink
}

// --- rung 2: over objproto on a real socket ------------------------------

func udpPair(tb testing.TB) (trsf.Transport, trsf.Transport) {
	ctx := tb.Context()
	log := slog.New(slog.DiscardHandler)

	sinkPort := freeUDPPorts(tb, 1)[0]
	sourceEP := udpEndpoint(tb, 0, log)
	sinkEP := udpEndpoint(tb, sinkPort, log)

	sourceConn, sinkConn := udpConnPair(tb, ctx, sourceEP, sinkEP, sinkPort)
	return trsfOver(ctx, false, sourceConn, log), trsfOver(ctx, true, sinkConn, log)
}

// --- rung 3: the same, spliced through a relay ---------------------------

// relayPair puts a middle endpoint between the two ends and copies bytes
// across it, which is what a server between two peers does. Both of the
// relay's legs share ONE endpoint, and therefore one socket and the one pair
// of goroutines that drives it (transport/udp.go): that sharing is part of
// what is being measured, so splitting the legs across two endpoints here
// would measure a topology nobody runs.
func relayPair(splice func(context.Context, trsf.ReceiveStream, trsf.SendStream)) func(testing.TB) (trsf.Transport, trsf.Transport) {
	return func(tb testing.TB) (trsf.Transport, trsf.Transport) {
		return newRelayPair(tb, splice)
	}
}

func newRelayPair(tb testing.TB, splice func(context.Context, trsf.ReceiveStream, trsf.SendStream)) (trsf.Transport, trsf.Transport) {
	ctx := tb.Context()
	log := slog.New(slog.DiscardHandler)

	ports := freeUDPPorts(tb, 2)
	relayPort, sinkPort := ports[0], ports[1]
	sourceEP := udpEndpoint(tb, 0, log)
	relayEP := udpEndpoint(tb, relayPort, log)
	sinkEP := udpEndpoint(tb, sinkPort, log)

	sourceConn, relayInbound := udpConnPair(tb, ctx, sourceEP, relayEP, relayPort)
	relayOutbound, sinkConn := udpConnPair(tb, ctx, relayEP, sinkEP, sinkPort)

	source := trsfOver(ctx, false, sourceConn, log)
	relayIn := trsfOver(ctx, true, relayInbound, log)
	relayOut := trsfOver(ctx, false, relayOutbound, log)
	sink := trsfOver(ctx, true, sinkConn, log)

	go func() {
		in, err := relayIn.AcceptBidirectionalStream(ctx)
		if err != nil {
			return
		}
		out := relayOut.CreateBidirectionalStream()
		if out == nil {
			return
		}
		splice(ctx, in, out)
	}()
	return source, sink
}

// spliceOneWay is the read-then-write loop a spliced relay runs: one chunk is
// read, then written, then the next is read. The two legs never overlap, and
// that alternation is one of the things this rung exists to price.
func spliceOneWay(ctx context.Context, src trsf.ReceiveStream, dst trsf.SendStream) {
	for {
		data, eof, err := src.ReadDirectContext(ctx, relayChunk)
		if err != nil {
			return
		}
		if len(data) > 0 || eof {
			// ReadDirect hands back a fresh slice, so passing it straight to
			// AppendData satisfies that call's "must be copied first" without
			// a second copy.
			if err := dst.AppendDataContext(ctx, eof, data); err != nil {
				return
			}
		}
		if eof {
			return
		}
	}
}

// splicePipelined is spliceOneWay with the read and the write decoupled by a
// bounded queue, so the two legs can be busy at once.
//
// **It is 17.1% SLOWER than the alternating version** (29.1 -> 24.1 MB/s over
// 8 runs each, resolving ±11%), and it is kept as the standing evidence for
// that. Decoupling a relay's two legs is an obvious-looking idea that gets
// proposed from the shape of the code — the alternation is real, and the
// legs really never overlap — but the alternation is not what the relay rung
// costs. A relay does the work twice, receive and decrypt then encrypt and
// send, both through the one endpoint's single sender and reader; the
// overlap this buys is worth less than the extra handoff and queue it costs.
// Anything aimed at the relay's ~2.5x has to beat 29.1, not 24.1.
func splicePipelined(ctx context.Context, src trsf.ReceiveStream, dst trsf.SendStream) {
	// Depth 8, so the reader can run ahead by half a megabyte at 64 KB a
	// chunk without becoming an unbounded buffer in front of the send
	// stream's own 1 MB limit.
	queue := make(chan []byte, 8)
	go func() {
		defer close(queue)
		for {
			data, eof, err := src.ReadDirectContext(ctx, relayChunk)
			if err != nil {
				return
			}
			if len(data) > 0 {
				select {
				case queue <- data:
				case <-ctx.Done():
					return
				}
			}
			if eof {
				return
			}
		}
	}()
	for data := range queue {
		if err := dst.AppendDataContext(ctx, false, data); err != nil {
			return
		}
	}
}

// --- wiring shared by the two socket-backed rungs ------------------------

func udpEndpoint(tb testing.TB, port uint16, log *slog.Logger) objproto.Endpoint {
	tb.Helper()
	ep, err := transport.UDPEndpoint(log, port, objproto.EndpointModeMutual)
	if err != nil {
		tb.Fatalf("udp endpoint on port %d: %v", port, err)
	}
	return ep
}

// udpConnPair completes one ECDH handshake from dialEP to listenEP and
// returns both ends of it. The dial has to run while the accept is waiting:
// the accepted connection is produced by the same exchange the dial is
// blocked on, so doing them in sequence deadlocks.
func udpConnPair(tb testing.TB, ctx context.Context, dialEP, listenEP objproto.Endpoint, listenPort uint16) (dialed, accepted objproto.Connection) {
	tb.Helper()
	cid, err := objproto.NewRandomConnectionID("udp",
		netip.AddrPortFrom(netip.AddrFrom4([4]byte{127, 0, 0, 1}), listenPort))
	if err != nil {
		tb.Fatalf("connection id: %v", err)
	}

	type result struct {
		conn objproto.Connection
		err  error
	}
	dial := make(chan result, 1)
	go func() {
		c, err := objproto.DoECDHHandshake(ctx, dialEP, cid, ecdh.P521(), objproto.AES128GCM)
		dial <- result{c, err}
	}()
	accepted, err = listenEP.WaitNewActiveConnection(10 * time.Second)
	if err != nil {
		tb.Fatalf("accept on port %d: %v", listenPort, err)
	}
	d := <-dial
	if d.err != nil {
		tb.Fatalf("dial to port %d: %v", listenPort, d.err)
	}
	tb.Cleanup(func() {
		_ = d.conn.Close()
		_ = accepted.Close()
	})
	return d.conn, accepted
}

// trsfOver wires a trsf transport onto one objproto connection the way an
// application does: NewStreams, then the two pump goroutines.
func trsfOver(ctx context.Context, isServer bool, conn objproto.Connection, log *slog.Logger) trsf.Transport {
	t := trsf.NewStreams(ctx, isServer, trsf.DefaultInitialMTU, trsf.DefaultMaxMTU, conn, log)
	go trsf.AutoSend(ctx, t, conn, nil)
	go trsf.AutoReceive(ctx, t, conn, nil)
	return t
}

// freeUDPPorts picks n distinct ports by binding them all at once and letting
// go only afterwards. UDPEndpoint takes a port rather than reporting the one
// it was given, so the ports have to be chosen before the endpoints exist;
// binding them simultaneously is what keeps the kernel from handing the same
// port back twice.
func freeUDPPorts(tb testing.TB, n int) []uint16 {
	tb.Helper()
	held := make([]*net.UDPConn, 0, n)
	ports := make([]uint16, 0, n)
	for range n {
		c, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.IPv6loopback, Port: 0})
		if err != nil {
			tb.Fatalf("pick a port: %v", err)
		}
		held = append(held, c)
		ports = append(ports, uint16(c.LocalAddr().(*net.UDPAddr).Port))
	}
	for _, c := range held {
		if err := c.Close(); err != nil {
			tb.Fatalf("release a port: %v", err)
		}
	}
	return ports
}

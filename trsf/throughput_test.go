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
//	BenchmarkThroughput/relay-setproxy
//	                           the same topology, except the middle endpoint
//	                           FORWARDS packets (objproto.SetProxy) instead of
//	                           terminating them: one connection end-to-end, and
//	                           a relay that holds no keys.
//
// mock/udp is what the packet layer costs. udp/relay is what relaying costs,
// measured with the process boundary and the application removed, so it
// cannot be confused with either. relay/relay-setproxy splits that further,
// into the part a relay pays for being in the path at all and the part it
// pays for terminating the crypto once it is there.
//
// A single run of any of these is not a result — that is the lesson the
// spread above paid for. Establish a difference the way Go already knows how:
//
//	go test ./trsf -run '^$' -bench Throughput -benchtime 1x -count 10 > new.txt
//	benchstat old.txt new.txt
//
// TRSF_BENCH_MB overrides the per-iteration transfer size (default 32).
//
// **CHECK THE MACHINE'S LOAD FIRST, and the relay rungs need it near zero.**
// A relay rung runs three connections and four transports in this one
// process; on a busy box it swung 20.6–29.1 MB/s while mock (144.7–155.1) and
// udp (87.1–90.6) held steady over the same period. Two interleaved 20-run
// sets then disagreed by 26 points on the same comparison — so the ± figure
// computed from a single run's stdev UNDERSTATES the real error whenever
// something else is running, and a relay-rung difference under ~25% taken on
// a loaded box is not a result. mock and udp are far more forgiving; mock is
// also the control for any objproto-side change, since it never reaches it.

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
		{"relay", relayPair(spliceOneWay, false)},
		{"relay-setproxy", proxyPair},
		{"relay-pipelined", relayPair(splicePipelined, false)},
		{"relay-2ep", relayPair(spliceOneWay, true)},
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
func relayPair(splice func(context.Context, trsf.ReceiveStream, trsf.SendStream), splitEndpoints bool) func(testing.TB) (trsf.Transport, trsf.Transport) {
	return func(tb testing.TB) (trsf.Transport, trsf.Transport) {
		return newRelayPair(tb, splice, splitEndpoints)
	}
}

func newRelayPair(tb testing.TB, splice func(context.Context, trsf.ReceiveStream, trsf.SendStream), splitEndpoints bool) (trsf.Transport, trsf.Transport) {
	ctx := tb.Context()
	log := slog.New(slog.DiscardHandler)

	ports := freeUDPPorts(tb, 2)
	relayPort, sinkPort := ports[0], ports[1]
	sourceEP := udpEndpoint(tb, 0, log)
	relayEP := udpEndpoint(tb, relayPort, log)
	sinkEP := udpEndpoint(tb, sinkPort, log)

	// splitEndpoints gives the relay's outbound leg a socket, and therefore a
	// sender and reader goroutine, of its own. It is the test for whether
	// those being per-endpoint is what a relay pays: with one endpoint both
	// legs funnel through a single pair, with two they do not, and nothing
	// else differs between the two rungs.
	//
	// NOT ESTABLISHED either way. Three run sets gave +8.1%, -17.7%, -13.7%
	// — the sign does not even hold, all of them on a loaded box. What can be
	// said is only that nothing here supports a second endpoint HELPING.
	// Re-run it on an idle machine before believing any of those numbers.
	relayDialEP := relayEP
	if splitEndpoints {
		relayDialEP = udpEndpoint(tb, 0, log)
	}
	sourceConn, relayInbound := udpConnPair(tb, ctx, sourceEP, relayEP, relayPort)
	relayOutbound, sinkConn := udpConnPair(tb, ctx, relayDialEP, sinkEP, sinkPort)

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
// **It never went faster than the alternating version.** Four independent run
// sets put it at -17.1%, -1.2%, -8.3% and -19.3%: the direction is consistent
// and the magnitude is not, so read it as "decoupling does not help", never as
// a number. (An earlier commit here claimed the -17.1% as the result; it did
// not reproduce. See the load caveat at the top of this file.)
//
// Decoupling a relay's two legs is an obvious-looking idea that the shape of
// the code keeps suggesting — the alternation is real, and the legs really
// never overlap. But the alternation is not what the relay rung costs. A
// relay does the work twice, receive and decrypt then encrypt and send, both
// through the one endpoint's single sender and reader, and the overlap a
// queue buys is worth less than the handoff it adds. This is kept as the
// cheapest way to stop the idea being proposed a third time.
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

// --- rung 4: the same, forwarded through a relay that never decrypts ------

// proxyPair puts a middle endpoint between the two ends that FORWARDS packets
// rather than terminating them. SetProxy registers the setting under both
// connection ids (objproto.go:474), so receive()'s proxy branch matches a
// datagram from either side and sends it straight back out without ever
// reaching decryption or an activeConnection. The ECDH therefore completes
// end-to-end between source and sink, and the relay holds no keys.
//
// Against the relay rung, exactly one thing differs: whether the middle box
// terminates the crypto or forwards the packet. Hop count is the same, the
// relay still drives both directions through its single endpoint's one sender
// and one reader goroutine, and the process layout is unchanged. What is
// removed is AES-GCM twice, two of the four trsf stacks, the
// ReadDirect/AppendData copy, and the alternation between the legs. So the gap
// between the two rungs is the price of terminating in the middle, isolated
// from the price of being in the path.
//
// It cannot reach the udp rung: the relay still receives and re-sends one
// datagram per packet, and that is what being in the path costs.
//
// **Measured +80%,** 10 interleaved rounds on a quieted box (medians: mock
// 193.6, udp 110.7, relay-setproxy 65.6, relay 36.4 MB/s), which recovers 39%
// of the udp/relay gap and leaves 40% still owed to being in the path at all.
// A first set taken while the box was busy AND block-ordered — the two things
// the header warns about — put it at +92% with no rounds missing, so the
// direction survives both methods and only the magnitude is soft. One round in
// ten produced no relay-setproxy result and the reason was not captured; the
// rung passed 6/6 when re-run alone, and the +92% set had no gaps, so the
// exclusion is not what produces the difference.
func proxyPair(tb testing.TB) (trsf.Transport, trsf.Transport) {
	ctx := tb.Context()
	log := slog.New(slog.DiscardHandler)

	// The source gets a known port here, where the other rungs leave it
	// ephemeral: receive() keys a packet by the address it came FROM
	// (objproto.go, NewConnectionID(transport, from, ...)), so the proxy has to
	// be told the source's own address, and UDPEndpoint does not report the
	// port it bound. Binding it is bookkeeping, not a change to the path.
	ports := freeUDPPorts(tb, 3)
	sourcePort, relayPort, sinkPort := ports[0], ports[1], ports[2]
	sourceEP := udpEndpoint(tb, sourcePort, log)
	relayEP := udpEndpoint(tb, relayPort, log)
	sinkEP := udpEndpoint(tb, sinkPort, log)

	loopback := func(port uint16) netip.AddrPort {
		return netip.AddrPortFrom(netip.AddrFrom4([4]byte{127, 0, 0, 1}), port)
	}

	// One connection id at three addresses. The proxy setting maps the two
	// ENDS — keyed by where their packets come from — while the source dials
	// the same id at the RELAY, which is the address it puts on the wire.
	// Both ends end up believing their peer sits at the relay; only the relay
	// knows otherwise. This is the shape runner/relay_handler.go builds from
	// serverCID.Addr and the target's addr.
	slot, err := objproto.NewRandomConnectionID("udp", loopback(relayPort))
	if err != nil {
		tb.Fatalf("slot id: %v", err)
	}
	owned := objproto.NewConnectionID("udp", loopback(sourcePort), slot.ID)
	allocate := objproto.NewConnectionID("udp", loopback(sinkPort), slot.ID)
	dialCID := objproto.NewConnectionID("udp", loopback(relayPort), slot.ID)
	if err := relayEP.SetProxy(owned, allocate); err != nil {
		tb.Fatalf("SetProxy(%v, %v): %v", owned, allocate, err)
	}

	// The dial has to run while the accept waits, for the reason udpConnPair
	// gives: the accepted connection is produced by the exchange the dial is
	// blocked on.
	type result struct {
		conn objproto.Connection
		err  error
	}
	dial := make(chan result, 1)
	go func() {
		c, err := objproto.DoECDHHandshake(ctx, sourceEP, dialCID, ecdh.P521(), objproto.AES128GCM)
		dial <- result{c, err}
	}()
	sinkConn, err := sinkEP.WaitNewActiveConnection(10 * time.Second)
	if err != nil {
		tb.Fatalf("accept behind the proxy: %v", err)
	}
	d := <-dial
	if d.err != nil {
		tb.Fatalf("dial through the proxy: %v", d.err)
	}
	tb.Cleanup(func() {
		_ = d.conn.Close()
		_ = sinkConn.Close()
	})

	return trsfOver(ctx, false, d.conn, log), trsfOver(ctx, true, sinkConn, log)
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

// TestUDPPacketOccupancy reports how full the packets on the UDP path
// actually are — payload bytes moved, divided by packets the sender put on
// the wire.
//
// It exists because the two remaining levers on this transport are bytes per
// packet and work per byte, and every arithmetic estimate of either one has
// to assume an answer to this. It is a count, not a rate, so unlike the
// throughput rungs it is unaffected by what else the machine is doing.
func TestUDPPacketOccupancy(t *testing.T) {
	ctx := t.Context()
	log := slog.New(slog.DiscardHandler)

	sinkPort := freeUDPPorts(t, 1)[0]
	sourceEP := udpEndpoint(t, 0, log)
	sinkEP := udpEndpoint(t, sinkPort, log)
	sourceConn, sinkConn := udpConnPair(t, ctx, sourceEP, sinkEP, sinkPort)
	source := trsfOver(ctx, false, sourceConn, log)
	sink := trsfOver(ctx, true, sinkConn, log)
	send, recv := connectedStream(t, ctx, source, sink)

	const size = 8 << 20
	// ConsumePacketNumber both reads and advances, so each of these two calls
	// burns a number of its own; the pair of them costs 2.
	before := sourceConn.ConsumePacketNumber()
	if err := bulkTransfer(ctx, send, recv, size, relayChunk); err != nil {
		t.Fatalf("transfer: %v", err)
	}
	after := sourceConn.ConsumePacketNumber()

	packets := after - before - 1
	if packets == 0 {
		t.Fatal("sender consumed no packet numbers")
	}
	mtu := source.GetInternalState().CurrentMTU
	perPacket := float64(size) / float64(packets)
	t.Logf("%d payload bytes in %d packets = %.0f bytes/packet, at MTU %d (%.0f%% full)",
		size, packets, perPacket, mtu, perPacket/float64(mtu)*100)
}

package trsf_test

import (
	"context"
	"testing"
	"time"
)

// What bounds the datagram send rate?
//
// A harness measurement put a udp tunnel at ~500 datagrams/s losslessly and
// flat across a 128x payload sweep, which says the ceiling is a PACKET rate and
// not a byte rate. The explanation reached for at the time -- the run loop
// emits at most one datagram per pass, so the ceiling IS the loop's iteration
// rate -- was read out of the code and never measured, and the numbers did not
// fit it: the same loop pushes ~4,850 packets/s for a stream workload on the
// same box.
//
// This measures the two rates directly. If sent/s tracks the loop's
// iterations/s, the one-per-pass reading is right and the lever is the loop.
// If the loop spins far faster than it emits, the constraint is somewhere else
// and raising the queue depth or the per-pass count would buy nothing.
//
//	go test ./trsf -run '^$' -bench RateAgainstLoopRate -benchtime 1x -v
//
// Benchmarks, so a timed measurement that asserts almost nothing is not paid
// for on every `go test ./...`.
const rateProbeKind = 0x48

func BenchmarkDatagramSendRateAgainstLoopRate(b *testing.B) {
	t := b
	for b.Loop() {
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	source, sink := udpPair(t)
	claim := func(k uint8) bool { return k == rateProbeKind }
	source.SetDatagramKinds(claim)
	sink.SetDatagramKinds(claim)

	// Drained, so the receive queue is never what refuses.
	go func() {
		for {
			if _, err := sink.ReceiveDatagram(ctx); err != nil {
				return
			}
		}
	}()

	payload := make([]byte, 1100)
	payload[0] = rateProbeKind

	// One warm-up second: the first datagrams pay the connection's own ramp.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		_ = source.SendDatagram(payload)
	}

	before := source.GetInternalState()
	start := time.Now()
	var accepted, refused int64
	for time.Since(start) < 3*time.Second {
		if err := source.SendDatagram(payload); err != nil {
			refused++
			continue
		}
		accepted++
	}
	elapsed := time.Since(start)
	after := source.GetInternalState()

	loops := after.LoopIterations - before.LoopIterations
	sent := after.DatagramsSent - before.DatagramsSent
	queueDrops := after.DatagramsDroppedSendQueue - before.DatagramsDroppedSendQueue
	congDrops := after.DatagramsDroppedCongestion - before.DatagramsDroppedCongestion

	secs := elapsed.Seconds()
	t.Logf("offered    %8.0f/s (accepted %d, refused %d)", float64(accepted+refused)/secs, accepted, refused)
	t.Logf("sent       %8.0f/s (%d)", float64(sent)/secs, sent)
	t.Logf("loop iter  %8.0f/s (%d)", float64(loops)/secs, loops)
	t.Logf("drops      send_queue=%d congestion=%d", queueDrops, congDrops)
	if sent > 0 {
		t.Logf("loops per datagram sent: %.2f", float64(loops)/float64(sent))
	}

	// The only thing asserted: the loop ran. Everything else is the reading,
	// and nailing a rate to a number on a shared box would make this a flake
	// rather than a measurement.
	if loops == 0 {
		t.Fatal("the run loop did not iterate at all during the measurement window")
	}
}

// The control. A stream on the same pair, so the loop's packet rate is measured
// on a workload that is known to reach thousands per second. Without it a
// datagram rate has nothing to be slow relative to.
func BenchmarkStreamPacketRateAgainstLoopRate(b *testing.B) {
	t := b
	for b.Loop() {
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	source, sink := udpPair(t)
	send, recv := connectedStream(t, ctx, source, sink)

	go func() {
		buf := make([]byte, 64*1024)
		for {
			if _, err := recv.Read(buf); err != nil {
				return
			}
		}
	}()

	chunk := make([]byte, 64*1024)
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		_ = send.AppendData(false, chunk)
	}

	before := source.GetInternalState()
	start := time.Now()
	var offered int64
	for time.Since(start) < 3*time.Second {
		if err := send.AppendData(false, chunk); err != nil {
			continue
		}
		offered += int64(len(chunk))
	}
	elapsed := time.Since(start)
	after := source.GetInternalState()

	loops := after.LoopIterations - before.LoopIterations
	secs := elapsed.Seconds()
	// Packets are not counted directly, so they are inferred from the bytes the
	// application handed over and the size the loop packs into one packet. It
	// is an upper bound on payload and so a lower bound on packets.
	mtu := after.CurrentMTU
	t.Logf("offered    %8.2f MB/s", float64(offered)/secs/(1<<20))
	t.Logf("loop iter  %8.0f/s (%d)", float64(loops)/secs, loops)
	if mtu > 0 {
		t.Logf("implied packets %8.0f/s at mtu %d", float64(offered)/float64(mtu)/secs, mtu)
	}
	if loops == 0 {
		t.Fatal("the run loop did not iterate at all during the measurement window")
	}
}

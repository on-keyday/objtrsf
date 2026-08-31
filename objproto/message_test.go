package objproto

import (
	"log/slog"
	"testing"
)

// SendMessage has two paths — a direct channel send when the reader is
// keeping up, and a goroutine when the channel is full — and a burst uses
// both. The two can complete in either order relative to each other, so the
// seqNum reordering is what has to hold the sequence together. This drives a
// burst far larger than the buffer, from one caller, the way the socket
// reader does, and checks that every message arrives exactly once in the
// order it was sent.
func TestSendMessageOrderAcrossBothPaths(t *testing.T) {
	const (
		buffer = 4
		count  = 5000
	)
	c := NewMessageChannel(buffer, slog.New(slog.DiscardHandler))
	t.Cleanup(c.CloseChannel)

	go func() {
		for i := range count {
			// One caller, no synchronisation between sends: this is
			// receiveApplication's calling pattern.
			if err := c.SendMessage(Message{PacketNumber: uint64(i)}); err != nil {
				return
			}
		}
	}()

	for i := range count {
		msg, err := c.ReceiveMessage()
		if err != nil {
			t.Fatalf("message %d: %v", i, err)
		}
		if msg.PacketNumber != uint64(i) {
			t.Fatalf("out of order at %d: got packet number %d", i, msg.PacketNumber)
		}
	}
}

// A burst that never fills the channel must not spawn at all, which is the
// whole point of the fast path. Nothing exposes the goroutine count directly,
// so this asserts the observable consequence instead: with a buffer larger
// than the burst, every message is already in the channel by the time
// SendMessage returns, so a receiver that never blocks still gets all of them
// in order.
func TestSendMessageFastPathDeliversWithoutBlocking(t *testing.T) {
	const count = 64
	c := NewMessageChannel(count, slog.New(slog.DiscardHandler))
	t.Cleanup(c.CloseChannel)

	for i := range count {
		if err := c.SendMessage(Message{PacketNumber: uint64(i)}); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
	}
	if got := len(c.messageChan); got != count {
		t.Fatalf("fast path left %d of %d messages outside the channel; a "+
			"send that fits took the goroutine path", count-got, count)
	}
	for i := range count {
		msg, err := c.ReceiveMessage()
		if err != nil {
			t.Fatalf("message %d: %v", i, err)
		}
		if msg.PacketNumber != uint64(i) {
			t.Fatalf("out of order at %d: got packet number %d", i, msg.PacketNumber)
		}
	}
}

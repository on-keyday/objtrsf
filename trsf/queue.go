package trsf

import (
	"sync"
	"sync/atomic"
)

// pushReason says WHY something was put on a trigger queue.
//
// It exists because the wake counters attribute a park to the CHANNEL that
// ended it, and one channel is many-to-one: sendTrigger is pushed from ten
// places meaning at least five different things. "The park ended on the send
// trigger" therefore cannot distinguish "the application supplied more data"
// (the transport is starved) from "an ACK retired a range" (the window
// reopened) — opposite conclusions from one number, and the first reading of
// these counters drew the wrong one on a lossy path for exactly that reason.
//
// The rule the mistake teaches: attribute a counter where the DECISION is
// made, not where the event happens to pass through. armedPacer is counted
// inside nextWakeDeadline, which is why it meant the same thing on a clean
// path and a lossy one; wakeSend is counted at the select, which is why it did
// not.
type pushReason uint8

const (
	// pushOther is stream creation, a cancel and a peer flow-window update:
	// rare next to the rest, and none of them answers the app-versus-window
	// question. A requeued retransmit used to be folded in here and is not:
	// measured on a 2 ms netem path it was 4,966 of the interval's pushes
	// against 4,953 lost packets, so "other" was really "loss" wearing a name
	// that hid it — the uninterpretable bucket these reasons exist to remove.
	pushOther pushReason = iota
	// pushApp — the application supplied data or flagged EOF. This one, and
	// only this one, means the transport was waiting on its caller.
	pushApp
	// pushACK — an ACK retired a range, so the stream can send again. Means
	// the window was the constraint, not the application.
	pushACK
	// pushSelf — the run loop's own continuation: data is still buffered after
	// the packet it just built, or another packet took this iteration's slot.
	// Not waiting for anything external; the loop is cycling.
	pushSelf
	// pushCwnd — a stream parked in congestionBlocked was revived because
	// CanSend() reopened. The unambiguous "this was congestion-blocked" signal.
	pushCwnd
	// pushLoss — the loss detector gave up on a packet and its range went back
	// on the retransmit queue. One per LOST PACKET, not per congestion event:
	// detectLost retires many packets in one pass, so this can dwarf the loss
	// event count and is the honest measure of retransmission pressure.
	pushLoss
	numPushReasons
)

type trigger struct {
	notify chan struct{}
}

func newTrigger() *trigger {
	return &trigger{notify: make(chan struct{}, 1)}
}

type withTriggerQueue[T any] struct {
	m     sync.Mutex
	set   map[*T]struct{}
	queue []*T
	// pushes counts push EVENTS by reason, including the ones the dedupe
	// below drops. Deliberately: the question is what wanted the loop to run,
	// not how many times it woke, and those differ — an item already queued is
	// not re-enqueued and fires no notification. So pushes >= wakes, always,
	// and the two are not two views of one number.
	pushes [numPushReasons]atomic.Uint64
	*trigger
}

func newWithTriggerQueue[T any]() *withTriggerQueue[T] {
	return &withTriggerQueue[T]{
		trigger: newTrigger(),
	}
}

func (q *withTriggerQueue[T]) Len() int {
	q.m.Lock()
	defer q.m.Unlock()
	return len(q.queue)
}

// PushCounts reports the per-reason event counts. Only the send trigger's are
// read; the other queues carry the array and never look at it.
func (q *withTriggerQueue[T]) PushCounts() [numPushReasons]uint64 {
	var out [numPushReasons]uint64
	for i := range q.pushes {
		out[i] = q.pushes[i].Load()
	}
	return out
}

// Push enqueues without saying why. Kept for the queues whose pushes have only
// one meaning (recv, send actions, window updates, cancels); the send trigger
// uses PushBecause.
func (q *withTriggerQueue[T]) Push(s *T) { q.PushBecause(s, pushOther) }

func (q *withTriggerQueue[T]) PushBecause(s *T, why pushReason) {
	// Counted before the dedupe: an event that found the item already queued
	// still says what wanted the loop to run.
	q.pushes[why].Add(1)
	q.m.Lock()
	defer q.m.Unlock()
	if q.set == nil {
		q.set = make(map[*T]struct{})
	}
	if _, exists := q.set[s]; exists {
		return
	}
	q.set[s] = struct{}{}
	q.queue = append(q.queue, s)
	q.trigger.Notify()
}

func (q *trigger) Notify() {
	select {
	case q.notify <- struct{}{}:
	default:
	}
}

func (q *trigger) Notification() <-chan struct{} {
	return q.notify
}

func (q *withTriggerQueue[T]) Pop() *T {
	q.m.Lock()
	defer q.m.Unlock()
	if len(q.queue) == 0 {
		return nil
	}
	s := q.queue[0]
	q.queue[0] = nil // release the popped item; the backing array outlives the slice
	q.queue = q.queue[1:]
	delete(q.set, s)
	if len(q.queue) != 0 {
		q.trigger.Notify() // notify again if there are more items
	}
	return s
}

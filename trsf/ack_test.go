package trsf

import (
	"testing"
	"time"
)

func TestACKRange(t *testing.T) {
	testTarget := []Range{
		{Begin: 0, End: 10},
		{Begin: 20, End: 50},
		{Begin: 60, End: 100},
	}
	const held = 7500 * time.Microsecond
	obj, err := TransferACK(testTarget, held)
	if err != nil {
		t.Fatal(err)
	}
	ranges, ackDelay, err := ParseTransferACK(obj)
	if err != nil {
		t.Fatal(err)
	}
	// The delay round-trips at microsecond resolution, which is what the wire
	// carries: a sender that reads it back wrong would subtract the wrong
	// amount from every RTT sample rather than failing visibly.
	if ackDelay != held {
		t.Errorf("ack_delay = %v, want %v", ackDelay, held)
	}
	if len(ranges) != len(testTarget) {
		t.Fatalf("expected %d ranges, got %d", len(testTarget), len(ranges))
	}
	for i, r := range ranges {
		if r != testTarget[i] {
			t.Fatalf("expected range %v, got %v", testTarget[i], r)
		}
	}
}

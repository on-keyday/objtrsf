package trsf

import (
	"testing"
)

func TestACKRange(t *testing.T) {
	testTarget := []Range{
		{Begin: 0, End: 10},
		{Begin: 20, End: 50},
		{Begin: 60, End: 100},
	}
	obj, err := TransferACK(testTarget)
	if err != nil {
		t.Fatal(err)
	}
	ranges, err := ParseTransferACK(obj)
	if err != nil {
		t.Fatal(err)
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

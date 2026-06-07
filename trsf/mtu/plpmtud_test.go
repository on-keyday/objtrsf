package mtu_test

import (
	"testing"
	"time"

	"github.com/on-keyday/objtrsf/trsf/mtu"
)

func TestMTUD(t *testing.T) {
	sizes := []int{1200, 1201, 1350, 1425, 1462, 1481, 1490, 1495, 1498, 1499, 1500}
	doProbeUntil := func(expected int) {
		i := 0
		p := mtu.NewMTUTracker(1200, 1500, 10*time.Second)
		for {
			if i > 9 {
				t.Errorf("failed to converge to %d, current %d", expected, p.CurrentMTU())
				return
			}
			size := p.Probe(time.Now())
			if size == -1 {
				if p.CurrentMTU() != expected {
					t.Errorf("expected %d, got %d", expected, p.CurrentMTU())
				}
				return
			}
			if size > expected { // simulate loss
				p.OnLost(time.Now())
				got := p.Probe(time.Now())
				if got != size {
					t.Errorf("after loss, expected probe %d, got %d", size, got)
				}
				p.OnLost(time.Now())
				if got2 := p.Probe(time.Now()); got2 != size {
					t.Errorf("after 2nd loss, expected probe %d, got %d", size, got2)
				}
				p.OnLost(time.Now())
			} else {
				p.OnACK(time.Now())
			}
			i++
		}
	}
	for _, size := range sizes {
		doProbeUntil(size)
	}
}

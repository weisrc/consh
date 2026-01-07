package consh

import (
	"hash/fnv"
	"math"
	"strconv"
	"testing"
)

func TestMovement(t *testing.T) {
	epsillon := 0.2
	loadFactor := 1.0 + epsillon
	paritionCount := 1024
	nodeCount := 64
	averageLoad := float64(paritionCount) / float64(nodeCount)
	maxMoves := int(math.Ceil(averageLoad / (epsillon * epsillon)))

	p := New(fnv.New64(), loadFactor).Partitioned(paritionCount)

	for i := range nodeCount {
		p.Add("node"+strconv.Itoa(i), 100)
	}

	allocations := p.Allocations()

	check := func() {
		newAllocations := p.Allocations()
		moves := 0
		for i := range paritionCount {
			if allocations[i] != newAllocations[i] {
				moves++
			}
		}
		allocations = newAllocations
		if moves > maxMoves {
			t.Errorf("expected less than %d, got %d", maxMoves, moves)
		}
	}

	for i := range 10 {
		p.Add("node_new"+strconv.Itoa(i), 100)
		check()
	}

	for i := range 10 {
		p.Remove("node" + strconv.Itoa(i))
		check()
	}
}

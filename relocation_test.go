package consh

import (
	"hash/fnv"
	"math"
	"strconv"
	"testing"
)

func countRelocations(oldAlloc, newAlloc []*Node) int {
	relocations := 0
	for i := range oldAlloc {
		if oldAlloc[i] != newAlloc[i] {
			relocations++
		}
	}
	return relocations
}

func TestRelocation(t *testing.T) {
	epsillon := 0.2
	loadFactor := 1.0 + epsillon
	paritionCount := 1024
	nodeCount := 64
	averageLoad := float64(paritionCount) / float64(nodeCount)

	// expected upper bound on the number of relocations
	maxRelocations := int(math.Ceil(averageLoad / (epsillon * epsillon)))

	p := New(fnv.New64(), loadFactor).Partitioned(paritionCount)

	for i := range nodeCount {
		p.Add("node"+strconv.Itoa(i), 100)
	}

	allocations := p.Allocations()

	check := func() {
		newAllocations := p.Allocations()
		relocations := countRelocations(allocations, newAllocations)
		allocations = newAllocations

		if relocations > maxRelocations {
			t.Errorf("expected less than %d relocations, got %d", maxRelocations, relocations)
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

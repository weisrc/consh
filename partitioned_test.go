package consh

import (
	"hash/fnv"
	"math"
	"strconv"
	"testing"
)

func TestPartitionSet(t *testing.T) {
	partitionCount := 1024
	maxDifference := 100

	p := NewPartitioned(1, fnv.New64(), partitionCount)

	p.Add("node1", 100)
	p.Add("node2", 200)

	set1 := p.PartitionSet("node1")
	set2 := p.PartitionSet("node2")

	if len(set1)+len(set2) != partitionCount {
		t.Errorf("expected all partitions to be assigned, got %d + %d", len(set1), len(set2))
	}

	if math.Abs(float64(len(set1)*2-len(set2))) > float64(maxDifference) {
		t.Errorf("expected partitions to be fairly distributed, got %d and %d", len(set1), len(set2))
	}

	p.Remove("node1")
	set2 = p.PartitionSet("node2")
	if len(set2) != partitionCount {
		t.Errorf("expected all partitions to be assigned to node2, got %d", len(set2))
	}

	p.Add("node3", 100)
	set2 = p.PartitionSet("node2")
	set3 := p.PartitionSet("node3")

	if len(set2)+len(set3) != partitionCount {
		t.Errorf("expected all partitions to be assigned after re-adding node, got %d + %d", len(set2), len(set3))
	}

	if math.Abs(float64(len(set2)-len(set3)*2)) > float64(maxDifference) {
		t.Errorf("expected partitions to be fairly distributed after re-adding node, got %d and %d", len(set2), len(set3))
	}
}

func BenchmarkAddRemove(b *testing.B) {
	p := NewPartitioned(1.25, fnv.New64(), 23)

	b.ResetTimer()

	for i := 0; b.Loop(); i++ {
		nodeId := "node" + strconv.Itoa(i)
		p.Add(nodeId, 20)
		p.Allocations()
		p.Remove(nodeId)
		p.Allocations()
	}
}

func BenchmarkLocateKey(b *testing.B) {
	p := NewPartitioned(1.25, fnv.New64(), 23)
	p.Add("nodeA", 20)
	p.Add("nodeB", 20)

	b.ResetTimer()

	for i := 0; b.Loop(); i++ {
		_ = p.LocateKey([]byte("key" + strconv.Itoa(i)))
	}
}

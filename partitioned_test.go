package consh

import (
	"hash/fnv"
	"math"
	"strconv"
	"testing"
)

func TestPartitions(t *testing.T) {
	partitionCount := 1024
	maxDifference := 100

	p := New(fnv.New64(), 1.25).Partitioned(partitionCount)

	p.Add("node1", 100)
	p.Add("node2", 200)

	set1 := p.Partitions("node1")
	set2 := p.Partitions("node2")

	if len(set1)+len(set2) != partitionCount {
		t.Errorf("expected all partitions to be assigned, got %d + %d", len(set1), len(set2))
	}

	if math.Abs(float64(len(set1)*2-len(set2))) > float64(maxDifference) {
		t.Errorf("expected partitions to be fairly distributed, got %d and %d", len(set1), len(set2))
	}

	p.Remove("node1")
	set2 = p.Partitions("node2")
	if len(set2) != partitionCount {
		t.Errorf("expected all partitions to be assigned to node2, got %d", len(set2))
	}

	p.Add("node3", 100)
	set2 = p.Partitions("node2")
	set3 := p.Partitions("node3")

	if len(set2)+len(set3) != partitionCount {
		t.Errorf("expected all partitions to be assigned after re-adding node, got %d + %d", len(set2), len(set3))
	}

	if math.Abs(float64(len(set2)-len(set3)*2)) > float64(maxDifference) {
		t.Errorf("expected partitions to be fairly distributed after re-adding node, got %d and %d", len(set2), len(set3))
	}
}

func TestLocateN(t *testing.T) {
	p := New(fnv.New64(), 1.25).Partitioned(128)
	p.Add("node1", 3)
	p.Add("node2", 3)
	p.Add("node3", 3)

	nodes := p.LocateN("mykey", 2)
	if len(nodes) != 2 {
		t.Errorf("expected 2 nodes, got %d", len(nodes))
	}
	if nodes[0] == nodes[1] {
		t.Errorf("expected different nodes, got the same node '%s'", nodes[0].Key)
	}

	if nodes[0] != p.Locate("mykey") {
		t.Errorf("expected first located node to match Locate result, got '%s' and '%s'", nodes[0].Key, p.Locate("mykey").Key)
	}

	nodes = p.LocateN("mykey", 5)
	if len(nodes) != 3 {
		t.Errorf("expected 3 nodes, got %d", len(nodes))
	}

	nodes = p.LocateN("mykey", 0)
	if len(nodes) != 0 {
		t.Errorf("expected 0 nodes, got %d", len(nodes))
	}
}

func BenchmarkAddRemove(b *testing.B) {
	p := New(fnv.New64(), 1.25).Partitioned(100)

	b.ResetTimer()

	for i := 0; b.Loop(); i++ {
		nodeId := "node" + strconv.Itoa(i)
		p.Add(nodeId, 20)
		p.Allocations()
		p.Remove(nodeId)
		p.Allocations()
	}
}

func BenchmarkLocate(b *testing.B) {
	p := New(fnv.New64(), 1.25).Partitioned(100)
	p.Add("nodeA", 20)
	p.Add("nodeB", 20)

	b.ResetTimer()

	for i := 0; b.Loop(); i++ {
		_ = p.Locate("key" + strconv.Itoa(i))
	}
}

func BenchmarkLocateN(b *testing.B) {
	p := New(fnv.New64(), 1.25).Partitioned(100)

	for i := range 10 {
		p.Add("node"+strconv.Itoa(i), 20)
	}

	b.ResetTimer()

	for i := 0; b.Loop(); i++ {
		_ = p.LocateN("key"+strconv.Itoa(i), 3)
	}
}

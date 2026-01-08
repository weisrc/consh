package consh

import (
	"hash/fnv"
	"math"
	"testing"
)

func TestPartitionedList(t *testing.T) {
	p := New(fnv.New64(), 1.25).Partitioned(128)
	node1 := p.Add("node1", 3)
	node2 := p.Add("node2", 5)

	nodes := p.List()
	if len(nodes) != 2 {
		t.Errorf("expected 2 nodes, got %d", len(nodes))
	}
	if nodes[0] != node1 && nodes[1] != node1 {
		t.Errorf("expected node1 to be in the list")
	}
	if nodes[0] != node2 && nodes[1] != node2 {
		t.Errorf("expected node2 to be in the list")
	}
}

func TestPartitionedPartitions(t *testing.T) {
	partitionCount := 1024
	maxDifference := 100

	p := New(fnv.New64(), 1.25).Partitioned(partitionCount)

	node1 := p.Add("node1", 100)
	node2 := p.Add("node2", 200)

	set1 := p.Partitions(node1)
	set2 := p.Partitions(node2)

	if len(set1)+len(set2) != partitionCount {
		t.Errorf("expected all partitions to be assigned, got %d + %d", len(set1), len(set2))
	}

	if math.Abs(float64(len(set1)*2-len(set2))) > float64(maxDifference) {
		t.Errorf("expected partitions to be fairly distributed, got %d and %d", len(set1), len(set2))
	}

	p.Remove("node1")
	set2 = p.Partitions(p.Get("node2"))
	if len(set2) != partitionCount {
		t.Errorf("expected all partitions to be assigned to node2, got %d", len(set2))
	}

	node3 := p.Add("node3", 100)
	set2 = p.Partitions(node2)
	set3 := p.Partitions(node3)

	if len(set2)+len(set3) != partitionCount {
		t.Errorf("expected all partitions to be assigned after re-adding node, got %d + %d", len(set2), len(set3))
	}

	if math.Abs(float64(len(set2)-len(set3)*2)) > float64(maxDifference) {
		t.Errorf("expected partitions to be fairly distributed after re-adding node, got %d and %d", len(set2), len(set3))
	}
}

func TestPartitionedIndexByHash(t *testing.T) {
	p := New(fnv.New64(), 1.25).Partitioned(1)

	index := p.Index("mykey")
	if index != 0 {
		t.Errorf("expected index 0 for 1 partition, got %d", index)
	}
}

func TestPartitionedLocateN(t *testing.T) {
	p := New(fnv.New64(), 1.25).Partitioned(128)
	p.Add("node1", 3)
	p.Add("node2", 3)
	p.Add("node3", 3)

	nodes := p.LocateN("mykey", 2)
	if len(nodes) != 2 {
		t.Errorf("expected 2 nodes, got %d", len(nodes))
	}
	if nodes[0] == nodes[1] {
		t.Errorf("expected different nodes, got the same node '%s'", nodes[0].name)
	}

	if nodes[0] != p.Locate("mykey") {
		t.Errorf("expected first located node to match Locate result, got '%s' and '%s'", nodes[0].name, p.Locate("mykey").name)
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

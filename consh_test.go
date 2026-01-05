package consh

import (
	"hash/fnv"
	"testing"
)

func TestAdd(t *testing.T) {
	consh := New(1.5, fnv.New64())
	consh.Add("node1", 3)
	nodes := consh.List()
	if len(nodes) != 1 {
		t.Errorf("expected 1 node, got %d", len(nodes))
	}
	if nodes[0].Key != "node1" {
		t.Errorf("expected node id 'node1', got '%s'", nodes[0].Key)
	}
	if nodes[0].Load != 0 {
		t.Errorf("expected node load 0, got %d", nodes[0].Load)
	}
}

func TestRemove(t *testing.T) {
	consh := New(1.5, fnv.New64())
	consh.Add("node1", 3)
	consh.Remove("node1")
	nodes := consh.List()
	if len(nodes) != 0 {
		t.Errorf("expected 0 nodes, got %d", len(nodes))
	}
}

func TestMapAllocate(t *testing.T) {
	consh := New(1.5, fnv.New64())
	consh.Add("node1", 3)

	hashes := make([]uint64, 10)
	for i := range hashes {
		hashes[i] = uint64(i * 100)
	}

	assignments := consh.MapAllocateHashes(hashes)
	if len(assignments) != 10 {
		t.Errorf("expected 10 assignments, got %d", len(assignments))
	}
	for _, node := range assignments {
		if node.Key != "node1" {
			t.Errorf("expected all assignments to be 'node1', got '%s'", node.Key)
		}
	}
	consh.Remove("node1")
	assignments = consh.MapAllocateHashes(hashes)
	for _, node := range assignments {
		if node != nil {
			t.Errorf("expected all assignments to be nil, got '%s'", node.Key)
		}
	}
}

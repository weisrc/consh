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

func TestMapAllocateKeys(t *testing.T) {
	consh := New(1.5, fnv.New64())
	consh.Add("node1", 3)

	keys := make([][]byte, 10)
	for i := 0; i < 10; i++ {
		keys[i] = []byte{byte(i)}
	}

	allocations := consh.MapAllocateKeys(keys)
	if len(allocations) != 10 {
		t.Errorf("expected 10 assignments, got %d", len(allocations))
	}
	for _, node := range allocations {
		if node.Key != "node1" {
			t.Errorf("expected all assignments to be 'node1', got '%s'", node.Key)
		}
	}
	consh.Remove("node1")
	allocations = consh.MapAllocateKeys(keys)
	for _, node := range allocations {
		if node != nil {
			t.Errorf("expected all assignments to be nil, got '%s'", node.Key)
		}
	}
}

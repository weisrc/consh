package consh

import (
	"hash/fnv"
	"strconv"
	"testing"
)

func TestConshAdd(t *testing.T) {
	consh := New(fnv.New64(), 1.25)
	consh.Add("node1", 3)
	nodes := consh.List()
	if len(nodes) != 1 {
		t.Errorf("expected 1 node, got %d", len(nodes))
	}
	if nodes[0].key != "node1" {
		t.Errorf("expected node id 'node1', got '%s'", nodes[0].key)
	}
	if nodes[0].load != 0 {
		t.Errorf("expected node load 0, got %d", nodes[0].load)
	}
}

func TestConshRemove(t *testing.T) {
	consh := New(fnv.New64(), 1.25)
	consh.Add("node1", 3)
	consh.Remove("node1")
	nodes := consh.List()
	if len(nodes) != 0 {
		t.Errorf("expected 0 nodes, got %d", len(nodes))
	}
}

func TestConshAllocateMany(t *testing.T) {
	consh := New(fnv.New64(), 1.25)
	consh.Add("node1", 3)

	keys := []string{}
	for i := range 10 {
		keys = append(keys, "key"+strconv.Itoa(i))
	}

	allocations := consh.AllocateMany(keys)
	if len(allocations) != 10 {
		t.Errorf("expected 10 assignments, got %d", len(allocations))
	}
	for _, node := range allocations {
		if node.key != "node1" {
			t.Errorf("expected all assignments to be 'node1', got '%s'", node.key)
		}
	}
	consh.Remove("node1")
	allocations = consh.AllocateMany(keys)
	for _, node := range allocations {
		if node != nil {
			t.Errorf("expected all assignments to be nil, got '%s'", node.key)
		}
	}
}

func TestConshLocateN(t *testing.T) {
	consh := New(fnv.New64(), 1.25)
	consh.Add("node1", 3)
	consh.Add("node2", 3)
	consh.Add("node3", 3)

	consh.AllocateMany([]string{"mykey", "anotherkey"})

	nodes := consh.LocateN("mykey", 2)
	if len(nodes) != 2 {
		t.Errorf("expected 2 nodes, got %d", len(nodes))
	}
	if nodes[0] == nodes[1] {
		t.Errorf("expected different nodes, got the same node '%s'", nodes[0].key)
	}

	if nodes[0] != consh.Locate("mykey") {
		t.Errorf("expected first located node to match Locate result, got '%s' and '%s'", nodes[0].key, consh.Locate("mykey").key)
	}

	nodes = consh.LocateN("mykey", 5)
	if len(nodes) != 3 {
		t.Errorf("expected 3 nodes, got %d", len(nodes))
	}

	nodes = consh.LocateN("mykey", 0)
	if len(nodes) != 0 {
		t.Errorf("expected 0 nodes, got %d", len(nodes))
	}
}

package consh

// Physical node in the consistent hashing ring
type Node struct {
	key     string
	weight  int
	load    int
	maxLoad int
	removed bool
}

// Get the key of the physical node.
func (n *Node) Key() string {
	return n.key
}

// Get the weight of the physical node.
func (n *Node) Weight() int {
	return n.weight
}

// Get the current load of the physical node.
func (n *Node) Load() int {
	return n.load
}

// Get the maximum load of the physical node.
func (n *Node) MaxLoad() int {
	return n.maxLoad
}

// Get a snapshot of the physical node's state.
func (n *Node) Snapshot() NodeSnapshot {
	return NodeSnapshot{
		Key:     n.key,
		Weight:  n.weight,
		Load:    n.load,
		MaxLoad: n.maxLoad,
	}
}

// Snapshot of a Node's state
type NodeSnapshot struct {
	Key     string
	Weight  int
	Load    int
	MaxLoad int
}

// Virtual node in the consistent hashing ring
type VirtualNode struct {
	hash uint64
	node *Node
}

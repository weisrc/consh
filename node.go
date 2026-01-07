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

// Virtual node in the consistent hashing ring
type VirtualNode struct {
	hash uint64
	node *Node
}

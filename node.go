package consh

// Physical node in the consistent hashing ring
type Node struct {
	name    string
	weight  int
	load    int
	maxLoad int
	removed bool
}

// Get the name of the physical node.
func (n *Node) Name() string {
	return n.name
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
type virtualNode struct {
	hash uint64
	node *Node
}

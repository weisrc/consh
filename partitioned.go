package consh

import (
	"encoding/binary"
)

// Partitioned consistent hashing ring
type Partitioned struct {
	consh       *Consh
	hashes      []uint64
	allocations []*Node
}

// Create a new Partitioned consistent hashing ring with n partitions.
func NewPartitioned(consh *Consh, n int) *Partitioned {
	hashes := make([]uint64, n)

	for i := range n {
		consh.hasher.Reset()
		binary.Write(consh.hasher, binary.LittleEndian, uint32(i))
		hashes[i] = consh.hasher.Sum64()
	}

	return &Partitioned{
		consh:       consh,
		hashes:      hashes,
		allocations: nil,
	}
}

// Add a new physical node with name and a weight.
// See Consh.Add for additional details.
func (p *Partitioned) Add(name string, weight int) *Node {
	p.allocations = nil
	return p.consh.Add(name, weight)
}

// Remove a physical node by its name.
func (p *Partitioned) Remove(name string) *Node {
	p.allocations = nil
	return p.consh.Remove(name)
}

// Get a physical node with the given name.
func (p *Partitioned) Get(name string) *Node {
	return p.consh.Get(name)
}

// List all physical nodes.
func (p *Partitioned) List() []*Node {
	return p.consh.List()
}

// Get the partition index for a resource by its hash.
func (p *Partitioned) IndexByHash(hash uint64) int {
	return int(hash % uint64(len(p.hashes)))
}

// Get the partition index for a resource by its key.
func (p *Partitioned) Index(key string) int {
	return p.IndexByHash(p.consh.HashString(key))
}

// Get the owner physical node for a given partition index.
func (p *Partitioned) Owner(index int) *Node {
	return p.Allocations()[index]
}

// Locate the physical node for a resource by its hash.
func (p *Partitioned) LocateByHash(hash uint64) *Node {
	return p.Owner(p.IndexByHash(hash))
}

// Locate N physical nodes for a resource by its hash.
func (p *Partitioned) LocateNByHash(hash uint64, n int) []*Node {
	index := p.IndexByHash(hash)
	nodes := make([]*Node, 0, len(p.consh.nodes))
	seen := make(map[*Node]struct{})

	for i := 0; len(nodes) < n && i < len(p.hashes); i++ {
		if index >= len(p.hashes) {
			index = 0
		}
		node := p.Owner(index)
		index++
		if _, exists := seen[node]; !exists && node != nil {
			seen[node] = struct{}{}
			nodes = append(nodes, node)
		}
	}

	return nodes
}

// Locate the physical node for a resource by its key.
func (p *Partitioned) Locate(key string) *Node {
	return p.LocateByHash(p.consh.HashString(key))
}

// Locate N physical nodes for a resource by its key.
func (p *Partitioned) LocateN(key string, n int) []*Node {
	return p.LocateNByHash(p.consh.HashString(key), n)
}

// Get all physical node allocations for partitions.
// The i-th element corresponds to the owner of the i-th partition.
func (p *Partitioned) Allocations() []*Node {
	if p.allocations != nil {
		return p.allocations
	}

	p.allocations = p.consh.AllocateManyByHash(p.hashes)
	return p.allocations
}

// Get the set of partition indices owned by the physical node.
func (p *Partitioned) Partitions(node *Node) map[int]struct{} {
	partitions := make(map[int]struct{})
	for i, assignedNode := range p.Allocations() {
		if node == assignedNode {
			partitions[i] = struct{}{}
		}
	}
	return partitions
}

package consh

import (
	"encoding/binary"
)

type PartitionedConsh struct {
	consh       Consh
	hashes      []uint64
	allocations []*Node
}

func NewPartitioned(consh Consh, n int) PartitionedConsh {
	hashes := make([]uint64, n)

	for i := range n {
		consh.hasher.Reset()
		binary.Write(consh.hasher, binary.LittleEndian, uint32(i))
		hashes[i] = consh.hasher.Sum64()
	}

	return PartitionedConsh{
		consh:       consh,
		hashes:      hashes,
		allocations: nil,
	}
}

func (p *PartitionedConsh) Add(key string, weight int) {
	p.consh.Add(key, weight)
	p.allocations = nil
}

func (p *PartitionedConsh) Remove(key string) {
	p.consh.Remove(key)
	p.allocations = nil
}

func (p *PartitionedConsh) Get(key string) *Node {
	return p.consh.Get(key)
}

func (p *PartitionedConsh) List() []*Node {
	return p.consh.List()
}

func (p *PartitionedConsh) IndexByHash(hash uint64) int {
	return int(hash % uint64(len(p.hashes)))
}

func (p *PartitionedConsh) Index(key string) int {
	return p.IndexByHash(p.consh.HashString(key))
}

func (p *PartitionedConsh) Owner(index int) *Node {
	return p.Allocations()[index]
}

func (p *PartitionedConsh) LocateByHash(hash uint64) *Node {
	return p.Owner(p.IndexByHash(hash))
}

func (p *PartitionedConsh) LocateNByHash(hash uint64, n int) []*Node {
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

func (p *PartitionedConsh) Locate(key string) *Node {
	return p.LocateByHash(p.consh.HashString(key))
}

func (p *PartitionedConsh) LocateN(key string, n int) []*Node {
	return p.LocateNByHash(p.consh.HashString(key), n)
}

func (p *PartitionedConsh) Allocations() []*Node {
	if p.allocations != nil {
		return p.allocations
	}

	p.allocations = p.consh.AllocateManyByHash(p.hashes)
	return p.allocations
}

func (p *PartitionedConsh) OwnedPartitions(key string) map[int]struct{} {
	partitions := make(map[int]struct{})
	node := p.consh.Get(key)
	if node == nil {
		return partitions
	}
	for i, assignedNode := range p.Allocations() {
		if node == assignedNode {
			partitions[i] = struct{}{}
		}
	}
	return partitions
}

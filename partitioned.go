package consh

import (
	"encoding/binary"
)

type Partitioned struct {
	consh       *Consh
	hashes      []uint64
	allocations []*Node
}

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

func (p *Partitioned) Add(key string, weight int) {
	p.consh.Add(key, weight)
	p.allocations = nil
}

func (p *Partitioned) Remove(key string) {
	p.consh.Remove(key)
	p.allocations = nil
}

func (p *Partitioned) Get(key string) *Node {
	return p.consh.Get(key)
}

func (p *Partitioned) List() []*Node {
	return p.consh.List()
}

func (p *Partitioned) IndexByHash(hash uint64) int {
	return int(hash % uint64(len(p.hashes)))
}

func (p *Partitioned) Index(key string) int {
	return p.IndexByHash(p.consh.HashString(key))
}

func (p *Partitioned) Owner(index int) *Node {
	return p.Allocations()[index]
}

func (p *Partitioned) LocateByHash(hash uint64) *Node {
	return p.Owner(p.IndexByHash(hash))
}

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

func (p *Partitioned) Locate(key string) *Node {
	return p.LocateByHash(p.consh.HashString(key))
}

func (p *Partitioned) LocateN(key string, n int) []*Node {
	return p.LocateNByHash(p.consh.HashString(key), n)
}

func (p *Partitioned) Allocations() []*Node {
	if p.allocations != nil {
		return p.allocations
	}

	p.allocations = p.consh.AllocateManyByHash(p.hashes)
	return p.allocations
}

func (p *Partitioned) OwnedPartitions(key string) map[int]struct{} {
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

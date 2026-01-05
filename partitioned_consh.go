package consh

import (
	"encoding/binary"
)

type PartitionedConsh struct {
	consh       Consh
	hashes      []uint64
	allocations []*Node
}

func NewPartitionedConsh(consh Consh, partitionCount int) PartitionedConsh {
	hashes := make([]uint64, partitionCount)

	for i := range partitionCount {
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

func (p *PartitionedConsh) Add(nodeId string, nodeWeight int) {
	p.consh.Add(nodeId, nodeWeight)
	p.allocations = nil
}

func (p *PartitionedConsh) Remove(nodeId string) {
	p.consh.Remove(nodeId)
	p.allocations = nil
}

func (p *PartitionedConsh) Get(nodeId string) *Node {
	return p.consh.Get(nodeId)
}

func (p *PartitionedConsh) List() []*Node {
	return p.consh.List()
}

func (p *PartitionedConsh) PartitionByHash(resourceHash uint64) int {
	return int(resourceHash % uint64(len(p.hashes)))
}

func (p *PartitionedConsh) PartitionByKey(resourceKey []byte) int {
	return p.PartitionByHash(p.consh.hash(resourceKey))
}

func (p *PartitionedConsh) PartitionOwner(partitionKey int) *Node {
	return p.Allocations()[partitionKey]
}

func (p *PartitionedConsh) LocateHash(resourceHash uint64) *Node {
	return p.PartitionOwner(p.PartitionByHash(resourceHash))
}

func (p *PartitionedConsh) LocateKey(resourceKey []byte) *Node {
	return p.LocateHash(p.consh.hash(resourceKey))
}

func (p *PartitionedConsh) Allocations() []*Node {
	if p.allocations != nil {
		return p.allocations
	}

	p.allocations = p.consh.MapAllocateHashes(p.hashes)
	return p.allocations
}

func (p *PartitionedConsh) PartitionSet(nodeKey string) map[int]struct{} {
	partitions := make(map[int]struct{})
	node := p.consh.Get(nodeKey)
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

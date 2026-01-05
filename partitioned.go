package consh

import (
	"encoding/binary"
	"hash"
)

type Partitioned struct {
	consh       Consh
	hashes      []uint64
	allocations []*Node
}

func NewPartitioned(loadFactor float64, hasher hash.Hash64, partitionCount int) Partitioned {
	hashes := make([]uint64, partitionCount)
	for i := range partitionCount {
		hasher.Reset()
		binary.Write(hasher, binary.LittleEndian, uint32(i))
		hashes[i] = hasher.Sum64()
	}

	return Partitioned{
		consh:       New(loadFactor, hasher),
		hashes:      hashes,
		allocations: nil,
	}
}

func (p *Partitioned) Add(nodeId string, nodeWeight int) {
	p.consh.Add(nodeId, nodeWeight)
	p.allocations = nil
}

func (p *Partitioned) Remove(nodeId string) {
	p.consh.Remove(nodeId)
	p.allocations = nil
}

func (p *Partitioned) Get(nodeId string) *Node {
	return p.consh.Get(nodeId)
}

func (p *Partitioned) List() []*Node {
	return p.consh.List()
}

func (p *Partitioned) PartitionByHash(resourceHash uint64) int {
	return int(resourceHash % uint64(len(p.hashes)))
}

func (p *Partitioned) PartitionByKey(resourceKey []byte) int {
	return p.PartitionByHash(p.consh.hash(resourceKey))
}

func (p *Partitioned) PartitionOwner(partitionKey int) *Node {
	return p.Allocations()[partitionKey]
}

func (p *Partitioned) LocateHash(resourceHash uint64) *Node {
	return p.PartitionOwner(p.PartitionByHash(resourceHash))
}

func (p *Partitioned) LocateKey(resourceKey []byte) *Node {
	return p.LocateHash(p.consh.hash(resourceKey))
}

func (p *Partitioned) Allocations() []*Node {
	if p.allocations != nil {
		return p.allocations
	}

	p.allocations = p.consh.MapAllocateHashes(p.hashes)
	return p.allocations
}

func (p *Partitioned) PartitionSet(nodeKey string) map[int]struct{} {
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

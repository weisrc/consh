package consh

import (
	"hash"
	"math"
	"sort"
)

type Node struct {
	Key    string
	Weight int
	Load   int
}

type VirtualNode struct {
	hash uint64
	node *Node
}

type Consh struct {
	loadFactor  float64
	hasher      hash.Hash64
	ring        []VirtualNode
	nodeMap     map[string]*Node
	baseMaxLoad float64
}

func New(loadFactor float64, hasher hash.Hash64) Consh {
	return Consh{
		loadFactor:  loadFactor,
		hasher:      hasher,
		ring:        []VirtualNode{},
		nodeMap:     map[string]*Node{},
		baseMaxLoad: 0,
	}
}

func (c Consh) Partitioned(partitionCount int) PartitionedConsh {
	return NewPartitionedConsh(c, partitionCount)
}

func (c *Consh) Add(nodeKey string, nodeWeight int) bool {
	if nodeWeight <= 0 || nodeWeight > math.MaxUint16 {
		panic("weight must be between 1 and 65535")
	}

	if _, exists := c.nodeMap[nodeKey]; exists {
		return false
	}

	node := &Node{
		Key:    nodeKey,
		Weight: nodeWeight,
		Load:   0,
	}

	c.nodeMap[nodeKey] = node

	c.hasher.Reset()
	c.hasher.Write([]byte(nodeKey))

	for i := range nodeWeight {
		c.hasher.Write([]byte{byte(i), byte(i >> 8)})
		hash := c.hasher.Sum64()
		vNode := VirtualNode{
			hash: hash,
			node: node,
		}
		c.ring = append(c.ring, vNode)
	}

	return true
}

func (c *Consh) Get(nodeKey string) *Node {
	return c.nodeMap[nodeKey]
}

func (c *Consh) List() []*Node {
	nodes := make([]*Node, 0, len(c.nodeMap))
	for _, node := range c.nodeMap {
		nodes = append(nodes, node)
	}
	return nodes
}

func (c *Consh) Remove(nodeKey string) bool {
	node, exists := c.nodeMap[nodeKey]
	if !exists {
		return false
	}
	delete(c.nodeMap, nodeKey)
	filtered := make([]VirtualNode, 0, len(c.ring))
	for _, vNode := range c.ring {
		if vNode.node != node {
			filtered = append(filtered, vNode)
		}
	}
	c.ring = filtered
	return true
}

func (c *Consh) Begin(totalLoad int) {
	c.baseMaxLoad = float64(totalLoad) * c.loadFactor / float64(len(c.ring))

	for _, node := range c.nodeMap {
		node.Load = 0
	}

	sort.Slice(c.ring, func(i, j int) bool {
		return c.ring[i].hash < c.ring[j].hash
	})
}

func (c *Consh) MapAllocateKeys(resourceKeys [][]byte) []*Node {
	mapped := make([]*Node, len(resourceKeys))

	if len(c.nodeMap) == 0 {
		return mapped
	}

	c.Begin(len(resourceKeys))

	for i, key := range resourceKeys {
		mapped[i] = c.AllocateKey(key)
	}

	return mapped
}

func (c *Consh) AllocateKey(resourceKey []byte) *Node {
	return c.AllocateHash(c.hash(resourceKey))
}

func (c *Consh) LocateKey(resourceKey []byte) *Node {
	return c.LocateHash(c.hash(resourceKey))
}

func (c *Consh) MapAllocateHashes(resourceHashes []uint64) []*Node {
	mapped := make([]*Node, len(resourceHashes))

	if len(c.nodeMap) == 0 {
		return mapped
	}

	c.Begin(len(resourceHashes))

	for i, hash := range resourceHashes {
		mapped[i] = c.AllocateHash(hash)
	}

	return mapped
}

func (c *Consh) AllocateHash(resourceHash uint64) *Node {
	node := c.LocateHash(resourceHash)
	if node == nil {
		panic("load factor too low")
	}
	node.Load++
	return node
}

func (c *Consh) LocateHash(resourceHash uint64) *Node {
	index := sort.Search(len(c.ring), func(j int) bool {
		return c.ring[j].hash >= resourceHash
	})
	if index >= len(c.ring) {
		index = 0
	}
	for i := 0; i < len(c.ring); i++ {
		vNode := c.ring[(index+i)%len(c.ring)]
		maxLoad := int(c.baseMaxLoad * float64(vNode.node.Weight))
		if vNode.node.Load <= maxLoad {
			return vNode.node
		}
	}
	return nil
}

func (c *Consh) hash(data []byte) uint64 {
	c.hasher.Reset()
	c.hasher.Write(data)
	return c.hasher.Sum64()
}

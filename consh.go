package consh

import (
	"hash"
	"math"
	"sort"
)

type Node struct {
	Key     string
	Weight  int
	Load    int
	maxLoad int
	removed bool
}

type VirtualNode struct {
	hash uint64
	node *Node
}

type Consh struct {
	hasher      hash.Hash64
	loadFactor  float64
	ring        []VirtualNode
	nodes       map[string]*Node
	needsSort   bool
	needsFilter bool
}

func New(hasher hash.Hash64, loadFactor float64) *Consh {
	return &Consh{
		hasher:      hasher,
		loadFactor:  loadFactor,
		ring:        []VirtualNode{},
		nodes:       map[string]*Node{},
		needsSort:   false,
		needsFilter: false,
	}
}

func (c *Consh) Partitioned(n int) *Partitioned {
	return NewPartitioned(c, n)
}

func (c *Consh) Add(key string, weight int) bool {
	if weight <= 0 || weight > math.MaxUint16 {
		panic("weight must be between 1 and 65535")
	}

	if _, exists := c.nodes[key]; exists {
		return false
	}

	node := &Node{
		Key:     key,
		Weight:  weight,
		Load:    0,
		maxLoad: 0,
		removed: false,
	}

	c.nodes[key] = node

	c.hasher.Reset()
	c.hasher.Write([]byte(key))

	for i := range weight {
		c.hasher.Write([]byte{byte(i), byte(i >> 8)})
		c.ring = append(c.ring, VirtualNode{
			hash: c.hasher.Sum64(),
			node: node,
		})
	}

	c.needsSort = true
	return true
}

func (c *Consh) Get(key string) *Node {
	return c.nodes[key]
}

func (c *Consh) List() []*Node {
	nodes := make([]*Node, 0, len(c.nodes))
	for _, node := range c.nodes {
		nodes = append(nodes, node)
	}
	return nodes
}

func (c *Consh) Remove(key string) bool {
	node, exists := c.nodes[key]
	if !exists {
		return false
	}
	node.removed = true
	delete(c.nodes, key)

	c.needsFilter = true
	return true
}

func (c *Consh) Prepare(totalLoad int) {
	if c.needsFilter {
		newRing := c.ring[:0]
		for _, vNode := range c.ring {
			if !vNode.node.removed {
				newRing = append(newRing, vNode)
			}
		}
		c.ring = newRing
		c.needsFilter = false
	}

	if c.needsSort {
		sort.Slice(c.ring, func(i, j int) bool {
			return c.ring[i].hash < c.ring[j].hash
		})
		c.needsSort = false
	}

	baseMaxLoad := float64(totalLoad) * c.loadFactor / float64(len(c.ring))

	for _, node := range c.nodes {
		node.Load = 0
		node.maxLoad = int(math.Ceil(baseMaxLoad * float64(node.Weight)))
	}
}

func (c *Consh) AllocateMany(keys []string) []*Node {
	nodes := make([]*Node, len(keys))

	if len(c.nodes) == 0 {
		return nodes
	}

	c.Prepare(len(keys))

	for i, key := range keys {
		nodes[i] = c.Allocate(key)
	}

	return nodes
}

func (c *Consh) Allocate(key string) *Node {
	return c.AllocateByHash(c.HashString(key))
}

func (c *Consh) Locate(key string) *Node {
	return c.LocateByHash(c.HashString(key))
}

func (c *Consh) LocateN(key string, n int) []*Node {
	return c.LocateNByHash(c.HashString(key), n)
}

func (c *Consh) AllocateManyByHash(hashes []uint64) []*Node {
	nodes := make([]*Node, len(hashes))

	if len(c.nodes) == 0 {
		return nodes
	}

	c.Prepare(len(hashes))

	for i, hash := range hashes {
		nodes[i] = c.AllocateByHash(hash)
	}

	return nodes
}

func (c *Consh) AllocateByHash(hash uint64) *Node {
	node := c.LocateByHash(hash)
	if node == nil {
		panic("no available node found")
	}
	node.Load++
	return node
}

func (c *Consh) LocateByHash(hash uint64) *Node {
	index := sort.Search(len(c.ring), func(j int) bool {
		return c.ring[j].hash >= hash
	})

	for count := 0; count < len(c.ring); count++ {
		if index >= len(c.ring) {
			index = 0
		}
		vNode := c.ring[index]
		index++
		if vNode.node.Load < vNode.node.maxLoad {
			return vNode.node
		}
	}
	return nil
}

func (c *Consh) LocateNByHash(hash uint64, n int) []*Node {
	nodes := make([]*Node, 0, n)
	seen := make(map[*Node]struct{})

	index := sort.Search(len(c.ring), func(j int) bool {
		return c.ring[j].hash >= hash
	})

	for count := 0; count < len(c.ring) && len(nodes) < n; count++ {
		if index >= len(c.ring) {
			index = 0
		}
		vNode := c.ring[index]
		index++
		if _, exists := seen[vNode.node]; exists {
			continue
		}
		if len(nodes) != 0 || vNode.node.Load < vNode.node.maxLoad {
			nodes = append(nodes, vNode.node)
			seen[vNode.node] = struct{}{}
		}
	}
	return nodes
}

func (c *Consh) HashString(data string) uint64 {
	return c.Hash([]byte(data))
}

func (c *Consh) Hash(data []byte) uint64 {
	c.hasher.Reset()
	c.hasher.Write(data)
	return c.hasher.Sum64()
}

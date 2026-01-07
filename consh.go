package consh

import (
	"hash"
	"math"
	"sort"
)

// Consistent hashing ring
type Consh struct {
	hasher      hash.Hash64
	loadFactor  float64
	ring        []virtualNode
	nodes       map[string]*Node
	needsSort   bool
	needsFilter bool
}

// Create a new Consh instance.
// The inputed hasher instance should not be used elsewhere.
// Consh is not safe for concurrent use. Use `RWMutex` if needed.
func New(hasher hash.Hash64, loadFactor float64) *Consh {
	return &Consh{
		hasher:      hasher,
		loadFactor:  loadFactor,
		ring:        []virtualNode{},
		nodes:       map[string]*Node{},
		needsSort:   false,
		needsFilter: false,
	}
}

// Create a new Partitioned consistent hashing ring
func (c *Consh) Partitioned(n int) *Partitioned {
	return NewPartitioned(c, n)
}

// Add a new physical node with key and a weight.
// The weight determines the number of virtual nodes created for this physical node.
// The weight must be between 1 and 65535.
// Returns false if the node with the same key already exists.
func (c *Consh) Add(key string, weight int) bool {
	if weight <= 0 || weight > math.MaxUint16 {
		panic("weight must be between 1 and 65535")
	}

	if _, exists := c.nodes[key]; exists {
		return false
	}

	node := &Node{
		key:     key,
		weight:  weight,
		load:    0,
		maxLoad: 0,
		removed: false,
	}

	c.nodes[key] = node

	c.hasher.Reset()
	c.hasher.Write([]byte(key))

	for i := range weight {
		c.hasher.Write([]byte{byte(i), byte(i >> 8)})
		c.ring = append(c.ring, virtualNode{
			hash: c.hasher.Sum64(),
			node: node,
		})
	}

	c.needsSort = true
	return true
}

// Get a physical node by its key.
// Returns nil if the node does not exist.
func (c *Consh) Get(key string) *Node {
	return c.nodes[key]
}

// List all physical nodes.
func (c *Consh) List() []*Node {
	nodes := make([]*Node, 0, len(c.nodes))
	for _, node := range c.nodes {
		nodes = append(nodes, node)
	}
	return nodes
}

// Remove a physical node by its key.
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

// Prepare for allocations.
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
		node.load = 0
		node.maxLoad = int(math.Ceil(baseMaxLoad * float64(node.weight)))
	}
}

// Allocate multiple keys to their respective physical nodes.
// Returns the keys mapped to their allocated nodes.
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

// Allocate a key to its respective physical node.
// Must call Prepare before using.
func (c *Consh) Allocate(key string) *Node {
	return c.AllocateByHash(c.HashString(key))
}

// Locate the physical node for a key.
func (c *Consh) Locate(key string) *Node {
	return c.LocateByHash(c.HashString(key))
}

// Locate N physical nodes for a key.
func (c *Consh) LocateN(key string, n int) []*Node {
	return c.LocateNByHash(c.HashString(key), n)
}

// Allocate multiple hashes to their respective physical nodes.
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

// Allocate a hash to its respective physical node.
func (c *Consh) AllocateByHash(hash uint64) *Node {
	node := c.LocateByHash(hash)
	if node == nil {
		panic("no available node found")
	}
	node.load++
	return node
}

// Locate the physical node for a hash.
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
		if vNode.node.load < vNode.node.maxLoad {
			return vNode.node
		}
	}
	return nil
}

// Locate N physical nodes for a hash.
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
		if len(nodes) != 0 || vNode.node.load < vNode.node.maxLoad {
			nodes = append(nodes, vNode.node)
			seen[vNode.node] = struct{}{}
		}
	}
	return nodes
}

// Hash a string to uint64.
func (c *Consh) HashString(data string) uint64 {
	return c.Hash([]byte(data))
}

// Hash a byte slice to uint64.
func (c *Consh) Hash(data []byte) uint64 {
	c.hasher.Reset()
	c.hasher.Write(data)
	return c.hasher.Sum64()
}

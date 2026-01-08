package main

import (
	"fmt"
	"hash/fnv"
	"strconv"

	"github.com/weisrc/consh"
)

func main() {
	h := fnv.New64()       // should use xxhash for better distribution
	c := consh.New(h, 1.1) // create consh with load factor 1.1
	c.Add("node0", 100)    // add node0 with weight 100
	c.Add("node1", 100)    // the weight is the replication factor
	c.Add("node2", 200)    // this node is twice more powerful

	resources := make([]string, 100000) // create 100k resources

	for i := range len(resources) {
		resources[i] = strconv.Itoa(i)
	}

	allocations := c.AllocateMany(resources)

	for i := range resources {
		node := allocations[i]
		fmt.Printf("%s -> %s\n", resources[i], node.Name())
	}

	for _, node := range c.List() {
		fmt.Printf("%s has %d resources\n", node.Name(), node.Load())
	}

	// node0 has 25200 resources
	// node1 has 25900 resources
	// node2 has 48900 resources
}

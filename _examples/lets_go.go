package main

import (
	"fmt"
	"hash/fnv"
	"strconv"

	"github.com/weisrc/consh"
)

func main() {
	h := fnv.New64()       // use xxhash for better distribution
	c := consh.New(h, 1.1) // create consh with load factor 1.1
	c.Add("a", 100)        // add node0 with weight 20
	c.Add("b", 100)        // the weight is the replication factor
	c.Add("c", 200)        // node c is twice more powerful

	resources := make([]string, 10)

	for i := range len(resources) {
		resources[i] = strconv.Itoa(i)
	}

	allocations := c.AllocateMany(resources)

	for i := range resources {
		node := allocations[i]
		fmt.Printf("%s -> %s\n", resources[i], node.Key())
	}

	for _, node := range c.List() {
		fmt.Printf("node %s has %d resources\n", node.Key(), node.Load())
	}
}

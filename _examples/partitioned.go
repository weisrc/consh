package main

import (
	"fmt"
	"hash/fnv"

	"github.com/weisrc/consh"
)

func main() {
	c := consh.New(fnv.New64(), 1.25)
	p := c.Partitioned(1024) // partitioned into 1024 partitions

	nodeA := p.Add("nodeA", 100)
	nodeB := p.Add("nodeB", 200)

	setA := p.Partitions(nodeA) // get set owned by nodeA
	setB := p.Partitions(nodeB) // returns map[int]struct{}

	fmt.Printf("nodeA has %d partitions\n", len(setA))
	fmt.Printf("nodeB has %d partitions\n", len(setB))
	// nodeA has 314 partitions
	// nodeB has 710 partitions
}

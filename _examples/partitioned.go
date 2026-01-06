package main

import (
	"hash/fnv"

	"github.com/weisrc/consh"
)

func main() {
	c := consh.New(fnv.New64(), 1.25)
	p := c.Partitioned(1024) // partitioned into 1024 partitions

	p.Add("a", 100)
	p.Add("b", 200)

	setA := p.Partitions("a") // get set owned by a
	setB := p.Partitions("b") // returns map[int]struct{}

	println("node a has partitions:", len(setA))
	println("node b has partitions:", len(setB))
}

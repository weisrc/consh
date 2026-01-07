package consh

import (
	"hash/fnv"
	"strconv"
	"testing"
)

func BenchmarkAddRemove(b *testing.B) {
	p := New(fnv.New64(), 1.25).Partitioned(100)

	b.ResetTimer()

	for i := 0; b.Loop(); i++ {
		nodeId := "node" + strconv.Itoa(i)
		p.Add(nodeId, 20)
		p.Allocations()
		p.Remove(nodeId)
		p.Allocations()
	}
}

func BenchmarkLocate(b *testing.B) {
	p := New(fnv.New64(), 1.25).Partitioned(100)
	p.Add("nodeA", 20)
	p.Add("nodeB", 20)

	b.ResetTimer()

	for i := 0; b.Loop(); i++ {
		_ = p.Locate("key" + strconv.Itoa(i))
	}
}

func BenchmarkLocateN(b *testing.B) {
	p := New(fnv.New64(), 1.25).Partitioned(100)

	for i := range 10 {
		p.Add("node"+strconv.Itoa(i), 20)
	}

	b.ResetTimer()

	for i := 0; b.Loop(); i++ {
		_ = p.LocateN("key"+strconv.Itoa(i), 3)
	}
}

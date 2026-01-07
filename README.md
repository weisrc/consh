# Cons(h)

Go implementation of Consistent Hashing with:
- Bounded Loads
- Weighted Nodes
- Partitioning (optional)

## Let's Go!

```go
h := fnv.New64()       // should use xxhash for better distribution
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
```

## Partitioned

```go
c := consh.New(fnv.New64(), 1.25)
p := c.Partitioned(1024) // partitioned into 1024 partitions

p.Add("a", 100)
p.Add("b", 200)

setA := p.Partitions("a") // get partitions of a
setB := p.Partitions("b") // returns map[int]struct{}

println("node a has partitions:", len(setA))
println("node b has partitions:", len(setB))
```

## Benchmark

```
goos: linux
goarch: amd64
cpu: Intel(R) Core(TM) i5-8500 CPU @ 3.00GHz
BenchmarkAddRemove-6      298700                 4002 ns/op
BenchmarkLocate-6       12279928                99.73 ns/op
BenchmarkLocateN-6       5545964                233.6 ns/op
```

## License

MIT License. See LICENSE file for details.
# Cons(h)

Consistent Hashing with Bounded Loads in Go.

This is an implementation of this [paper](https://arxiv.org/pdf/1608.01350) with support for different number of virtual nodes (weight) per physical node and partitioning.

No hash function is enforced, you can use any hash function that implements `hash.Hash64` interface. However, a good hash function with uniform distribution is recommended for better performance.

## Let's Go!

Simple example to get started:

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

Example of using partitioned consistent hashing:

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

## Safety

All returned `*Node` are still in use and may change their load and max load values. Get and save their values to have a snapshot of them.

Use `sync.Mutex` or `sync.RWMutex` to protect concurrent access to the `Consh` or `PartitionedConsh` instance. Similarly, all returned `*Node` are not thread-safe.

## Benchmark

This library aims to be as efficient as possible and yet keep the code simple and readable. Here are some benchmark results:

```
goos: linux
goarch: amd64
cpu: Intel(R) Core(TM) i5-8500 CPU @ 3.00GHz
BenchmarkAddRemove-6      304366                 3932 ns/op
BenchmarkLocate-6       12159878                97.33 ns/op
BenchmarkLocateN-6       5523733                218.2 ns/op
```

## Tests

`movement_test.go` contains tests to verify the bounded load property during node addition and removal.

Other tests cover basic functionality of adding, removing nodes and locating resources.

## Roadmap

- Add more tests to improve coverage and verify correctness.
- Optimize performance further.
- Add more examples and documentation.
- Improve `LocateN` for better accuracy.
- Etcd integration for distributed systems in another repository.

## Contributions

Contributions are welcome! Feel free to open issues or submit pull requests.

## License

MIT License. See LICENSE for details.
# Optimistic Adaptive Radix Tree  

[![Go Report Card](https://goreportcard.com/badge/github.com/bobotu/opt-art)](https://goreportcard.com/report/github.com/bobotu/opt-art)
[![Build Status](https://travis-ci.org/bobotu/opt-art.svg?branch=master)](https://travis-ci.org/bobotu/opt-art)
[![Coverage Status](https://coveralls.io/repos/github/bobotu/opt-art/badge.svg?branch=master)](https://coveralls.io/github/bobotu/opt-art?branch=master)   

This is a Go implementation of ART.

## What is ART  
As mentioned in the origin paper, ART is a powerful indexing data structure.

> Its lookup performance surpasses highly tuned, read-only search trees, while supporting very efﬁcient insertions and
  deletions as well. At the same time, ART is very space efﬁcient and solves the problem of excessive worst-case space
  consumption, which plagues most radix trees, by adaptively choosing compact and efﬁcient data structures for internal 
  nodes. Even though ART’s performance is comparable to hash tables, it maintains the data in sorted order, which 
  enables additional operations like range scan and preﬁx lookup.

## Concurrent Operation
The main difference compared to other implementations (like [plar/go-adaptive-radix-tree](https://github.com/plar/go-adaptive-radix-tree))
is that this implementation support concurrent Read/Write operation.

Using the method described in `V. Leis, et al., The ART of Practical Synchronization, in DaMoN, 2016`.

## Usage  

```go
package main

import (
	"fmt"
	"github.com/bobotu/opt-art"
)

func main() {
	art := art.NewART()
	art.Put([]byte("hello"), "world")
	fmt.Println(art.Get([]byte("hello")))
	art.Delete([]byte("hello"))
}
```

You can check out [godoc.org](https://godoc.org/github.com/bobotu/opt-art) for more detailed documentation.  

## Performance  
Although this is only a rough implementation, there are still some good results on the benchmarks.  
Benchmarks performed on the same datasets as [plar/go-adaptive-radix-tree](https://github.com/plar/go-adaptive-radix-tree)

> * Words dataset contains list of 235,886 english words.
> *  UUIDs dataset contains 100,000 uuids.  

**opt-art**|**#**|**Average time**|**Bytes per operation**|**Allocs per operation**
:-----:|:-----:|:-----:|:-----:|:-----:
BenchmarkPutWords-8|20|69056403 ns/op|43403024 B/op|943544 allocs/op
BenchmarkPutUUID-8|50|27332784 ns/op|18400000 B/op|400000 allocs/op
BenchmarkGetWords-8|20|88016532 ns/op|0 B/op|0 allocs/op
BenchmarkGetUUID-8|100|22887744 ns/op|0 B/op|0 allocs/op  

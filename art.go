// Package art implements the Adaptive Radix Tree, and use Optimistic Locking method to achieve thread safe
// concurrent operation. The ART is described in "V. Leis, A. Kemper, and T. Neumann. The adaptive radix tree: ARTful
// indexing for main-memory databases. In ICDE, 2013". The Optimistic Syncing is described in "V. Leis, et al.,
// The ART of Practical Synchronization, in DaMoN, 2016".
package art

import (
	"sync/atomic"
	"unsafe"
)

// ART implements the Adaptive Radix Tree with Optimistic Locking.
// It looks like a KV data structure, which use byte slice as key.
// It support thread safe concurrent update and query.
type ART struct {
	root unsafe.Pointer
}

// OpFunc is ART query callback function.
// If OpFunc return true the current query will terminate immediately.
type OpFunc func(key []byte, value interface{}) (end bool)

// NewART create a new empty ART.
func NewART() *ART {
	return &ART{
		root: unsafe.Pointer(newNode4()),
	}
}

// Get lookup this tree, and return the value associate with the given key.
// This operation is thread safe.
func (t *ART) Get(key []byte) (interface{}, bool) {
	for {
		n := (*node)(atomic.LoadPointer(&t.root))
		if value, ex, ok := n.searchOpt(key, 0, nil, 0); ok {
			return value, ex
		}
	}
}

// Put put the given key and value into this tree, or replace exist key's value.
// This operation is thread safe.
func (t *ART) Put(key []byte, value interface{}) {
	for {
		n := (*node)(atomic.LoadPointer(&t.root))
		if n.insertOpt(key, value, 0, nil, 0, &t.root) {
			return
		}
	}
}

// Delete delete the given key and it's value from this tree.
// This operation is thread safe.
func (t *ART) Delete(key []byte) {
	for {
		n := (*node)(atomic.LoadPointer(&t.root))
		if n.removeOpt(key, 0, nil, 0, &t.root) {
			return
		}
	}
}

// Prefix find all key have the given prefix in this tree.
// This operation is thread safe.
func (t *ART) Prefix(prefix []byte, f OpFunc) {
	end := make([]byte, len(prefix))
	copy(end, prefix)
	end[len(end)-1]++
	t.Range(prefix, end, true, false, f)
}

// Range iterate the key in the given range.
// This operation is thread safe.
func (t *ART) Range(begin, end []byte, includeBegin, includeEnd bool, f OpFunc) {
	it := &iterator{
		end:          end,
		begin:        begin,
		includeBegin: includeBegin,
		includeEnd:   includeEnd,
		f:            f,
	}
	for {
		n := (*node)(atomic.LoadPointer(&t.root))
		end, ok := n.iterOpt(it, 0, nil, 0, 0, 0)
		if ok && end {
			return
		}
	}
}

// RangeTop is same as Range, but it will terminate after find k keys.
// This operation is thread safe.
func (t *ART) RangeTop(k int, begin, end []byte, includeBegin, includeEnd bool, f OpFunc) {
	it := &iterator{
		end:          end,
		begin:        begin,
		includeBegin: includeBegin,
		includeEnd:   includeEnd,
		k:            k,
	}
	it.setTopKOp(f)
	for {
		n := (*node)(atomic.LoadPointer(&t.root))
		end, ok := n.iterOpt(it, 0, nil, 0, 0, 0)
		if ok && end {
			return
		}
	}
}

// Min return the minimal key and it's value in this tree.
// This operation is thread safe.
func (t *ART) Min() ([]byte, interface{}) {
	for {
		n := (*node)(atomic.LoadPointer(&t.root))
		if k, v, ok := n.minimalOpt(nil, 0); ok {
			return k, v
		}
	}
}

// Max return the maximal key and it's value in this tree.
// This operation is thread safe.
func (t *ART) Max() ([]byte, interface{}) {
	for {
		n := (*node)(atomic.LoadPointer(&t.root))
		if k, v, ok := n.maximalOpt(nil, 0); ok {
			return k, v
		}
	}
}

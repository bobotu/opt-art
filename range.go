package art

import (
	"bytes"
	"sync/atomic"
	"unsafe"
)

type iterator struct {
	end   []byte
	begin []byte

	// prev record the last applied key.
	// When iterate restart due to conflict use prev as new begin key.
	prev []byte

	includeBegin bool
	includeEnd   bool
	k            int

	f OpFunc
}

func (it *iterator) getBegin() []byte {
	if it.prev == nil {
		return it.begin
	}
	// Resume the iterate.
	return it.prev
}

func (it *iterator) getEnd() []byte {
	return it.end
}

func (it *iterator) isIncludeEnd() bool {
	return it.includeEnd
}

func (it *iterator) isIncludeBegin() bool {
	if it.prev == nil {
		return it.includeBegin
	}
	// Always start at prev's next key.
	return false
}

func (it *iterator) setTopKOp(f OpFunc) {
	it.f = func(key []byte, value interface{}) bool {
		it.k--
		if f(key, value) {
			return true
		}
		if it.k == 0 {
			return true
		}
		return false
	}
}

func (n *node) fullCompare(version uint64, key []byte, depth int) (int, bool) {
	remain := len(key) - depth
	checkLen := min(n.prefixLen, min(maxPrefixLen, remain))
	cmp := bytes.Compare(n.prefix[:checkLen], key[depth:depth+checkLen])
	if cmp == 0 {
		needFull := remain > maxPrefixLen && n.prefixLen > maxPrefixLen
		if needFull {
			fullKey, ok := n.fullKey(version)
			if !ok {
				return 0, false
			}
			l := min(n.prefixLen, len(key))
			cmp = bytes.Compare(fullKey[depth+checkLen:depth+l], key[depth+checkLen:depth+l])
		}
	}
	if cmp == 0 {
		if n.prefixLen > remain {
			return 1, true
		}
		return 0, true
	}
	return cmp, true
}

func (n *node) iterOpt(it *iterator, depth int, parent *node, parentVersion uint64, beginCmp, endCmp int) (end, cont bool) {
	version, ok := n.rLock()
	if !ok {
		return false, false
	}
	if !parent.rUnlock(parentVersion) {
		return false, false
	}

	if beginCmp == 0 {
		if beginCmp, ok = n.fullCompare(version, it.getBegin(), depth); !ok {
			return false, false
		}
	} else if beginCmp < 0 {
		return false, true
	}
	if endCmp == 0 {
		if endCmp, ok = n.fullCompare(version, it.getEnd(), depth); !ok {
			return false, false
		}
	} else if endCmp > 0 {
		return true, true
	}
	depth += n.prefixLen

	usePrefixLeaf := true
	if beginCmp == 0 {
		if depth < len(it.getBegin()) {
			usePrefixLeaf = false
		} else {
			usePrefixLeaf = it.isIncludeBegin()
			beginCmp = 1
		}
	}
	if endCmp == 0 && depth == len(it.getEnd()) {
		usePrefixLeaf = it.isIncludeEnd()
		endCmp = 1
	}

	prefixLeaf := (*leaf)(atomic.LoadPointer(&n.prefixLeaf))
	if !n.lockCheck(version) {
		return false, false
	}
	if usePrefixLeaf && prefixLeaf != nil {
		k, v := prefixLeaf.key, prefixLeaf.value
		if !n.lockCheck(version) {
			return false, false
		}
		it.prev = k
		if it.f(k, v) {
			return true, true
		}
	}
	if endCmp > 0 {
		return true, true
	}

	return n.iterChild(it, version, depth, beginCmp, endCmp)
}

func (n *node) iterChild(it *iterator, version uint64, depth, beginCmp, endCmp int) (end, cont bool) {
	switch n.nodeType {
	case typeNode4:
		n4 := (*node4)(unsafe.Pointer(n))
		return n4.iterChild(it, version, depth, beginCmp, endCmp)
	case typeNode16:
		n16 := (*node16)(unsafe.Pointer(n))
		return n16.iterChild(it, version, depth, beginCmp, endCmp)
	case typeNode48:
		n48 := (*node48)(unsafe.Pointer(n))
		return n48.iterChild(it, version, depth, beginCmp, endCmp)
	case typeNode256:
		n256 := (*node256)(unsafe.Pointer(n))
		return n256.iterChild(it, version, depth, beginCmp, endCmp)
	default:
		panic("opt-art: unreachable code")
	}
}

func (n *node4) iterChild(it *iterator, version uint64, depth, beginCmp, endCmp int) (end, cont bool) {
	var bkey, ekey byte
	if beginCmp == 0 {
		bkey = it.getBegin()[depth]
	}
	if endCmp == 0 {
		ekey = it.getEnd()[depth]
	}
	for i := 0; i < int(n.numChildren); i++ {
		key, child := n.keys[i], (*node)(n.children[i])
		if !n.lockCheck(version) {
			return false, false
		}
		if beginCmp == 0 && key < bkey {
			continue
		}
		if endCmp == 0 && key > ekey {
			return true, true
		}
		end, ok := it.accessChild((*node)(unsafe.Pointer(n)), child, version, depth, beginCmp, endCmp, bkey, ekey, key)
		if !ok {
			return false, false
		}
		if end {
			return true, true
		}
	}
	return false, true
}

func (n *node16) iterChild(it *iterator, version uint64, depth, beginCmp, endCmp int) (end, cont bool) {
	var bkey, ekey byte
	if beginCmp == 0 {
		bkey = it.getBegin()[depth]
	}
	if endCmp == 0 {
		ekey = it.getEnd()[depth]
	}
	for i := 0; i < int(n.numChildren); i++ {
		key, child := n.keys[i], (*node)(n.children[i])
		if !n.lockCheck(version) {
			return false, false
		}
		if beginCmp == 0 && key < bkey {
			continue
		}
		if endCmp == 0 && key > ekey {
			return true, true
		}
		end, ok := it.accessChild((*node)(unsafe.Pointer(n)), child, version, depth, beginCmp, endCmp, bkey, ekey, key)
		if !ok {
			return false, false
		}
		if end {
			return true, true
		}
	}
	return false, true
}

func (n *node48) iterChild(it *iterator, version uint64, depth, beginCmp, endCmp int) (end, cont bool) {
	var bkey, ekey byte
	if beginCmp == 0 {
		bkey = it.getBegin()[depth]
	}
	if endCmp == 0 {
		ekey = it.getEnd()[depth]
	}
	for key := int(bkey); key < 256; key++ {
		if endCmp == 0 && byte(key) > ekey {
			return true, true
		}
		pos := n.index[key]
		if !n.lockCheck(version) {
			return false, false
		}
		if pos == 0 {
			continue
		}
		child := (*node)(n.children[pos-1])
		if !n.lockCheck(version) {
			return false, false
		}
		end, ok := it.accessChild((*node)(unsafe.Pointer(n)), child, version, depth, beginCmp, endCmp, bkey, ekey, byte(key))
		if !ok {
			return false, false
		}
		if end {
			return true, true
		}

	}
	return false, true
}

func (n *node256) iterChild(it *iterator, version uint64, depth, beginCmp, endCmp int) (end, cont bool) {
	var bkey, ekey byte
	if beginCmp == 0 {
		bkey = it.getBegin()[depth]
	}
	if endCmp == 0 {
		ekey = it.getEnd()[depth]
	}
	for key := int(bkey); key < 256; key++ {
		if endCmp == 0 && byte(key) > ekey {
			return true, true
		}
		child := (*node)(n.children[key])
		if !n.lockCheck(version) {
			return false, false
		}
		if child == nil {
			continue
		}
		end, ok := it.accessChild((*node)(unsafe.Pointer(n)), child, version, depth, beginCmp, endCmp, bkey, ekey, byte(key))
		if !ok {
			return false, false
		}
		if end {
			return true, true
		}
	}
	return false, true
}

func (it *iterator) accessChild(n *node, child *node, version uint64, depth, beginCmp, endCmp int, bkey, ekey, key byte) (end, ok bool) {
	if child.nodeType == typeLeaf {
		l := (*leaf)(unsafe.Pointer(child))
		k, v := l.key, l.value
		if !n.lockCheck(version) {
			return false, false
		}
		if beginCmp == 0 && key == bkey {
			cmp := bytes.Compare(k[depth:], it.getBegin()[depth:])
			if cmp < 0 || (cmp == 0 && !it.isIncludeBegin()) {
				return false, true
			}
		}
		if endCmp == 0 && key == ekey {
			cmp := bytes.Compare(k[depth:], it.getEnd()[depth:])
			if cmp > 0 || (cmp == 0 && !it.isIncludeEnd()) {
				return true, true
			}
		}
		it.prev = k
		return it.f(k, v), true
	} else {
		if beginCmp == 0 && key > bkey {
			beginCmp = 1
		}
		if endCmp == 0 && key < ekey {
			endCmp = -1
		}
		return child.iterOpt(it, depth+1, n, version, beginCmp, endCmp)
	}
}

func (n *node) firstChild() *node {
	switch n.nodeType {
	case typeNode4:
		n4 := (*node4)(unsafe.Pointer(n))
		return (*node)(atomic.LoadPointer(&n4.children[0]))
	case typeNode16:
		n16 := (*node16)(unsafe.Pointer(n))
		return (*node)(atomic.LoadPointer(&n16.children[0]))
	case typeNode48:
		n48 := (*node48)(unsafe.Pointer(n))
		for i := 0; i < 256; i++ {
			pos := n48.index[i]
			if pos == 0 {
				continue
			}
			if c := atomic.LoadPointer(&n48.children[pos-1]); c != nil {
				return (*node)(c)
			}
		}
	case typeNode256:
		n256 := (*node256)(unsafe.Pointer(n))
		for i := 0; i < 256; i++ {
			if c := atomic.LoadPointer(&n256.children[i]); c != nil {
				return (*node)(c)
			}
		}
	}
	panic("opt-art: unreachable code.")
}

func (n *node) minimalOpt(parent *node, parentVersion uint64) ([]byte, interface{}, bool) {
	var (
		version uint64
		ok      bool
	)

RECUR:
	if version, ok = n.rLock(); !ok {
		return nil, nil, false
	}
	if !parent.rUnlock(parentVersion) {
		return nil, nil, false
	}

	prefixLeaf := (*leaf)(atomic.LoadPointer(&n.prefixLeaf))
	if !n.lockCheck(version) {
		return nil, nil, false
	}
	if prefixLeaf != nil {
		k, v := prefixLeaf.key, prefixLeaf.value
		if !n.lockCheck(version) {
			return nil, nil, false
		}
		return k, v, true
	}

	child := n.firstChild()
	if !n.lockCheck(version) {
		return nil, nil, false
	}

	if child.nodeType == typeLeaf {
		l := (*leaf)(unsafe.Pointer(child))
		k, v := l.key, l.value
		if !n.lockCheck(version) {
			return nil, nil, false
		}
		return k, v, true
	}

	parent = n
	parentVersion = version
	n = child
	goto RECUR
}

func (n *node) lastChild() *node {
	switch n.nodeType {
	case typeNode4:
		n4 := (*node4)(unsafe.Pointer(n))
		return (*node)(atomic.LoadPointer(&n4.children[n4.numChildren-1]))
	case typeNode16:
		n16 := (*node16)(unsafe.Pointer(n))
		return (*node)(atomic.LoadPointer(&n16.children[n16.numChildren-1]))
	case typeNode48:
		n48 := (*node48)(unsafe.Pointer(n))
		for i := 255; i >= 0; i-- {
			pos := n48.index[i]
			if pos == 0 {
				continue
			}
			if c := atomic.LoadPointer(&n48.children[pos-1]); c != nil {
				return (*node)(c)
			}
		}
	case typeNode256:
		n256 := (*node256)(unsafe.Pointer(n))
		for i := 255; i >= 0; i-- {
			if c := atomic.LoadPointer(&n256.children[i]); c != nil {
				return (*node)(c)
			}
		}
	}
	panic("opt-art: unreachable code.")
}

func (n *node) maximalOpt(parent *node, parentVersion uint64) ([]byte, interface{}, bool) {
	var (
		version uint64
		ok      bool
	)

RECUR:
	if version, ok = n.rLock(); !ok {
		return nil, nil, false
	}
	if !parent.rUnlock(parentVersion) {
		return nil, nil, false
	}

	child := n.lastChild()
	if !n.lockCheck(version) {
		return nil, nil, false
	}

	if child.nodeType == typeLeaf {
		l := (*leaf)(unsafe.Pointer(child))
		k, v := l.key, l.value
		if !n.lockCheck(version) {
			return nil, nil, false
		}
		return k, v, true
	}

	parent = n
	parentVersion = version
	n = child
	goto RECUR
}

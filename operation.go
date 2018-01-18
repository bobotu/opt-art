package art

import (
	"sync/atomic"
	"unsafe"
)

func (n *node) checkPrefix(key []byte, depth int) (idx int) {
	if n.prefixLen == 0 {
		return 0
	}

	l := min(len(key)-depth, min(n.prefixLen, maxPrefixLen))

	for idx = 0; idx < l; idx++ {
		if n.prefix[idx] != key[depth+idx] {
			return idx
		}
	}
	return
}

func (n *node) findChild(key byte) (child *node, nodeLoc *unsafe.Pointer, position int) {
	switch n.nodeType {
	case typeNode4:
		n4 := (*node4)(unsafe.Pointer(n))
		for i := 0; i < int(n4.numChildren); i++ {
			if n4.keys[i] == key {
				return (*node)(n4.children[i]), &n4.children[i], i
			}
		}
	case typeNode16:
		// As measured in MassTree's paper, linear search may have better performance
		// than binary search on modern CPU.
		n16 := (*node16)(unsafe.Pointer(n))
		for i := 0; i < int(n16.numChildren); i++ {
			if n16.keys[i] == key {
				return (*node)(n16.children[i]), &n16.children[i], i
			}
		}
	case typeNode48:
		n48 := (*node48)(unsafe.Pointer(n))
		if idx := n48.index[key]; idx > 0 {
			return (*node)(n48.children[idx-1]), &n48.children[idx-1], int(key)
		}
	case typeNode256:
		n256 := (*node256)(unsafe.Pointer(n))
		return (*node)(n256.children[key]), &n256.children[key], int(key)
	}

	// Not found.
	return nil, nil, 0
}

func (n *node) searchOpt(key []byte, depth int, parent *node, parentVersion uint64) (interface{}, bool, bool) {
	var (
		version uint64
		ok      bool
	)

RECUR:
	if version, ok = n.rLock(); !ok {
		return nil, false, false
	}
	if !parent.rUnlock(parentVersion) {
		return nil, false, false
	}

	if n.checkPrefix(key, depth) != min(n.prefixLen, maxPrefixLen) {
		if !n.rUnlock(version) {
			return nil, false, false
		}
		return nil, false, true
	}
	depth += n.prefixLen

	if depth == len(key) {
		l := (*leaf)(atomic.LoadPointer(&n.prefixLeaf))
		var (
			value interface{}
			ex    bool
		)
		if l != nil && l.match(key) {
			value = l.value
			ex = true
		}
		if !n.rUnlock(version) {
			return nil, false, false
		}
		return value, ex, true
	}

	if depth > len(key) {
		return nil, false, n.rUnlock(version)
	}

	nextNode, _, _ := n.findChild(key[depth])
	if !n.lockCheck(version) {
		return nil, false, false
	}

	if nextNode == nil {
		if !n.rUnlock(version) {
			return nil, false, false
		}
		return nil, false, true
	}

	if nextNode.nodeType == typeLeaf {
		l := (*leaf)(unsafe.Pointer(nextNode))
		var (
			value interface{}
			ex    bool
		)
		if l.match(key) {
			value = l.value
			ex = true
		}
		if !n.rUnlock(version) {
			return nil, false, false
		}
		return value, ex, true
	}

	depth += 1
	parent = n
	parentVersion = version
	n = nextNode
	goto RECUR
}

func (n *node) insertSplitPrefix(key, fullKey []byte, value interface{}, depth int, prefixLen int, nodeLoc *unsafe.Pointer) {
	newNode := newNode4()
	if depth := depth + prefixLen; len(key) == depth {
		newNode.prefixLeaf = unsafe.Pointer(newLeaf(key, value))
	} else {
		newNode.insertChild(key[depth], unsafe.Pointer(newLeaf(key, value)))
	}
	newNode.prefixLen = prefixLen
	copy(newNode.prefix[:min(maxPrefixLen, prefixLen)], n.prefix[:])
	if n.prefixLen <= maxPrefixLen {
		newNode.insertChild(n.prefix[prefixLen], unsafe.Pointer(n))
		n.prefixLen -= prefixLen + 1
		copy(n.prefix[:min(maxPrefixLen, n.prefixLen)], n.prefix[prefixLen+1:])
	} else {
		newNode.insertChild(fullKey[depth+prefixLen], unsafe.Pointer(n))
		n.prefixLen -= prefixLen + 1
		copy(n.prefix[:min(maxPrefixLen, n.prefixLen)], fullKey[depth+prefixLen+1:])
	}
	atomic.StorePointer(nodeLoc, unsafe.Pointer(newNode))
}

func (n *node) fullKey(version uint64) ([]byte, bool) {
	if p := atomic.LoadPointer(&n.prefixLeaf); p != nil {
		l := (*leaf)(p)
		if !n.rUnlock(version) {
			return nil, false
		}
		return l.key, true
	}

	next := n.firstChild()
	if !n.lockCheck(version) {
		return nil, false
	}

	if next.nodeType == typeLeaf {
		l := (*leaf)(unsafe.Pointer(next))
		key := l.key
		if !n.rUnlock(version) {
			return nil, false
		}
		return key, true
	}

	v, ok := next.rLock()
	if !ok {
		return nil, false
	}
	return next.fullKey(v)
}

func (n *node) prefixMismatch(key []byte, depth int, parent *node, version, parentVersion uint64) (int, []byte, bool) {
	if n.prefixLen <= maxPrefixLen {
		return n.checkPrefix(key, depth), nil, true
	}

	var (
		fullKey []byte
		ok      bool
	)
	for {
		if !n.lockCheck(version) || !parent.lockCheck(parentVersion) {
			return 0, nil, false
		}
		if ok {
			break
		}
		fullKey, ok = n.fullKey(version)
	}

	i, l := depth, min(len(key), depth+n.prefixLen)
	for ; i < l; i++ {
		if key[i] != fullKey[i] {
			break
		}
	}
	return i - depth, fullKey, true
}

func (n *node) insertOpt(key []byte, value interface{}, depth int, parent *node, parentVersion uint64, nodeLoc *unsafe.Pointer) bool {
	var (
		version  uint64
		ok       bool
		nextNode *node
		nextLoc  *unsafe.Pointer
	)

RECUR:
	if version, ok = n.rLock(); !ok {
		return false
	}

	p, fullKey, ok := n.prefixMismatch(key, depth, parent, version, parentVersion)
	if !ok {
		return false
	}
	if p != n.prefixLen {
		if !parent.upgradeToLock(parentVersion) {
			return false
		}
		if !n.upgradeToLockWithNode(version, parent) {
			return false
		}
		n.insertSplitPrefix(key, fullKey, value, depth, p, nodeLoc)
		n.unlock()
		parent.unlock()
		return true
	}
	depth += n.prefixLen

	if depth == len(key) {
		if !n.upgradeToLock(version) {
			return false
		}
		if !parent.rUnlockWithNode(parentVersion, n) {
			return false
		}
		n.updatePrefixLeaf(key, value)
		n.unlock()
		return true
	}

	nextNode, nextLoc, _ = n.findChild(key[depth])
	if !n.lockCheck(version) {
		return false
	}

	if nextNode == nil {
		if n.isFull() {
			if !parent.upgradeToLock(parentVersion) {
				return false
			}
			if !n.upgradeToLockWithNode(version, parent) {
				return false
			}
			n.growAndInsert(key[depth], unsafe.Pointer(newLeaf(key, value)), nodeLoc)
			n.unlockObsolete()
			parent.unlock()
		} else {
			if !n.upgradeToLock(version) {
				return false
			}
			if !parent.rUnlockWithNode(parentVersion, n) {
				return false
			}
			n.insertChild(key[depth], unsafe.Pointer(newLeaf(key, value)))
			n.unlock()
		}
		return true
	}

	if !parent.rUnlock(parentVersion) {
		return false
	}

	if nextNode.nodeType == typeLeaf {
		if !n.upgradeToLock(version) {
			return false
		}
		l := (*leaf)(unsafe.Pointer(nextNode))
		l.updateOrExpand(key, value, depth+1, nextLoc)
		n.unlock()
		return true
	}

	depth += 1
	parent = n
	parentVersion = version
	nodeLoc = nextLoc
	n = nextNode
	goto RECUR
}

func (n *node) removeOpt(key []byte, depth int, parent *node, parentVersion uint64, nodeLoc *unsafe.Pointer) bool {
	var (
		version uint64
		ok      bool
	)

RECUR:
	if version, ok = n.rLock(); !ok {
		return false
	}
	if !parent.rUnlock(parentVersion) {
		return false
	}

	if n.checkPrefix(key, depth) != min(n.prefixLen, maxPrefixLen) {
		if !n.rUnlock(version) {
			return false
		}
		return true
	}
	depth += n.prefixLen

	if depth == len(key) {
		l := (*leaf)(atomic.LoadPointer(&n.prefixLeaf))
		if l == nil || !l.match(key) {
			if !n.rUnlock(version) {
				return false
			}
			return true
		}
		if n.shouldCompress(parent) {
			if !parent.upgradeToLock(parentVersion) {
				return false
			}
			if !n.upgradeToLockWithNode(version, parent) {
				return false
			}
			atomic.StorePointer(&n.prefixLeaf, nil)
			n4 := (*node4)(unsafe.Pointer(n))
			if !n4.compressChild(0, nodeLoc) {
				n.unlock()
				parent.unlock()
				return false
			}
			n.unlockObsolete()
			parent.unlock()
			return true
		} else {
			if !n.upgradeToLock(version) {
				return false
			}
			atomic.StorePointer(&n.prefixLeaf, nil)
			n.unlock()
			return true
		}
	}

	if depth > len(key) {
		return n.rUnlock(version)
	}

	nextNode, nextLoc, idx := n.findChild(key[depth])
	if !n.lockCheck(version) {
		return false
	}

	if nextNode == nil {
		if !n.rUnlock(version) {
			return false
		}
		return true
	}

	if nextNode.nodeType == typeLeaf {
		l := (*leaf)(unsafe.Pointer(nextNode))
		if !l.match(key) {
			if !n.rUnlock(version) {
				return false
			}
			return true
		}
		if n.shouldShrink(parent) {
			if !parent.upgradeToLock(parentVersion) {
				return false
			}
			if !n.upgradeToLockWithNode(version, parent) {
				return false
			}
			if !n.removeChildAndShrink(key[depth], nodeLoc) {
				n.unlock()
				parent.unlock()
				return false
			}
			n.unlockObsolete()
			parent.unlock()
			return true
		} else {
			if !n.upgradeToLock(version) {
				return false
			}
			n.removeChild(idx)
			n.unlock()
			return true
		}
	}

	depth += 1
	parent = n
	parentVersion = version
	nodeLoc = nextLoc
	n = nextNode
	goto RECUR
}

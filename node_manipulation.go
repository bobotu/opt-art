package art

import (
	"sync/atomic"
	"unsafe"
)

func (n *node) insertChild(key byte, child unsafe.Pointer) {
	switch n.nodeType {
	case typeNode4:
		(*node4)(unsafe.Pointer(n)).insertChild(key, child)
	case typeNode16:
		(*node16)(unsafe.Pointer(n)).insertChild(key, child)
	case typeNode48:
		(*node48)(unsafe.Pointer(n)).insertChild(key, child)
	case typeNode256:
		(*node256)(unsafe.Pointer(n)).insertChild(key, child)
	default:
		panic("opt-art: unreachable code")
	}
}

func (n *node4) insertChild(key byte, child unsafe.Pointer) {
	var i int
	for i = 0; i < int(n.numChildren); i++ {
		if key < n.keys[i] {
			break
		}
	}
	copy(n.keys[i+1:], n.keys[i:])
	copy(n.children[i+1:], n.children[i:])
	n.keys[i] = key
	atomic.StorePointer(&n.children[i], child)
	n.numChildren++
}

func (n *node16) insertChild(key byte, child unsafe.Pointer) {
	var i int
	for i = 0; i < int(n.numChildren); i++ {
		if key < n.keys[i] {
			break
		}
	}
	copy(n.keys[i+1:], n.keys[i:])
	copy(n.children[i+1:], n.children[i:])
	n.keys[i] = key
	atomic.StorePointer(&n.children[i], child)
	n.numChildren++
}

func (n *node48) insertChild(key byte, child unsafe.Pointer) {
	pos := n.allocSlot()
	n.children[pos] = child
	n.index[key] = int8(pos + 1)
	n.numChildren++
}

func (n *node256) insertChild(key byte, child unsafe.Pointer) {
	n.children[key] = child
	n.numChildren++
}

func (n *node) growAndInsert(key byte, child unsafe.Pointer, nodeLoc *unsafe.Pointer) {
	switch n.nodeType {
	case typeNode4:
		(*node4)(unsafe.Pointer(n)).growAndInsert(key, child, nodeLoc)
	case typeNode16:
		(*node16)(unsafe.Pointer(n)).growAndInsert(key, child, nodeLoc)
	case typeNode48:
		(*node48)(unsafe.Pointer(n)).growAndInsert(key, child, nodeLoc)
	case typeNode256:
		(*node256)(unsafe.Pointer(n)).growAndInsert(key, child, nodeLoc)
	default:
		panic("opt-art: unreachable code")
	}
}

func copyNode(newNodeP, nP unsafe.Pointer) {
	newNode, n := (*node)(newNodeP), (*node)(nP)
	newNode.numChildren = n.numChildren
	newNode.prefixLen = n.prefixLen
	newNode.prefix = n.prefix
	newNode.prefixLeaf = n.prefixLeaf
}

func (n *node4) growAndInsert(key byte, child unsafe.Pointer, nodeLoc *unsafe.Pointer) {
	newNode := newNode16()
	copy(newNode.keys[:], n.keys[:])
	copy(newNode.children[:], n.children[:])
	copyNode(unsafe.Pointer(newNode), unsafe.Pointer(n))
	newNode.insertChild(key, child)
	atomic.StorePointer(nodeLoc, unsafe.Pointer(newNode))
}

func (n *node16) growAndInsert(key byte, child unsafe.Pointer, nodeLoc *unsafe.Pointer) {
	newNode := newNode48()
	copy(newNode.children[:], n.children[:])
	newNode.slots = node48GrowSlots
	for idx, k := range n.keys {
		newNode.index[k] = int8(idx) + 1
	}
	copyNode(unsafe.Pointer(newNode), unsafe.Pointer(n))
	newNode.insertChild(key, child)
	atomic.StorePointer(nodeLoc, unsafe.Pointer(newNode))
}

func (n *node48) growAndInsert(key byte, child unsafe.Pointer, nodeLoc *unsafe.Pointer) {
	newNode := newNode256()
	for i := range newNode.children {
		if idx := n.index[i]; idx > 0 {
			newNode.children[i] = n.children[idx-1]
		}
	}
	copyNode(unsafe.Pointer(newNode), unsafe.Pointer(n))
	newNode.insertChild(key, child)
	atomic.StorePointer(nodeLoc, unsafe.Pointer(newNode))
}

func (n *node256) growAndInsert(key byte, child unsafe.Pointer, nodeLoc *unsafe.Pointer) {
	n.insertChild(key, child)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (l *leaf) updateOrExpand(key []byte, value interface{}, depth int, nodeLoc *unsafe.Pointer) {
	if l.match(key) {
		l.value = value
		return
	}
	var (
		i         int
		prefixLen = min(len(key), len(l.key))
		newNode   = newNode4()
	)
	for i = depth; i < prefixLen; i++ {
		if l.key[i] != key[i] {
			break
		}
	}
	newNode.prefixLen = i - depth
	copy(newNode.prefix[:maxPrefixLen], key[depth:i])

	if i == len(l.key) {
		newNode.prefixLeaf = unsafe.Pointer(l)
	} else {
		newNode.insertChild(l.key[i], unsafe.Pointer(l))
	}
	if i == len(key) {
		newNode.prefixLeaf = unsafe.Pointer(newLeaf(key, value))
	} else {
		newNode.insertChild(key[i], unsafe.Pointer(newLeaf(key, value)))
	}
	atomic.StorePointer(nodeLoc, unsafe.Pointer(newNode))
}

func (n *node) updatePrefixLeaf(key []byte, value interface{}) {
	l := (*leaf)(n.prefixLeaf)
	if l == nil {
		atomic.StorePointer(&n.prefixLeaf, unsafe.Pointer(newLeaf(key, value)))
	} else {
		l.value = value
	}
}

func (n *node) removeChild(i int) {
	switch n.nodeType {
	case typeNode4:
		n4 := (*node4)(unsafe.Pointer(n))
		copy(n4.keys[i:], n4.keys[i+1:])
		copy(n4.children[i:], n4.children[i+1:])
		n4.numChildren--
	case typeNode16:
		n16 := (*node16)(unsafe.Pointer(n))
		copy(n16.keys[i:], n16.keys[i+1:])
		copy(n16.children[i:], n16.children[i+1:])
		n16.numChildren--
	case typeNode48:
		n48 := (*node48)(unsafe.Pointer(n))
		pos := int(n48.index[i] - 1)
		n48.index[i] = 0
		n48.children[pos] = nil
		n48.freeSlot(pos)
		n48.numChildren--
	case typeNode256:
		n256 := (*node256)(unsafe.Pointer(n))
		n256.children[i] = nil
		n256.numChildren--
	}
}

func (n *node) shouldShrink(parent *node) bool {
	switch n.nodeType {
	case typeNode4:
		if parent == nil {
			return false
		}
		if atomic.LoadPointer(&n.prefixLeaf) == nil {
			return n.numChildren <= 2
		} else {
			return n.numChildren <= 1
		}
	case typeNode16:
		return n.numChildren <= node16MinSize
	case typeNode48:
		return n.numChildren <= node48MinSize
	case typeNode256:
		// 256 will overflow to 0. But node256 never have 0 children,
		// so 0 simply means 256.
		return n.numChildren > 0 && n.numChildren <= node256MinSize
	default:
		panic("opt-art: unreachable code.")
	}
}

func (n *node) removeChildAndShrink(key byte, nodeLoc *unsafe.Pointer) bool {
	switch n.nodeType {
	case typeNode4:
		return (*node4)(unsafe.Pointer(n)).removeChildAndShrink(key, nodeLoc)
	case typeNode16:
		return (*node16)(unsafe.Pointer(n)).removeChildAndShrink(key, nodeLoc)
	case typeNode48:
		return (*node48)(unsafe.Pointer(n)).removeChildAndShrink(key, nodeLoc)
	case typeNode256:
		return (*node256)(unsafe.Pointer(n)).removeChildAndShrink(key, nodeLoc)
	default:
		panic("opt-art: unreachable code")
	}
}

func (n *node4) removeChildAndShrink(key byte, nodeLoc *unsafe.Pointer) bool {
	if n.prefixLeaf != nil {
		atomic.StorePointer(nodeLoc, n.prefixLeaf)
		return true
	}

	for i := 0; i < int(n.numChildren); i++ {
		if n.keys[i] != key {
			return n.compressChild(i, nodeLoc)
		}
	}

	panic("opt-art: unreachable code.")
}

func (n *node4) compressChild(idx int, nodeLoc *unsafe.Pointer) bool {
	child := (*node)(n.children[idx])
	if child.nodeType != typeLeaf {
		if !child.lock() {
			return false
		}
		prefixLen := n.prefixLen
		if prefixLen < maxPrefixLen {
			n.prefix[prefixLen] = n.keys[idx]
			prefixLen++
		}
		if prefixLen < maxPrefixLen {
			subPrefixLen := min(child.prefixLen, maxPrefixLen-prefixLen)
			copy(n.prefix[prefixLen:], child.prefix[:subPrefixLen])
			prefixLen += subPrefixLen
		}

		copy(child.prefix[:], n.prefix[:min(prefixLen, maxPrefixLen)])
		child.prefixLen += n.prefixLen + 1
		child.unlock()
	}
	atomic.StorePointer(nodeLoc, unsafe.Pointer(child))
	return true
}

func (n *node16) removeChildAndShrink(key byte, nodeLoc *unsafe.Pointer) bool {
	newNode := newNode4()
	idx := 0
	for i := 0; i < int(n.numChildren); i++ {
		if n.keys[i] != key {
			newNode.keys[idx] = n.keys[i]
			newNode.children[idx] = n.children[i]
			idx++
		}
	}
	copyNode(unsafe.Pointer(newNode), unsafe.Pointer(n))
	newNode.numChildren = node16MinSize - 1
	atomic.StorePointer(nodeLoc, unsafe.Pointer(newNode))
	return true
}

func (n *node48) removeChildAndShrink(key byte, nodeLoc *unsafe.Pointer) bool {
	newNode := newNode16()
	idx := 0
	for i := 0; i < 256; i++ {
		if i != int(key) && n.index[i] != 0 {
			newNode.keys[idx] = uint8(i)
			newNode.children[idx] = n.children[n.index[i]-1]
			idx++
		}
	}
	copyNode(unsafe.Pointer(newNode), unsafe.Pointer(n))
	newNode.numChildren = node48MinSize - 1
	atomic.StorePointer(nodeLoc, unsafe.Pointer(newNode))
	return true
}

func (n *node256) removeChildAndShrink(key byte, nodeLoc *unsafe.Pointer) bool {
	newNode := newNode48()
	for i := 0; i < 256; i++ {
		if i != int(key) && n.children[i] != nil {
			pos := newNode.allocSlot()
			newNode.index[i] = int8(pos) + 1
			newNode.children[pos] = n.children[i]
		}
	}
	copyNode(unsafe.Pointer(newNode), unsafe.Pointer(n))
	newNode.numChildren = node256MinSize - 1
	atomic.StorePointer(nodeLoc, unsafe.Pointer(newNode))
	return true
}

func (n *node) shouldCompress(parent *node) bool {
	if n.nodeType == typeNode4 {
		return n.numChildren == 1 && parent != nil
	}
	return false
}

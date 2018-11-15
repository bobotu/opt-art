package art

import (
	"bytes"
	"math/bits"
	"unsafe"
)

const (
	typeNode4 = iota
	typeNode16
	typeNode48
	typeNode256
	typeLeaf
)

const maxPrefixLen = 8

const (
	node16MinSize  = 4
	node48MinSize  = 13
	node256MinSize = 38
)

// node is the basic data of each node. Embedded into node{4, 16, 48, 256}.
type node struct {
	nodeType uint8

	// numChildren is number of children expect prefixLeaf.
	numChildren uint8

	// version is the optimistic lock.
	version uint64

	// prefixLeaf store value of key which is prefix of other keys.
	// eg. [1]'s value will store here when [1, 0] exist.
	prefixLeaf unsafe.Pointer

	// prefixLen and prefix is the optimistic path compression.
	prefixLen int
	prefix    [maxPrefixLen]byte
}

func (n *node) isFull() bool {
	switch n.nodeType {
	case typeNode4:
		return n.numChildren == 4
	case typeNode16:
		return n.numChildren == 16
	case typeNode48:
		return n.numChildren == 48
	case typeNode256:
		return false
	}

	panic("opt-art: unreachable code")
}

type node4 struct {
	node
	keys     [4]byte
	children [5]unsafe.Pointer
}

func newNode4() *node4 {
	n := new(node4)
	n.nodeType = typeNode4
	return n
}

type node16 struct {
	node
	keys     [16]byte
	children [16]unsafe.Pointer
}

func newNode16() *node16 {
	n := new(node16)
	n.nodeType = typeNode16
	return n
}

const (
	node48EmptySlots = 0xffff000000000000
	node48GrowSlots  = 0xffffffff00000000
)

type node48 struct {
	node
	index    [256]int8
	children [48]unsafe.Pointer
	slots    uint64
}

func newNode48() *node48 {
	n := new(node48)
	n.nodeType = typeNode48
	n.slots = node48EmptySlots
	return n
}

func (n *node48) allocSlot() int {
	idx := 48 - bits.Len64(^n.slots)
	n.slots |= uint64(1) << (48 - uint(idx) - 1)
	return idx
}

func (n *node48) freeSlot(idx int) {
	n.slots &= ^(uint64(1) << (48 - uint(idx) - 1))
}

type node256 struct {
	node
	children [256]unsafe.Pointer
}

func newNode256() *node256 {
	n := new(node256)
	n.nodeType = typeNode256
	return n
}

type leaf struct {
	nodeType uint8
	key      []byte
	value    []byte
}

func newLeaf(key []byte, value []byte) *leaf {
	l := &leaf{
		nodeType: typeLeaf,
		key:      key,
		value:    value,
	}
	return l
}

func (l *leaf) match(key []byte) bool {
	return len(l.key) == len(key) && bytes.Compare(key, l.key) == 0
}

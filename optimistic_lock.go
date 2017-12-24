package art

import (
	"runtime"
	"sync/atomic"
)

const spinCount = 30

func (n *node) rLock() (uint64, bool) {
	v := n.waitUnlock()
	if v&1 == 1 {
		return 0, false
	}
	return v, true
}

func (n *node) lockCheck(version uint64) bool {
	if n == nil {
		return true
	}
	return n.rUnlock(version)
}

func (n *node) rUnlock(version uint64) bool {
	if n == nil {
		return true
	}
	return version == atomic.LoadUint64(&n.version)
}

func (n *node) rUnlockWithNode(version uint64, lockedNode *node) bool {
	if n == nil {
		return true
	}
	if version != atomic.LoadUint64(&n.version) {
		lockedNode.unlock()
		return false
	}
	return true
}

func (n *node) upgradeToLock(version uint64) bool {
	if n == nil {
		return true
	}
	return atomic.CompareAndSwapUint64(&n.version, version, version+2)
}

func (n *node) upgradeToLockWithNode(version uint64, lockedNode *node) bool {
	if n == nil {
		return true
	}
	if !atomic.CompareAndSwapUint64(&n.version, version, version+2) {
		lockedNode.unlock()
		return false
	}
	return true
}

func (n *node) lock() bool {
	for {
		version, ok := n.rLock()
		if !ok {
			return false
		}
		if n.upgradeToLock(version) {
			break
		}
	}

	return true
}

func (n *node) unlock() {
	if n == nil {
		return
	}
	atomic.AddUint64(&n.version, 2)
}

func (n *node) unlockObsolete() {
	if n == nil {
		return
	}
	atomic.AddUint64(&n.version, 3)
}

func (n *node) waitUnlock() uint64 {
	v := atomic.LoadUint64(&n.version)
	count := spinCount
	for v&2 == 2 {
		if count <= 0 {
			runtime.Gosched()
			count = spinCount
		}
		count--
		v = atomic.LoadUint64(&n.version)
	}
	return v
}

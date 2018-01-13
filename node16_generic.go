// +build !amd64

package art

func (n *node16) findChild(key byte) uint16 {
	// As measured in MassTree's paper, linear search may have better performance
	// than binary search on modern CPU.
	var i uint8
	for i = 0; i < n.numChildren; i++ {
		if n.keys[i] == key {
			return 1 << i
		}
	}
	return 0
}

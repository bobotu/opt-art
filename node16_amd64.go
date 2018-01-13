// +build amd64

package art

//go:noescape
func node16FindChildASM(keys *byte, key byte) uint16

func (n *node16) findChild(key byte) uint16 {
	var mask uint16 = (1 << n.numChildren) - 1
	return node16FindChildASM(&n.keys[0], key) & mask
}

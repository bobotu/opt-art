// +build amd64

package art

var useAVX2 bool

func init() {
	useAVX2 = supportAVX2()
}

//go:noescape
func node16FindChildAVX2(keys *byte, key byte, nc uint8) uint8

//go:noescape
func supportAVX2() bool

func (n *node16) findChild(key byte) uint8 {
	if useAVX2 {
		i := node16FindChildAVX2(&n.keys[0], key, n.numChildren)
		return i
	}
	return n.findChildLinear(key)
}

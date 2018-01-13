// +build amd64

package art

var useSSE2 bool

func init() {
	useSSE2 = supportSSE2()
}

//go:noescape
func node16FindChildASM(keys *byte, key byte) uint16

//go:noescape
func supportSSE2() bool

func (n *node16) findChild(key byte) uint16 {
	if useSSE2 {
		var mask uint16 = (1 << n.numChildren) - 1
		return node16FindChildASM(&n.keys[0], key) & mask
	}
	return n.findChildLinear(key)
}

// +build !amd64

package art

func (n *node16) findChild(key byte) uint16 {
	return n.findChildLinear(key)
}

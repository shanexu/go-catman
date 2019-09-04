package catman

import (
	"strconv"
	"strings"

	"github.com/emirpasic/gods/utils"
)

type ZNodeName struct {
	name     string
	prefix   string
	sequence int
}

func NewZNodeName(name string) *ZNodeName {
	z := &ZNodeName{
		name:     name,
		prefix:   name,
		sequence: -1,
	}
	idx := strings.LastIndex(name, "-")
	if idx >= 0 {
		z.prefix = name[0:idx]
		seq, err := strconv.ParseInt(name[idx+1:], 10, 64)
		if err == nil {
			z.sequence = int(seq)
		}
	}
	return z
}

func (n *ZNodeName) Name() string {
	return n.name
}

var ZNodeNameComparator = utils.Comparator(func(a interface{}, b interface{}) int {
	aAsserted := a.(*ZNodeName)
	bAsserted := b.(*ZNodeName)

	answer := aAsserted.sequence - bAsserted.sequence
	if answer != 0 {
		return answer
	}
	return utils.StringComparator(aAsserted.prefix, bAsserted.prefix)
})

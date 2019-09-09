package catman

import (
	"github.com/emirpasic/gods/utils"
)

type LeaderOffer struct {
	id       int
	nodePath string
	hostName string
}

func NewLeaderOffer(id int, nodePath string, hostName string) *LeaderOffer {
	return &LeaderOffer{id, nodePath, hostName}
}

func (l *LeaderOffer) Id() int {
	return l.id
}

func (l *LeaderOffer) SetId(id int) {
	l.id = id
}

func (l *LeaderOffer) NodePath() string {
	return l.nodePath
}

func (l *LeaderOffer) SetNodePath(nodePath string) {
	l.nodePath = nodePath
}

func (l *LeaderOffer) HostName() string {
	return l.hostName
}

func (l *LeaderOffer) SetHostName(hostName string) {
	l.hostName = hostName
}

var LeaderOfferComparator = utils.Comparator(func(a interface{}, b interface{}) int {
	aAsserted := a.(*LeaderOffer)
	bAsserted := b.(*LeaderOffer)

	return utils.IntComparator(aAsserted.id, bAsserted.id)
})

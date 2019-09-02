package catman

import (
	"strconv"
	"strings"

	"github.com/emirpasic/gods/maps/treemap"
	"github.com/samuel/go-zookeeper/zk"
	"math"
)

const prefix = "qn-"

type DistributedQueue struct {
	conn *CatMan
	dir  string
}

func (q *DistributedQueue) Element() []byte {
	return nil
}

func (q *DistributedQueue) Remove() []byte {
	return nil
}

func (q *DistributedQueue) Take() []byte {
	return nil
}

func (q *DistributedQueue) Offer() []byte {
	return nil
}

func (q *DistributedQueue) Peek() []byte {
	return nil
}

func (q *DistributedQueue) Poll() []byte {
	return nil
}

func (q *DistributedQueue) children2Ordered(childNames []string) *treemap.Map {
	orderedChildren := treemap.NewWithIntComparator()
	for _, childName := range childNames {
		if !strings.HasPrefix(childName, prefix) {
			continue
		}
		suffix := childName[len(prefix):]
		childId, err := strconv.ParseInt(suffix, 10, 64)
		if err != nil {
			// number format error
			continue
		}
		orderedChildren.Put(int(childId), childName)
	}
	return orderedChildren
}

func (q *DistributedQueue) orderedChildren() (*treemap.Map, error) {
	childNames, err := q.conn.Children(q.dir)
	if err != nil {
		return nil, err
	}

	orderedChildren := q.children2Ordered(childNames)
	return orderedChildren, nil
}

func (q *DistributedQueue) orderedChildrenW() (*treemap.Map, <-chan zk.Event, error) {
	childNames, events, err := q.conn.ChildrenW(q.dir)
	if err != nil {
		return nil, nil, err
	}

	orderedChildren := q.children2Ordered(childNames)
	return orderedChildren, events, nil
}

func (q *DistributedQueue) smallestChildName() (string, error) {
	childNames, err := q.conn.Children(q.dir)
	if err != nil {
		return "", err
	}

	var minId int64 = math.MaxInt64
	var minName string
	for _, childName := range childNames {
		if !strings.HasPrefix(childName, prefix) {
			continue
		}
		suffix := childName[len(prefix):]
		childId, err := strconv.ParseInt(suffix, 10, 64)
		if err != nil {
			// number format error
			continue
		}
		if childId < minId {
			minId = childId
			minName = childName
		}
	}

	return minName, nil
}

func (cm *CatMan) NewDistributedQueue(dir string) *DistributedQueue {
	return &DistributedQueue{
		conn: cm,
		dir:  dir,
	}
}

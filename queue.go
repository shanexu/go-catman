package catman

import (
	"errors"
	"math"
	"strconv"
	"strings"

	"github.com/emirpasic/gods/maps/treemap"
	"github.com/samuel/go-zookeeper/zk"

	"github.com/shanexu/go-catman/utils"
)

const prefix = "qn-"

var (
	ErrNoSuchElement = errors.New("no such element")
)

type DistributedQueue struct {
	cm  *CatMan
	dir string
	log utils.Logger
}

// Return the head of the queue without modifying the queue.
func (q *DistributedQueue) Element() ([]byte, error) {
	for {
		orderedChildren, err := q.orderedChildren()
		if err != nil {
			if err == zk.ErrNoNode {
				return nil, ErrNoSuchElement
			}
			return nil, err
		}
		if orderedChildren.Size() == 0 {
			return nil, ErrNoSuchElement
		}

		for _, headNode := range orderedChildren.Values() {
			headNode, _ := headNode.(string)
			if headNode == "" {
				continue
			}
			data, err := q.cm.CMGet(q.dir + "/" + headNode)
			if err != nil {
				if err == zk.ErrNoNode {
					//Another client removed the node first, try next
					continue
				}
				return nil, err
			}
			return data, nil
		}
	}
}

// Attempts to remove the head of the queue and return it.
func (q *DistributedQueue) Remove() ([]byte, error) {
	for {
		orderedChildren, err := q.orderedChildren()
		if err != nil {
			if err == zk.ErrNoNode {
				return nil, ErrNoSuchElement
			}
			return nil, err
		}
		if orderedChildren.Size() == 0 {
			return nil, ErrNoSuchElement
		}

		for _, headNode := range orderedChildren.Values() {
			headNode, _ := headNode.(string)
			if headNode == "" {
				continue
			}
			path := q.dir + "/" + headNode
			data, err := q.cm.CMGet(path)
			if err != nil {
				if err == zk.ErrNoNode {
					//Another client removed the node first, try next
					continue
				}
				return nil, err
			}
			err = q.cm.CMDelete(path, -1)
			if err != nil {
				if err == zk.ErrNoNode {
					//Another client removed the node first, try next
					continue
				}
				return nil, err
			}
			return data, nil
		}

	}
}

// Removes the head of the queue and returns it, blocks until it succeeds.
func (q *DistributedQueue) Take() ([]byte, error) {
	for {
		orderedChildren, events, err := q.orderedChildrenW()
		if err != nil {
			if err == zk.ErrNoNode {
				if _, err := q.cm.CMCreate(q.dir, nil); err != nil {
					return nil, err
				}
				continue
			}
		}
		if orderedChildren.Size() == 0 {
			<-events
			continue
		}
		for _, headNode := range orderedChildren.Values() {
			headNode, _ := headNode.(string)
			if headNode == "" {
				continue
			}
			path := q.dir + "/" + headNode
			data, err := q.cm.CMGet(path)
			if err != nil {
				if err == zk.ErrNoNode {
					//Another client removed the node first, try next
					continue
				}
				return nil, err
			}
			err = q.cm.CMDelete(path, -1)
			if err != nil {
				if err == zk.ErrNoNode {
					//Another client removed the node first, try next
					continue
				}
				return nil, err
			}
			return data, nil
		}
	}
}

// Inserts data into queue.
func (q *DistributedQueue) Offer(data []byte) (bool, error) {
	for {
		_, err := q.cm.CMCreateSequential(q.dir+"/"+prefix, data)
		if err != nil {
			if err == zk.ErrNoNode {
				q.cm.CMCreate(q.dir, nil)
				continue
			}
			return false, err
		}
		return true, nil
	}
}

// Returns the data at the first element of the queue, or null if the queue is empty.
func (q *DistributedQueue) Peek() ([]byte, error) {
	data, err := q.Element()
	if err != nil {
		if err == ErrNoSuchElement {
			return nil, nil
		}
		return nil, err
	}
	return data, nil
}

// Attempts to remove the head of the queue and return it. Returns null if the queue is empty.
func (q *DistributedQueue) Poll() ([]byte, error) {
	data, err := q.Remove()
	if err != nil {
		if err == ErrNoSuchElement {
			return nil, nil
		}
		return nil, err
	}
	return data, nil
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
			q.log.Warnf("found child node with improper format : %s %s", childName, err)
			continue
		}
		orderedChildren.Put(int(childId), childName)
	}
	return orderedChildren
}

func (q *DistributedQueue) orderedChildren() (*treemap.Map, error) {
	childNames, err := q.cm.CMChildren(q.dir)
	if err != nil {
		return nil, err
	}

	orderedChildren := q.children2Ordered(childNames)
	return orderedChildren, nil
}

func (q *DistributedQueue) orderedChildrenW() (*treemap.Map, <-chan zk.Event, error) {
	childNames, events, err := q.cm.CMChildrenW(q.dir)
	if err != nil {
		return nil, nil, err
	}

	orderedChildren := q.children2Ordered(childNames)
	return orderedChildren, events, nil
}

func (q *DistributedQueue) smallestChildName() (string, error) {
	childNames, err := q.cm.CMChildren(q.dir)
	if err != nil {
		if err == zk.ErrNoNode {
			q.log.Warnf("caught: %s", err)
			return "", nil
		}
		return "", err
	}

	var minId int64 = math.MaxInt64
	var minName string
	for _, childName := range childNames {
		if !strings.HasPrefix(childName, prefix) {
			q.log.Warnf("found child node with improper name: %s", childName)
			continue
		}
		suffix := childName[len(prefix):]
		childId, err := strconv.ParseInt(suffix, 10, 64)
		if err != nil {
			q.log.Warnf("Found child node with improper format : %s %s", childName, err)
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
		cm:  cm,
		dir: dir,
	}
}

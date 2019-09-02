package catman

const queuePrefix = "qn-"

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

func (cm *CatMan) NewDistributedQueue(dir string) *DistributedQueue {
	return &DistributedQueue{
		conn: cm,
		dir:  dir,
	}
}

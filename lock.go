package catman

import (
	"go.uber.org/atomic"
	"time"
)

type RwLock struct {
	cm         *CatMan
	closed     *atomic.Bool
	retryDelay time.Duration
	retryCount int
}

func (cm *CatMan) NewRWLocker() *RwLock {
	return &RwLock{
		cm:         cm,
		closed:     atomic.NewBool(false),
		retryDelay: time.Millisecond * 500,
		retryCount: 10,
	}
}

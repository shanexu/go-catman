package catman

import (
	"time"

	"github.com/samuel/go-zookeeper/zk"
	"go.uber.org/atomic"
)

type RwLock struct {
	cm         *CatMan
	acl        []zk.ACL
	closed     *atomic.Bool
	retryDelay time.Duration
	retryCount int
}

// Performs the operation - which may be involved multiple times if the connection
// to ZooKeeper closes during this operation.
type ZooKeeperOperation func() (bool, error)

func (cm *CatMan) NewRWLocker() *RwLock {
	return &RwLock{
		cm:         cm,
		closed:     atomic.NewBool(false),
		retryDelay: time.Millisecond * 500,
		retryCount: 10,
	}
}

// Closes this strategy and releases any ZooKeeper resources; but keeps the
// ZooKeeper instance open.
func (l *RwLock) Close() {
	if l.closed.CAS(false, true) {
		l.doClose()
	}
}

// return zookeeper client instance.
func (l *RwLock) CatMan() *CatMan {
	return l.cm
}

// return the acl its using.
func (l *RwLock) Acl() []zk.ACL {
	return l.acl
}

// set the acl.
func (l *RwLock) SetAcl(acl []zk.ACL) {
	l.acl = acl
}

// get the retry delay
func (l *RwLock) RetryDelay() time.Duration {
	return l.retryDelay
}

// Sets the time waited between retry delays.
func (l *RwLock) SetRetryDelay(retryDelay time.Duration) {
	l.retryDelay = retryDelay
}

// Perform the given operation, retrying if the connection fails.
func (l *RwLock) retryOperation(operation ZooKeeperOperation) (interface{}, error) {
	var error error
	for i := 0; i < l.retryCount; i++ {
		result, err := operation()
		if err == nil {
			return result, nil
		}
		if err == zk.ErrSessionExpired {
			return nil, err
		}
		if err == zk.ErrConnectionClosed {
			error = err
		}
		l.retryWait(i)

	}
	return nil, error
}

// Ensures that the given path exists with no data, the current
func (l *RwLock) ensurePathExists(path string) error {
	return l.ensureExists(path, nil, l.acl, 0)
}

// Ensures that the given path exists with the given data, ACL and flags.
func (l *RwLock) ensureExists(path string, data []byte, acl []zk.ACL, flag int32) error {
	_, err := l.retryOperation(func() (bool, error) {
		ok, _, err := l.cm.Conn().Exists(path)
		if err != nil {
			return false, err
		}
		if ok {
			return true, nil
		}
		_, err = l.cm.Create(path, data, func(c *CreateConfig) {
			c.Flag = flag
			c.ACL = acl
		})
		if err == nil {
			return true, nil
		}
		if err == zk.ErrNodeExists {
			return true, nil
		}
		return false, err
	})
	return err
}

func (l *RwLock) doClose() {

}

func (l *RwLock) retryWait(attemptCount int) {
	if attemptCount > 0 {
		time.Sleep(l.retryDelay * time.Duration(attemptCount))
	}
}

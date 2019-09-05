package catman

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/emirpasic/gods/sets/treeset"
	"github.com/samuel/go-zookeeper/zk"
	"go.uber.org/atomic"
)

type Lock struct {
	cm         *CatMan
	acl        []zk.ACL
	closed     *atomic.Bool
	retryDelay time.Duration
	retryCount int
	dir        string
	id         string
	idName     *ZNodeName
	ownerId    string
	data       []byte
	l          sync.Mutex
	callback   LockListener
}

// Performs the operation - which may be involved multiple times if the connection
// to ZooKeeper closes during this operation.
type ZooKeeperOperationFunc func() (bool, error)

func (f ZooKeeperOperationFunc) Execute() (bool, error) {
	return f()
}

type ZooKeeperOperation interface {
	Execute() (bool, error)
}

type LockListener interface {
	LockAcquired()
	LockReleased()
}

func (cm *CatMan) NewLock(dir string, acl []zk.ACL, callback LockListener) *Lock {
	l := &Lock{
		cm:         cm,
		closed:     atomic.NewBool(false),
		retryDelay: time.Millisecond * 500,
		retryCount: 10,
		dir:        dir,
		acl:        acl,
		data:       []byte{0x12, 0x34},
		callback:   callback,
	}
	return l
}

// Closes this strategy and releases any ZooKeeper resources; but keeps the
// ZooKeeper instance open.
func (l *Lock) Close() {
	if l.closed.CAS(false, true) {
		l.doClose()
	}
}

// Returns true if this protocol has been closed.
func (l *Lock) Closed() bool {
	return l.closed.Load()
}

// return the current locklistener.
func (l *Lock) LockListener() LockListener {
	return l.callback
}

// register a different call back listener.
func (l *Lock) SetLockListener(callback LockListener) {
	l.callback = callback
}

// return zookeeper client instance.
func (l *Lock) CatMan() *CatMan {
	return l.cm
}

// return the acl its using.
func (l *Lock) Acl() []zk.ACL {
	return l.acl
}

// set the acl.
func (l *Lock) SetAcl(acl []zk.ACL) {
	l.acl = acl
}

// get the retry delay
func (l *Lock) RetryDelay() time.Duration {
	return l.retryDelay
}

// Sets the time waited between retry delays.
func (l *Lock) SetRetryDelay(retryDelay time.Duration) {
	l.retryDelay = retryDelay
}

// Perform the given operation, retrying if the connection fails.
func (l *Lock) retryOperation(operation ZooKeeperOperation) (bool, error) {
	var error error
	for i := 0; i < l.retryCount; i++ {
		result, err := operation.Execute()
		if err == nil {
			return result, nil
		}
		if err == zk.ErrSessionExpired {
			return false, err
		}
		if err == zk.ErrConnectionClosed {
			error = err
			l.retryWait(i)
			continue
		}
		return false, err
	}
	return false, error
}

// Ensures that the given path exists with no data, the current
func (l *Lock) ensurePathExists(path string) error {
	return l.ensureExists(path, nil, l.acl, 0)
}

// Ensures that the given path exists with the given data, ACL and flags.
func (l *Lock) ensureExists(path string, data []byte, acl []zk.ACL, flag int32) error {
	_, err := l.retryOperation(ZooKeeperOperationFunc(func() (bool, error) {
		ok, _, err := l.cm.Exists(path)
		if err != nil {
			return false, err
		}
		if ok {
			return true, nil
		}
		_, err = l.cm.CMCreate(path, data, func(c *CreateConfig) {
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
	}))
	return err
}

func (l *Lock) doClose() {

}

func (l *Lock) retryWait(attemptCount int) {
	if attemptCount > 0 {
		time.Sleep(l.retryDelay * time.Duration(attemptCount))
	}
}

func (l *Lock) Lock() (bool, error) {
	l.l.Lock()
	defer l.l.Unlock()
	if l.Closed() {
		return false, nil
	}
	l.ensurePathExists(l.dir)

	return l.retryOperation(ZooKeeperOperationFunc(l.zop))
}

func (l *Lock) Unlock() error {
	l.l.Lock()
	defer l.l.Unlock()
	if !l.Closed() && l.id != "" {
		err := l.cm.Delete(l.id, -1)
		if err != nil && err != zk.ErrNoNode {
			return err
		}
		defer func() {
			if l.callback != nil {
				l.callback.LockReleased()
			}
			l.id = ""
		}()
		return nil
	}
	return nil
}

func (l *Lock) IsOwner() bool {
	return l.id != "" && l.id == l.ownerId
}

// find if we have been created earler if not create our node.
func (l *Lock) findPrefixInChildren(prefix string, dir string) error {
	names, err := l.cm.CMChildren(dir)
	if err != nil {
		return err
	}
	for _, name := range names {
		if strings.HasPrefix(name, prefix) {
			l.id = name
			break
		}
	}
	if l.id == "" {
		id, _, err := l.cm.CMCreateEphemeralSequential(dir+"/"+prefix, l.data, func(c *CreateConfig) { c.ACL = l.acl })
		if err != nil {
			return err
		}
		l.id = id
		l.idName = NewZNodeName(id)
	}
	return nil
}

func (l *Lock) zop() (bool, error) {
	for {
		if l.id == "" {
			sessionID := l.cm.SessionID()
			prefix := fmt.Sprintf("x-%d-", sessionID)
			err := l.findPrefixInChildren(prefix, l.dir)
			if err != nil {
				return false, err
			}
			l.idName = NewZNodeName(l.id)
		}
		names, err := l.cm.CMChildren(l.dir)
		if err != nil {
			return false, err
		}
		if len(names) == 0 {
			l.id = ""
		} else {
			sortedNames := treeset.NewWith(ZNodeNameComparator)
			for _, name := range names {
				sortedNames.Add(NewZNodeName(l.dir + "/" + name))
			}
			it := sortedNames.Iterator()
			it.Next()
			l.ownerId = it.Value().(*ZNodeName).Name()
			lastChild := largestLessThan(sortedNames, l.idName)
			if lastChild != nil {
				lastChildId := lastChild.Name()
				ok, _, events, err := l.cm.ExistsW(lastChildId) // watch
				if err != nil && err != zk.ErrNoNode {
					return false, err
				}
				if !ok {
				} else {
					<-events
				}
			} else {
				if l.IsOwner() {
					if l.callback != nil {
						l.callback.LockAcquired()
					}
				}
				return true, nil
			}
		}
		if l.id != "" {
			break
		}
	}
	return false, nil
}

func largestLessThan(sortedNames *treeset.Set, idNmae *ZNodeName) (z *ZNodeName) {
	it := sortedNames.Iterator()
	for it.Next() {
		child := it.Value().(*ZNodeName)
		if ZNodeNameComparator(child, idNmae) >= 0 {
			return
		}
		z = child
	}
	return
}

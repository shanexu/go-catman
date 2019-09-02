package catman

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/samuel/go-zookeeper/zk"
)

var (
	ErrBadPath = errors.New("bad path")
)

type ErrUnexpectedEvent struct {
	zk.EventType
}

func (ue *ErrUnexpectedEvent) Error() string {
	return fmt.Sprintf("unexpected event %v", ue.EventType)
}

type CatMan struct {
	conn *zk.Conn
}

func NewCatMan(conn *zk.Conn) *CatMan {
	return &CatMan{conn}
}

func (cm *CatMan) Conn() *zk.Conn {
	return cm.conn
}

func (cm *CatMan) CreatePath(path string, data []byte) (string, error) {
	return cm.conn.Create(path, data, 0, defaultACL())
}

func (cm *CatMan) CreateSequential(pathPrefix string, data []byte) (string, error) {
	return cm.Conn().Create(pathPrefix, data, zk.FlagSequence, defaultACL())
}

func (cm *CatMan) CreateProtectedEphemeralSequential(
	path string,
	data []byte,
) (string, int64, error) {
	path, err := cm.conn.CreateProtectedEphemeralSequential(path, data, defaultACL())
	if err != nil {
		return "", 0, err
	}
	seq, err := path2Seq(path)
	return path, seq, err
}

func path2Seq(path string) (int64, error) {
	idx := strings.LastIndex(path, "-")
	if idx == -1 {
		return 0, ErrBadPath
	}
	seq, err := strconv.ParseInt(path[idx+1:], 10, 64)
	return seq, err
}

func (cm *CatMan) Watch(ctx context.Context, path string, ch chan<- []byte) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			value, _, events, err := cm.conn.GetW(path)
			if err != nil {
				return err
			}
			ch <- value
			select {
			case <-ctx.Done():
				return nil
			case e := <-events:
				switch e.Type {
				case zk.EventNodeDataChanged:
					continue
				default:
					return &ErrUnexpectedEvent{e.Type}
				}
			}
		}
	}
}

func (cm *CatMan) WatchDeletion(ctx context.Context, path string) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			_, _, events, err := cm.conn.GetW(path)
			if err != nil {
				return err
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case e := <-events:
				switch e.Type {
				case zk.EventNodeDataChanged:
					continue
				case zk.EventNodeDeleted:
					return nil
				default:
					return &ErrUnexpectedEvent{e.Type}
				}
			}
		}
	}
}

func (cm *CatMan) WatchChildren(
	ctx context.Context,
	path string,
	ch chan<- []string,
) error {
	var nodeDataChanged bool
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			value, _, events, err := cm.conn.ChildrenW(path)
			if err != nil {
				return err
			}
			if !nodeDataChanged {
				ch <- value
			}
			select {
			case <-ctx.Done():
				return nil
			case e := <-events:
				switch e.Type {
				case zk.EventNodeDataChanged:
					nodeDataChanged = true
				case zk.EventNodeChildrenChanged:
					nodeDataChanged = false
				default:
					return &ErrUnexpectedEvent{e.Type}
				}
			}
		}
	}
}

func (cm *CatMan) Children(parent string) ([]string, error) {
	children, _, err := cm.conn.Children(parent)
	return children, err
}

func (cm *CatMan) ChildrenW(parent string) ([]string, <-chan zk.Event, error) {
	children, _, events, err := cm.conn.ChildrenW(parent)
	return children, events, err
}

func defaultACL() []zk.ACL {
	const perm = zk.PermAdmin | zk.PermRead | zk.PermWrite | zk.PermCreate |
		zk.PermDelete
	return zk.WorldACL(perm)
}

type Watcher interface {
	Process(zk.Event)
}

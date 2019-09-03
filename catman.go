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

var (
	OpenAclUnsafe = zk.WorldACL(zk.PermAll)
	CreatorAllAcl = zk.AuthACL(zk.PermAll)
	ReadAclUnsafe = zk.WorldACL(zk.PermRead)
)

type ErrUnexpectedEvent struct {
	zk.EventType
}

func (ue *ErrUnexpectedEvent) Error() string {
	return fmt.Sprintf("unexpected event %v", ue.EventType)
}

type CatMan struct {
	conn       *zk.Conn
	defaultACL []zk.ACL
}

func NewCatMan(conn *zk.Conn) *CatMan {
	return &CatMan{conn, OpenAclUnsafe}
}

func NewCatManWithOption(conn *zk.Conn, acl []zk.ACL) *CatMan {
	return &CatMan{conn, acl}
}

func (cm *CatMan) Conn() *zk.Conn {
	return cm.conn
}

func (cm *CatMan) Get(path string) ([]byte, error) {
	data, _, err := cm.conn.Get(path)
	return data, err
}

func (cm *CatMan) Delete(path string, version int32) error {
	return cm.conn.Delete(path, version)
}

type CreateConfig struct {
	Flag int32
	ACL  []zk.ACL
}

type CreateConfigOption func(*CreateConfig)

func (cm *CatMan) Create(path string, data []byte, opts ...CreateConfigOption) (string, error) {
	c := &CreateConfig{
		Flag: 0,
		ACL:  cm.defaultACL,
	}

	for _, opt := range opts {
		opt(c)
	}
	return cm.conn.Create(path, data, c.Flag, c.ACL)
}

func (cm *CatMan) CreateSequential(pathPrefix string, data []byte, opts ...CreateConfigOption) (string, error) {
	c := &CreateConfig{
		Flag: 0,
		ACL:  cm.defaultACL,
	}

	for _, opt := range opts {
		opt(c)
	}

	return cm.conn.Create(pathPrefix, data, zk.FlagSequence, c.ACL)
}

func (cm *CatMan) CreateEphemeralSequential(pathPrefix string, data []byte, opts ...CreateConfigOption) (string, int64, error) {
	c := &CreateConfig{
		Flag: 0,
		ACL:  cm.defaultACL,
	}

	for _, opt := range opts {
		opt(c)
	}

	path, err := cm.conn.Create(pathPrefix, data, zk.FlagSequence|zk.FlagEphemeral, c.ACL)
	if err != nil {
		return "", 0, err
	}
	seq, err := path2Seq(path)
	return path, seq, err
}

func (cm *CatMan) CreateProtectedEphemeralSequential(
	path string,
	data []byte,
	opts ...CreateConfigOption,
) (string, int64, error) {
	c := &CreateConfig{
		Flag: 0,
		ACL:  cm.defaultACL,
	}

	for _, opt := range opts {
		opt(c)
	}

	path, err := cm.conn.CreateProtectedEphemeralSequential(path, data, c.ACL)
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

type Watcher interface {
	Process(zk.Event)
}

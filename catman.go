package catman

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/samuel/go-zookeeper/zk"

	"github.com/shanexu/go-catman/utils"
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
	*zk.Conn
	defaultACL []zk.ACL
	wg         sync.WaitGroup
	log        utils.Logger
}

type CatManConfig struct {
	ACL     []zk.ACL
	Watcher Watcher
	Log     utils.Logger
}

type CatManConfigOption func(*CatManConfig)

func NewCatMan(servers []string, sessionTimeout time.Duration, opts ...CatManConfigOption) (*CatMan, error) {
	conn, events, err := zk.Connect(servers, sessionTimeout)
	if err != nil {
		return nil, err
	}
	l := utils.NewSimpleLogger()
	c := &CatManConfig{
		ACL:     OpenAclUnsafe,
		Watcher: defaultWatchFuncGen(l),
		Log:     l,
	}
	for _, opt := range opts {
		opt(c)
	}
	cm := &CatMan{
		Conn:       conn,
		defaultACL: c.ACL,
		log:        c.Log,
	}
	cm.wg.Add(1)
	cm.processEvents(events, c.Watcher)
	return cm, nil
}

func (cm *CatMan) Close() {
	cm.Conn.Close()
	cm.wg.Wait()
}

func (cm *CatMan) processEvents(events <-chan zk.Event, watcher Watcher) {
	go func() {
		for event := range events {
			watcher.Process(event)
		}
		cm.wg.Done()
	}()
}

func (cm *CatMan) CMGet(path string) ([]byte, error) {
	data, _, err := cm.Conn.Get(path)
	return data, err
}

func (cm *CatMan) CMDelete(path string, version int32) error {
	return cm.Conn.Delete(path, version)
}

type CreateConfig struct {
	Flag int32
	ACL  []zk.ACL
}

type CreateConfigOption func(*CreateConfig)

func (cm *CatMan) CMCreate(path string, data []byte, opts ...CreateConfigOption) (string, error) {
	c := &CreateConfig{
		Flag: 0,
		ACL:  cm.defaultACL,
	}

	for _, opt := range opts {
		opt(c)
	}
	return cm.Conn.Create(path, data, c.Flag, c.ACL)
}

func (cm *CatMan) CMCreateSequential(pathPrefix string, data []byte, opts ...CreateConfigOption) (string, error) {
	c := &CreateConfig{
		Flag: 0,
		ACL:  cm.defaultACL,
	}

	for _, opt := range opts {
		opt(c)
	}

	return cm.Conn.Create(pathPrefix, data, zk.FlagSequence, c.ACL)
}

func (cm *CatMan) CMCreateEphemeralSequential(pathPrefix string, data []byte, opts ...CreateConfigOption) (string, int64, error) {
	c := &CreateConfig{
		Flag: 0,
		ACL:  cm.defaultACL,
	}

	for _, opt := range opts {
		opt(c)
	}

	path, err := cm.Conn.Create(pathPrefix, data, zk.FlagSequence|zk.FlagEphemeral, c.ACL)
	if err != nil {
		return "", 0, err
	}
	seq, err := path2Seq(path)
	return path, seq, err
}

func (cm *CatMan) CMCreateProtectedEphemeralSequential(
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

	path, err := cm.Conn.CreateProtectedEphemeralSequential(path, data, c.ACL)
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

func (cm *CatMan) CMChildren(parent string, watcher Watcher) ([]string, error) {
	if watcher == nil {
		children, _, err := cm.Conn.Children(parent)
		return children, err
	}
	children, _, events, err := cm.Conn.ChildrenW(parent)
	go func() {
		event := <-events
		watcher.Process(event)
	}()
	return children, err
}

func (cm *CatMan) CMChildrenW(parent string) ([]string, <-chan zk.Event, error) {
	children, _, events, err := cm.Conn.ChildrenW(parent)
	return children, events, err
}

func (cm *CatMan) Subscribe(ctx context.Context, path string, ch chan<- []byte) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			value, _, events, err := cm.Conn.GetW(path)
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

func (cm *CatMan) CMExists(path string, watcher Watcher) (*zk.Stat, error) {
	if watcher == nil {
		_, stat, err := cm.Conn.Exists(path)
		return stat, err
	}
	_, stat, events, err := cm.Conn.ExistsW(path)
	if err != nil {
		return stat, err
	}
	go func() {
		event := <-events
		watcher.Process(event)
	}()
	return stat, err
}

func (cm *CatMan) SubscribeExistence(ctx context.Context, path string) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			_, _, events, err := cm.Conn.ExistsW(path)
			if err != nil {
				return err
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case e := <-events:
				switch e.Type {
				case zk.EventNodeDeleted:
					return nil
				default:
					return &ErrUnexpectedEvent{e.Type}
				}
			}
		}
	}
}

func (cm *CatMan) SubscribeChildren(
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
			value, _, events, err := cm.Conn.ChildrenW(path)
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

type Watcher interface {
	Process(zk.Event)
}

type WatcherFunc func(zk.Event)

func (f WatcherFunc) Process(event zk.Event) {
	f(event)
}

func defaultWatchFuncGen(log utils.Logger) WatcherFunc {
	{
		log := log
		return func(event zk.Event) {
			log.Debugf("receive events %+v", event)
		}
	}
}

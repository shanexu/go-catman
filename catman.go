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
	ErrNotInCandidates = errors.New("not in candidates")
	ErrBadPath         = errors.New("bad path")
)

type CatMan struct {
	conn *zk.Conn
}

func NewCatMan(conn *zk.Conn) *CatMan {
	return &CatMan{conn}
}

func (cm *CatMan) CreatePath(path string, data []byte) (string, error) {
	return cm.conn.Create(path, data, 0, defaultACL())
}

func (cm *CatMan) CreateEphemeralSequential(
	path string,
	data []byte,
) (string, int64, error) {
	path, err := cm.conn.CreateProtectedEphemeralSequential(path, data, defaultACL())
	if err != nil {
		return "", 0, err
	}
	seq, err := pathToSeq(path)
	return path, seq, err
}

func pathToSeq(path string) (int64, error) {
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

type TakeLeaderShip = func()

func (cm *CatMan) LeaderElector(
	ctx context.Context,
	takeLeaderShip TakeLeaderShip,
	parent string,
	data []byte,
) error {
STEP1:
	path, seq, err := cm.CreateEphemeralSequential(parent+"/", data)
	if err != nil {
		return err
	}
STEP2:
	children, err := cm.Children(parent)
	if err != nil {
		return err
	}
	cs := childrenToCandidate(parent, children)
	self := candidate{path, seq}
	j, err := findCandidateJ(cs, self)
	if err != nil {
		if err != ErrNotInCandidates {
			return err
		}
		goto STEP1
	}
	if j == nil {
		takeLeaderShip()
	}
	err = cm.WatchDeletion(ctx, j.path)
	if err != nil && err != zk.ErrNoNode {
		return err
	}
	goto STEP2
}

type candidate struct {
	path string
	seq  int64
}

func childrenToCandidate(parent string, children []string) (cs []candidate) {
	for _, path := range children {
		seq, err := pathToSeq(path)
		if err != nil {
			continue
		}
		cs = append(cs, candidate{parent + "/" + path, seq})
	}
	return
}

func findCandidateJ(cs []candidate, self candidate) (*candidate, error) {
	var in bool
	j := candidate{"", -1}
	for _, c := range cs {
		if !in && c.seq == self.seq {
			in = true
			continue
		}
		if j.seq < self.seq && j.seq < c.seq {
			j = c
		}
	}
	if !in {
		return nil, ErrNotInCandidates
	}
	if j.seq == -1 {
		return nil, nil
	}
	return &j, nil
}

func seqIn(seqs []int64, seq int64) bool {
	for _, s := range seqs {
		if s == seq {
			return true
		}
	}
	return false
}

type ErrUnexpectedEvent struct {
	zk.EventType
}

func (ue *ErrUnexpectedEvent) Error() string {
	return fmt.Sprintf("unexpected event %v", ue.EventType)
}

func defaultACL() []zk.ACL {
	const perm = zk.PermAdmin | zk.PermRead | zk.PermWrite | zk.PermCreate |
		zk.PermDelete
	return zk.WorldACL(perm)
}

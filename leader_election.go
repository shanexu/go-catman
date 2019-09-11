package catman

import (
	"context"
	"errors"

	"github.com/samuel/go-zookeeper/zk"
)

var (
	ErrNotInCandidates = errors.New("not in candidates")
)

type TakeLeaderShip = func()

func (cm *CatMan) LeaderElector(
	ctx context.Context,
	takeLeaderShip TakeLeaderShip,
	parent string,
	data []byte,
) error {
STEP1:
	path, seq, err := cm.CMCreateProtectedEphemeralSequential(parent+"/", data)
	if err != nil {
		return err
	}
STEP2:
	children, err := cm.CMChildren(parent, nil)
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
	err = cm.SubscribeExistence(ctx, j.path)
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
		seq, err := path2Seq(path)
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

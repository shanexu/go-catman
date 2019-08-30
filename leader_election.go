package catman

import (
	"errors"
)

var (
	ErrNotInCandidates = errors.New("not in candidates")
)

type TakeLeaderShip = func()

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

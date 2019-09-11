package catman

import (
	"errors"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/samuel/go-zookeeper/zk"
)

type LeaderElectionSupport struct {
	cm           *CatMan
	state        ElectionState
	listeners    []LeaderElectionAware
	rootNodeName string
	leaderOffer  *LeaderOffer
	hostName     string
	l            sync.Mutex
	ll           sync.RWMutex
}

func (cm *CatMan) NewLeaderElectionSupport(hostName, rootNodeName string) *LeaderElectionSupport {
	return &LeaderElectionSupport{
		state:        ElectionStateStop,
		cm:           cm,
		hostName:     hostName,
		rootNodeName: rootNodeName,
	}
}

func (l *LeaderElectionSupport) Start() (err error) {
	l.l.Lock()
	defer l.l.Unlock()

	if l.cm == nil {
		return errors.New("cm is nill")
	}

	if l.hostName == "" {
		return errors.New("hostname is empty")
	}

	defer func() {
		if err != nil {
			l.becomeFailed(err)
		}
	}()

	err = l.makeOffer()
	if err != nil {
		return
	}
	err = l.determineElectionStatus()
	if err != nil {
		return
	}
	return nil
}

func (l *LeaderElectionSupport) Stop() (err error) {
	l.l.Lock()
	defer l.l.Unlock()
	l.state = ElectionStateStop
	l.dispatchEvent(ElectionEventStopStart)

	if l.leaderOffer != nil {
		err = l.cm.Delete(l.leaderOffer.NodePath(), -1)
		defer func() {
			if err != nil {
				l.becomeFailed(err)
			}
		}()
		if err != nil {
			return
		}
	}

	l.dispatchEvent(ElectionEventStopComplete)
	return nil
}

func (l *LeaderElectionSupport) makeOffer() error {
	l.state = ElectionStateOffer
	l.dispatchEvent(ElectionEventOfferStart)

	newLeaderOffer := &LeaderOffer{}
	newLeaderOffer.SetHostName(l.hostName)
	nodePath, err := l.cm.Create(
		l.rootNodeName+"/"+"n_",
		[]byte(l.hostName),
		zk.FlagEphemeral|zk.FlagSequence,
		OpenAclUnsafe,
	)
	if err != nil {
		return err
	}
	newLeaderOffer.SetNodePath(nodePath)
	l.leaderOffer = newLeaderOffer

	l.dispatchEvent(ElectionEventOfferComplete)

	return nil
}

func (l *LeaderElectionSupport) determineElectionStatus() error {
	l.state = ElectionStateDetermine
	l.dispatchEvent(ElectionEventDetermineStart)

	currentLeaderOffer := l.leaderOffer

	components := strings.Split(currentLeaderOffer.NodePath(), "/")
	id, err := strconv.ParseInt(components[len(components)-1][len("n_"):], 10, 64)
	if err != nil {
		return err
	}
	currentLeaderOffer.SetId(int(id))

	cs, err := l.cm.CMChildren(l.rootNodeName, nil)
	if err != nil {
		return err
	}
	leaderOffers, err := l.toLeaderOffers(cs)
	if err != nil {
		return err
	}

	for i := range leaderOffers {
		leaderOffer := leaderOffers[i]
		if leaderOffer.Id() == currentLeaderOffer.Id() {
			l.dispatchEvent(ElectionEventDetermineComplete)

			if i == 0 {
				l.becomeLeader()
			} else {
				l.becomeReady(leaderOffers[i-1])
			}
			break
		}
	}

	return nil
}

func (l *LeaderElectionSupport) becomeReady(neighborLeaderOffer *LeaderOffer) error {
	ok, _, events, err := l.cm.ExistsW(neighborLeaderOffer.NodePath())
	if err != nil {
		return err
	}
	if ok {
		event := <-events
		l.process(event)
		l.dispatchEvent(ElectionEventReadyStart)
		l.state = ElectionStateReady
		l.dispatchEvent(ElectionEventReadyComplete)
	} else {
		l.determineElectionStatus()
	}
	return nil
}

func (l *LeaderElectionSupport) becomeLeader() error {
	l.state = ElectionStateElected
	l.dispatchEvent(ElectionEventElectedStart)
	l.dispatchEvent(ElectionEventElectedComplete)
	return nil
}

func (l *LeaderElectionSupport) becomeFailed(err error) error {
	l.state = ElectionStateFailed
	l.dispatchEvent(ElectionEventFailed)
	return nil
}

func (l *LeaderElectionSupport) GetLeaderHostName() (string, error) {
	cs, err := l.cm.CMChildren(l.rootNodeName, nil)
	if err != nil {
		return "", err
	}
	leaderOffers, err := l.toLeaderOffers(cs)
	if err != nil {
		return "", err
	}
	if len(leaderOffers) > 0 {
		return leaderOffers[0].HostName(), nil
	}
	return "", nil
}

func (l *LeaderElectionSupport) toLeaderOffers(strings []string) ([]*LeaderOffer, error) {
	var leaderOffers []*LeaderOffer

	for _, offer := range strings {
		data, _, err := l.cm.Get(l.rootNodeName + "/" + offer)
		if err != nil {
			return nil, err
		}
		id, err := strconv.ParseInt(offer[len("n_"):], 10, 64)
		if err != nil {
			return nil, err
		}
		hostName := string(data)
		leaderOffers = append(leaderOffers, NewLeaderOffer(int(id), l.rootNodeName+"/"+offer, hostName))
	}
	sort.SliceStable(leaderOffers, func(i int, j int) bool {
		return LeaderOfferComparator(leaderOffers[i], leaderOffers[j]) == -1
	})
	return leaderOffers, nil
}

func (l *LeaderElectionSupport) process(event zk.Event) error {
	if event.Type == zk.EventNodeDeleted {
		if event.Path != l.leaderOffer.NodePath() && l.state != ElectionStateStop {
			err := l.determineElectionStatus()
			if err != nil {
				l.becomeFailed(err)
			}
		}
	}
	return nil
}

func (l *LeaderElectionSupport) dispatchEvent(event ElectionEvent) {
	l.ll.RLock()
	defer l.ll.RUnlock()
	for _, observer := range l.listeners {
		observer.OnElectionEvent(event)
	}
}

func (l *LeaderElectionSupport) AddListener(listener LeaderElectionAware) {
	l.ll.Lock()
	defer l.ll.Unlock()
	l.listeners = append(l.listeners, listener)
}

func (l *LeaderElectionSupport) RemoveListener(listener LeaderElectionAware) {
	l.ll.Lock()
	defer l.ll.Unlock()
	i := 0
	for ; i < len(l.listeners); i++ {
		if listener == l.listeners[i] {
			break
		}
	}
	if i == len(l.listeners) {
		return
	}
	l.listeners = append(l.listeners[0:i], l.listeners[i+1:]...)
}

func (l *LeaderElectionSupport) SetRootNodeName(rootNodeName string) {
	l.rootNodeName = rootNodeName
}

// Called during each state transition. Current, low level events are provided
// at the beginning and end of each state. For instance, START may be followed
// by OFFER_START, OFFER_COMPLETE, DETERMINE_START, DETERMINE_COMPLETE, and so
// on
type LeaderElectionAware interface {
	OnElectionEvent(event ElectionEvent)
}

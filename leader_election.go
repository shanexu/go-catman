package catman

import (
	"errors"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/samuel/go-zookeeper/zk"

	"github.com/shanexu/go-catman/utils"
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
	log          utils.Logger
}

func (cm *CatMan) NewLeaderElectionSupport(hostName, rootNodeName string) *LeaderElectionSupport {
	return &LeaderElectionSupport{
		state:        ElectionStateStop,
		cm:           cm,
		hostName:     hostName,
		rootNodeName: rootNodeName,
		log:          cm.log,
	}
}

// start the election process. This method will create a leader offer,
// determine its status, and either become the leader or become ready.
func (l *LeaderElectionSupport) Start() error {
	l.l.Lock()
	defer l.l.Unlock()

	l.state = ElectionStateStart
	l.dispatchEvent(ElectionEventStart)

	l.log.Info("starting leader election support")

	if l.cm == nil {
		return errors.New("no instance of CatMan provided")
	}

	if l.hostName == "" {
		return errors.New("no hostname provided")
	}

	if err := l.makeOffer(); err != nil {
		l.becomeFailed(err)
		return err
	}

	if err := l.determineElectionStatus(); err != nil {
		l.becomeFailed(err)
		return err
	}

	return nil
}

// stops all election services, revokes any outstanding leader offers, and
// disconnects from ZooKeeper.
func (l *LeaderElectionSupport) Stop() error {
	l.l.Lock()
	defer l.l.Unlock()

	l.state = ElectionStateStop
	l.dispatchEvent(ElectionEventStopStart)

	l.log.Info("stopping leader election support")

	if l.leaderOffer != nil {
		if err := l.cm.Delete(l.leaderOffer.NodePath(), -1); err != nil {
			l.becomeFailed(err)
			return err
		}
		l.log.Infof("removed leader offer %s", l.leaderOffer.NodePath())
	}

	l.dispatchEvent(ElectionEventStopComplete)
	return nil
}

func (l *LeaderElectionSupport) makeOffer() error {
	l.state = ElectionStateOffer
	l.dispatchEvent(ElectionEventOfferStart)

	newLeaderOffer := &LeaderOffer{}
	newLeaderOffer.SetHostName(l.hostName)
	hostnameBytes := []byte(l.hostName)
	nodePath, err := l.cm.Create(
		l.rootNodeName+"/"+"n_",
		hostnameBytes,
		zk.FlagEphemeral|zk.FlagSequence,
		OpenAclUnsafe,
	)
	if err != nil {
		return err
	}
	newLeaderOffer.SetNodePath(nodePath)
	l.leaderOffer = newLeaderOffer

	l.log.Debugf("created leader offer %+v", l.leaderOffer)

	l.dispatchEvent(ElectionEventOfferComplete)

	return nil
}

func (l *LeaderElectionSupport) LeaderOffer() *LeaderOffer {
	return l.leaderOffer
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

	children, err := l.cm.CMChildren(l.rootNodeName, nil)
	if err != nil {
		return err
	}
	leaderOffers, err := l.toLeaderOffers(children)
	if err != nil {
		return err
	}

	for i := range leaderOffers {
		leaderOffer := leaderOffers[i]
		if leaderOffer.Id() == currentLeaderOffer.Id() {
			l.log.Debugf("there are %d leader offers. I am %d in line.", len(leaderOffers), i)

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
	l.log.Infof(
		"%s not elected leader. Watching node:%s",
		l.leaderOffer.NodePath(),
		neighborLeaderOffer.NodePath())

	stat, _ := l.cm.CMExists(neighborLeaderOffer.NodePath(), l)

	if stat != nil {
		l.dispatchEvent(ElectionEventReadyStart)
		l.log.Debugf(
			"we're behind %s in line and they're alive. Keeping an eye on them",
			neighborLeaderOffer.NodePath())
		l.state = ElectionStateReady
	} else {
		l.log.Infof(
			"we're behind %s in line and they're alive. Keeping an eye on them",
			neighborLeaderOffer.NodePath())
		l.determineElectionStatus()
	}
	return nil
}

func (l *LeaderElectionSupport) becomeLeader() error {
	l.state = ElectionStateElected
	l.dispatchEvent(ElectionEventElectedStart)

	l.log.Infof("Becoming leader with node: %s", l.LeaderOffer().NodePath())

	l.dispatchEvent(ElectionEventElectedComplete)
	return nil
}

func (l *LeaderElectionSupport) becomeFailed(err error) error {
	l.log.Infof("failed in state %s - Exception: %s", l.state, err)

	l.state = ElectionStateFailed
	l.dispatchEvent(ElectionEventFailed)

	return nil
}

func (l *LeaderElectionSupport) LeaderHostName() (string, error) {
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
		data, err := l.cm.CMGet(l.rootNodeName + "/" + offer)
		if err != nil {
			return nil, err
		}
		hostName := string(data)
		id, err := strconv.ParseInt(offer[len("n_"):], 10, 64)
		if err != nil {
			return nil, err
		}
		leaderOffers = append(leaderOffers, NewLeaderOffer(int(id), l.rootNodeName+"/"+offer, hostName))
	}
	sort.SliceStable(leaderOffers, func(i int, j int) bool {
		return LeaderOfferComparator(leaderOffers[i], leaderOffers[j]) == -1
	})
	return leaderOffers, nil
}

func (l *LeaderElectionSupport) Process(event zk.Event) {
	if event.Type == zk.EventNodeDeleted {
		if event.Path != l.leaderOffer.NodePath() && l.state != ElectionStateStop {
			l.log.Debugf(
				"node %s deleted. need to run through the election process.",
				event.Path)
			l.l.Lock()
			defer l.l.Unlock()
			if err := l.determineElectionStatus(); err != nil {
				l.becomeFailed(err)
			}
		}
	}
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

func (l *LeaderElectionSupport) RootNodeName() string {
	return l.rootNodeName
}

func (l *LeaderElectionSupport) SetRootNodeName(rootNodeName string) {
	l.rootNodeName = rootNodeName
}

func (l *LeaderElectionSupport) HostName() string {
	return l.hostName
}

func (l *LeaderElectionSupport) SetHostName(hostName string) {
	l.hostName = hostName
}

type ElectionEvent int

const (
	ElectionEventStart ElectionEvent = iota
	ElectionEventOfferStart
	ElectionEventOfferComplete
	ElectionEventDetermineStart
	ElectionEventDetermineComplete
	ElectionEventElectedStart
	ElectionEventElectedComplete
	ElectionEventReadyStart
	ElectionEventReadyComplete
	ElectionEventFailed
	ElectionEventStopStart
	ElectionEventStopComplete
)

type ElectionState int

const (
	ElectionStateStart ElectionState = iota
	ElectionStateOffer
	ElectionStateDetermine
	ElectionStateElected
	ElectionStateReady
	ElectionStateFailed
	ElectionStateStop
)

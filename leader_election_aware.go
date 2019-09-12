package catman

// Called during each state transition. Current, low level events are provided
// at the beginning and end of each state. For instance, START may be followed
// by OFFER_START, OFFER_COMPLETE, DETERMINE_START, DETERMINE_COMPLETE, and so
// on
type LeaderElectionAware interface {
	OnElectionEvent(event ElectionEvent)
}

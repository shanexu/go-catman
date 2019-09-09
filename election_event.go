package catman

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

package catman

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

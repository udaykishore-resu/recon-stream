package recon

// ClassifyInput is everything a BreakClassifier may look at. It is assembled
// by the engine so classifiers stay pure and testable.
type ClassifyInput struct {
	// Legs are the legs being placed into the break (1..n).
	Legs []Leg
	// Related are other legs sharing a txn_ref with any of Legs, regardless of
	// their status (matched, open, broken). Useful for duplicate/FX detection.
	Related []Leg
	// Trigger is why the break is being opened: ref_conflict, duplicate, window_expired.
	Trigger string
	// Rules are the active matcher rules (tolerances, windows).
	Rules Rules
}

// Classification is a classifier verdict with an auditable identifier.
type Classification struct {
	Category     Category
	Confidence   float64
	ClassifierID string // name@version
	Evidence     string // one-line human-readable justification
}

// BreakClassifier is the ML slot. The engine treats it as advisory: it only
// labels and scores breaks, it never decides whether something matches.
type BreakClassifier interface {
	Classify(in ClassifyInput) Classification
}

// Break triggers.
const (
	TriggerRefConflict   = "ref_conflict"
	TriggerDuplicate     = "duplicate"
	TriggerWindowExpired = "window_expired"
)

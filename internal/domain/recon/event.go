package recon

import (
	"encoding/json"
	"fmt"
	"time"
)

// Event types appended to the recon_events log. The log is the source of
// truth; legs/matches/breaks tables are projections that can be rebuilt via
// replay.
const (
	EvLegIngested   = "leg.ingested"
	EvLegDuplicate  = "leg.duplicate" // exact re-delivery; recorded for audit, no state change
	EvLegExpired    = "leg.expired"
	EvMatchCreated  = "match.created"
	EvBreakOpened   = "break.opened"
	EvBreakResolved = "break.resolved"
	EvWindowClosed  = "window.closed"
	EvReplayStarted = "replay.started"
)

// Event is an append-only record of something that happened in the engine.
type Event struct {
	Seq         int64           `json:"seq"`
	Type        string          `json:"type"`
	At          time.Time       `json:"at"`
	AggregateID string          `json:"aggregate_id"`
	Payload     json.RawMessage `json:"payload"`
}

// NewEvent marshals payload into an Event (Seq is assigned by the store).
func NewEvent(typ string, at time.Time, aggregateID string, payload any) (Event, error) {
	b, err := json.Marshal(payload)
	if err != nil {
		return Event{}, fmt.Errorf("marshal %s payload: %w", typ, err)
	}
	return Event{Type: typ, At: at, AggregateID: aggregateID, Payload: b}, nil
}

// ResolutionPayload is the audit payload of a break.resolved event.
type ResolutionPayload struct {
	BreakID string    `json:"break_id"`
	Reason  string    `json:"reason"`
	Actor   string    `json:"actor"`
	At      time.Time `json:"at"`
	MatchID string    `json:"match_id,omitempty"`
}

// ChangeSet is the atomic unit of persistence produced by one engine step.
// Stores must apply it transactionally: either every element lands or none.
type ChangeSet struct {
	Legs    []Leg   // upserts (status/match_id/break_id may change)
	Matches []Match // inserts
	Breaks  []Break // upserts (open or resolved)
	Events  []Event // appends, in order
}

// Empty reports whether the change set carries nothing to persist.
func (c ChangeSet) Empty() bool {
	return len(c.Legs) == 0 && len(c.Matches) == 0 && len(c.Breaks) == 0 && len(c.Events) == 0
}

// unmarshalLeg decodes a leg.ingested payload and strips derived state so the
// leg can be re-run through the engine as if it had just arrived.
func unmarshalLeg(raw json.RawMessage, l *Leg) error {
	if err := json.Unmarshal(raw, l); err != nil {
		return fmt.Errorf("decode leg payload: %w", err)
	}
	if err := l.Validate(); err != nil {
		return err
	}
	l.AssignID()
	l.Status, l.MatchID, l.BreakID = "", "", ""
	return nil
}

func (c *ChangeSet) addEvent(typ string, at time.Time, aggregateID string, payload any) error {
	ev, err := NewEvent(typ, at, aggregateID, payload)
	if err != nil {
		return err
	}
	c.Events = append(c.Events, ev)
	return nil
}

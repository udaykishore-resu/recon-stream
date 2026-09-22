package recon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Repository is the slice of the store the engine needs. ports.Store
// satisfies it; tests may pass a lighter fake.
type Repository interface {
	Apply(ctx context.Context, cs ChangeSet) error
	HasLeg(ctx context.Context, id string) (bool, error)
	LegsByTxnRef(ctx context.Context, txnRef string) ([]Leg, error)
	OpenLegs(ctx context.Context) ([]Leg, error)
	GetBreak(ctx context.Context, id string) (Break, error)
	EventsFrom(ctx context.Context, from int64, limit int) ([]Event, error)
	LastSeq(ctx context.Context) (int64, error)
	ResetProjections(ctx context.Context) error
}

// Hooks receive domain outcomes for metrics/logging. All are optional.
type Hooks struct {
	OnLeg   func(l Leg, outcome Outcome)
	OnMatch func(m Match)
	OnBreak func(b Break)
	OnOpen  func(openLegs int)
}

// Outcome is what happened to one ingested leg.
type Outcome string

// Outcomes an ingested leg can end in.
const (
	OutcomeDuplicate     Outcome = "duplicate"      // exact re-delivery, ignored
	OutcomeMatched       Outcome = "matched"        // joined a Match
	OutcomeBreak         Outcome = "break"          // opened a Break
	OutcomeOpen          Outcome = "open"           // waiting in the window
	OutcomeResolvedBreak Outcome = "resolved_break" // late arrival closed an existing Break
)

// IngestResult reports the outcome of one leg.
type IngestResult struct {
	LegID   string  `json:"leg_id"`
	Outcome Outcome `json:"outcome"`
	MatchID string  `json:"match_id,omitempty"`
	BreakID string  `json:"break_id,omitempty"`
	RuleID  string  `json:"rule_id,omitempty"`
}

// BatchResult summarises one Ingest call.
type BatchResult struct {
	Accepted   int            `json:"accepted"`
	Duplicates int            `json:"duplicates"`
	Matched    int            `json:"matched"`
	Breaks     int            `json:"breaks"`
	Open       int            `json:"open"`
	Results    []IngestResult `json:"results"`
}

// Engine is the single-writer reconciliation core for one process. It owns
// the in-memory open-leg index; every state transition is persisted through
// the Repository as an atomic ChangeSet before the caller sees the result,
// which is what makes at-least-once delivery safe.
type Engine struct {
	mu        sync.Mutex
	rules     Rules
	match     matcher
	ix        *Index
	repo      Repository
	clf       BreakClassifier
	clock     func() time.Time
	hooks     Hooks
	replaying bool
}

// NewEngine wires an engine. ttl is the open-leg window TTL.
func NewEngine(repo Repository, clf BreakClassifier, rules Rules, ttl time.Duration, hooks Hooks) (*Engine, error) {
	if err := rules.Validate(); err != nil {
		return nil, fmt.Errorf("rules: %w", err)
	}
	if ttl <= 0 {
		return nil, errors.New("window ttl must be > 0")
	}
	if repo == nil || clf == nil {
		return nil, errors.New("repository and classifier are required")
	}
	return &Engine{
		rules: rules,
		match: matcher{rules: rules},
		ix:    NewIndex(ttl),
		repo:  repo,
		clf:   clf,
		clock: time.Now,
		hooks: hooks,
	}, nil
}

// SetClock overrides the time source (tests).
func (e *Engine) SetClock(c func() time.Time) { e.clock = c }

// Rules returns the active rules.
func (e *Engine) Rules() Rules { return e.rules }

// OpenLegs returns the size of the open-leg index.
func (e *Engine) OpenLegs() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.ix.Len()
}

// Rebuild loads open legs from the repository into the index (process start).
func (e *Engine) Rebuild(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	legs, err := e.repo.OpenLegs(ctx)
	if err != nil {
		return fmt.Errorf("load open legs: %w", err)
	}
	e.ix = NewIndex(e.ix.ttl)
	for _, l := range legs {
		e.ix.Add(l)
	}
	e.notifyOpen()
	return nil
}

// Ingest validates the whole batch first (all-or-nothing on validation), then
// processes legs one by one, persisting each transition atomically.
func (e *Engine) Ingest(ctx context.Context, legs []Leg) (BatchResult, error) {
	for i := range legs {
		if err := legs[i].Validate(); err != nil {
			return BatchResult{}, fmt.Errorf("leg[%d] %s/%s: %w", i, legs[i].Source, legs[i].TxnRef, err)
		}
		legs[i].AssignID()
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	res := BatchResult{Results: make([]IngestResult, 0, len(legs))}
	for _, l := range legs {
		if err := ctx.Err(); err != nil {
			return res, err
		}
		r, err := e.ingestOne(ctx, l)
		if err != nil {
			return res, fmt.Errorf("ingest %s: %w", l.ID, err)
		}
		res.Results = append(res.Results, r)
		res.Accepted++
		switch r.Outcome {
		case OutcomeDuplicate:
			res.Duplicates++
		case OutcomeMatched, OutcomeResolvedBreak:
			res.Matched++
		case OutcomeBreak:
			res.Breaks++
		case OutcomeOpen:
			res.Open++
		}
	}
	e.notifyOpen()
	return res, nil
}

func (e *Engine) ingestOne(ctx context.Context, l Leg) (IngestResult, error) {
	now := e.clock().UTC()
	if l.IngestedAt.IsZero() {
		l.IngestedAt = now
	}
	seen, err := e.repo.HasLeg(ctx, l.ID)
	if err != nil {
		return IngestResult{}, fmt.Errorf("dedupe lookup: %w", err)
	}
	if seen {
		if !e.replaying {
			var cs ChangeSet
			if err := cs.addEvent(EvLegDuplicate, now, l.ID, l); err != nil {
				return IngestResult{}, err
			}
			if err := e.repo.Apply(ctx, cs); err != nil {
				return IngestResult{}, err
			}
		}
		e.fire(l, OutcomeDuplicate)
		return IngestResult{LegID: l.ID, Outcome: OutcomeDuplicate}, nil
	}

	related, err := e.repo.LegsByTxnRef(ctx, l.TxnRef)
	if err != nil {
		return IngestResult{}, fmt.Errorf("related lookup: %w", err)
	}
	var cs ChangeSet
	if !e.replaying {
		if err := cs.addEvent(EvLegIngested, now, l.ID, l); err != nil {
			return IngestResult{}, err
		}
	}

	// 1. Same source re-using a txn_ref with different content: duplicate break.
	for _, r := range related {
		if r.Source == l.Source && r.ID != l.ID {
			b, err := e.openBreak(&cs, now, TriggerDuplicate, related, l)
			if err != nil {
				return IngestResult{}, err
			}
			return e.commit(ctx, cs, l, IngestResult{LegID: l.ID, Outcome: OutcomeBreak, BreakID: b.ID})
		}
	}

	exists := func(txnRef, source string) bool {
		ls, err := e.repo.LegsByTxnRef(ctx, txnRef)
		if err != nil {
			return true // fail closed: treat as existing so the fallback rule stays conservative
		}
		for _, x := range ls {
			if x.Source == source {
				return true
			}
		}
		return false
	}

	// 2. Deterministic tiers against the open index.
	if m, ok := e.match.find(l, e.ix, exists); ok {
		m.MatchedAt = now
		l.Status, l.MatchID = LegMatched, m.ID
		cs.Legs = append(cs.Legs, l)
		for _, id := range m.LegIDs {
			if id == l.ID {
				continue
			}
			p, _ := e.ix.Get(id)
			p.Status, p.MatchID = LegMatched, m.ID
			cs.Legs = append(cs.Legs, p)
		}
		cs.Matches = append(cs.Matches, m)
		if err := cs.addEvent(EvMatchCreated, now, m.ID, m); err != nil {
			return IngestResult{}, err
		}
		if err := e.repo.Apply(ctx, cs); err != nil {
			return IngestResult{}, err
		}
		e.ix.Remove(m.LegIDs...)
		e.fire(l, OutcomeMatched)
		if e.hooks.OnMatch != nil {
			e.hooks.OnMatch(m)
		}
		return IngestResult{LegID: l.ID, Outcome: OutcomeMatched, MatchID: m.ID, RuleID: m.RuleID}, nil
	}

	// 3. Same ref, opposite source, open but unmatched: definite break with both legs.
	if partners := e.ix.ByRef(l); len(partners) > 0 {
		legs := append([]Leg{l}, partners...)
		b, err := e.openBreak(&cs, now, TriggerRefConflict, related, legs...)
		if err != nil {
			return IngestResult{}, err
		}
		if err := e.repo.Apply(ctx, cs); err != nil {
			return IngestResult{}, err
		}
		for _, p := range partners {
			e.ix.Remove(p.ID)
		}
		e.fire(l, OutcomeBreak)
		if e.hooks.OnBreak != nil {
			e.hooks.OnBreak(b)
		}
		return IngestResult{LegID: l.ID, Outcome: OutcomeBreak, BreakID: b.ID}, nil
	}

	// 4. Partner already expired into a single-leg break: late arrival heals it.
	for _, r := range related {
		if r.Source == l.Source || r.Status != LegBroken || r.BreakID == "" {
			continue
		}
		if !e.rules.WithinTolerance(l.AmountMinor, r.AmountMinor) || r.Currency != l.Currency {
			continue
		}
		b, err := e.repo.GetBreak(ctx, r.BreakID)
		if err != nil || b.Status != BreakOpen || len(b.LegIDs) != 1 {
			continue
		}
		m := e.match.pair(l, r, TierTolerant, RuleT2Late, tolerantConfidence(r.AmountMinor-l.AmountMinor, e.rules.Tolerance(l.AmountMinor))-0.05)
		m.MatchedAt = now
		l.Status, l.MatchID = LegMatched, m.ID
		r.Status, r.MatchID, r.BreakID = LegMatched, m.ID, ""
		resolvedAt := now
		b.Status, b.ResolvedAt = BreakResolved, &resolvedAt
		b.Reason, b.Actor = "late arrival matched by "+RuleT2Late, "system"
		cs.Legs = append(cs.Legs, l, r)
		cs.Matches = append(cs.Matches, m)
		cs.Breaks = append(cs.Breaks, b)
		if err := cs.addEvent(EvMatchCreated, now, m.ID, m); err != nil {
			return IngestResult{}, err
		}
		if err := cs.addEvent(EvBreakResolved, now, b.ID, ResolutionPayload{BreakID: b.ID, Reason: b.Reason, Actor: b.Actor, At: now, MatchID: m.ID}); err != nil {
			return IngestResult{}, err
		}
		if err := e.repo.Apply(ctx, cs); err != nil {
			return IngestResult{}, err
		}
		e.fire(l, OutcomeResolvedBreak)
		if e.hooks.OnMatch != nil {
			e.hooks.OnMatch(m)
		}
		return IngestResult{LegID: l.ID, Outcome: OutcomeResolvedBreak, MatchID: m.ID, BreakID: b.ID, RuleID: m.RuleID}, nil
	}

	// 5. The counterparty already reported this txn_ref (and it is matched or
	// broken elsewhere), yet this leg cannot join it: a definite break.
	for _, r := range related {
		if r.Source != l.Source {
			b, err := e.openBreak(&cs, now, TriggerRefConflict, related, l)
			if err != nil {
				return IngestResult{}, err
			}
			return e.commit(ctx, cs, l, IngestResult{LegID: l.ID, Outcome: OutcomeBreak, BreakID: b.ID})
		}
	}

	// 6. Nothing to match yet: wait in the window.
	l.Status = LegOpen
	cs.Legs = append(cs.Legs, l)
	if err := e.repo.Apply(ctx, cs); err != nil {
		return IngestResult{}, err
	}
	e.ix.Add(l)
	e.fire(l, OutcomeOpen)
	return IngestResult{LegID: l.ID, Outcome: OutcomeOpen}, nil
}

func (e *Engine) commit(ctx context.Context, cs ChangeSet, l Leg, r IngestResult) (IngestResult, error) {
	if err := e.repo.Apply(ctx, cs); err != nil {
		return IngestResult{}, err
	}
	e.fire(l, r.Outcome)
	if e.hooks.OnBreak != nil && len(cs.Breaks) > 0 {
		e.hooks.OnBreak(cs.Breaks[len(cs.Breaks)-1])
	}
	return r, nil
}

// openBreak classifies and records a break for legs, appending to cs.
func (e *Engine) openBreak(cs *ChangeSet, now time.Time, trigger string, related []Leg, legs ...Leg) (Break, error) {
	c := e.clf.Classify(ClassifyInput{Legs: legs, Related: related, Trigger: trigger, Rules: e.rules})
	ids := make([]string, 0, len(legs))
	for _, l := range legs {
		ids = append(ids, l.ID)
	}
	b := Break{
		ID:           DeriveID("brk", ids),
		LegIDs:       ids,
		Currency:     legs[0].Currency,
		Counterparty: legs[0].Counterparty,
		Category:     c.Category,
		Confidence:   c.Confidence,
		ClassifierID: c.ClassifierID,
		Trigger:      trigger,
		Status:       BreakOpen,
		OpenedAt:     now,
		Reason:       c.Evidence,
	}
	for i := range legs {
		legs[i].Status, legs[i].BreakID = LegBroken, b.ID
		cs.Legs = append(cs.Legs, legs[i])
	}
	cs.Breaks = append(cs.Breaks, b)
	if err := cs.addEvent(EvBreakOpened, now, b.ID, b); err != nil {
		return Break{}, err
	}
	return b, nil
}

// Sweep expires open legs older than the window TTL into breaks.
func (e *Engine) Sweep(ctx context.Context) (int, error) {
	return e.expire(ctx, -1, false)
}

// CloseWindow forces every open leg older than olderThan (0 = all) into a
// break. It models an EOD/cut-off and is exposed as POST /v1/windows/close.
func (e *Engine) CloseWindow(ctx context.Context, olderThan time.Duration) (int, error) {
	if olderThan < 0 {
		olderThan = 0
	}
	return e.expire(ctx, olderThan, true)
}

func (e *Engine) expire(ctx context.Context, olderThan time.Duration, forced bool) (int, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	now := e.clock().UTC()
	expired := e.ix.Expired(now, olderThan)
	if len(expired) == 0 {
		return 0, nil
	}
	count := 0
	for _, l := range expired {
		if err := ctx.Err(); err != nil {
			return count, err
		}
		if _, still := e.ix.Get(l.ID); !still {
			continue
		}
		related, err := e.repo.LegsByTxnRef(ctx, l.TxnRef)
		if err != nil {
			return count, fmt.Errorf("related lookup: %w", err)
		}
		var cs ChangeSet
		if err := cs.addEvent(EvLegExpired, now, l.ID, map[string]any{"leg_id": l.ID, "forced": forced}); err != nil {
			return count, err
		}
		b, err := e.openBreak(&cs, now, TriggerWindowExpired, related, l)
		if err != nil {
			return count, err
		}
		if err := e.repo.Apply(ctx, cs); err != nil {
			return count, fmt.Errorf("persist expiry of %s: %w", l.ID, err)
		}
		e.ix.Remove(l.ID)
		if e.hooks.OnBreak != nil {
			e.hooks.OnBreak(b)
		}
		count++
	}
	if forced {
		var cs ChangeSet
		if err := cs.addEvent(EvWindowClosed, now, "window", map[string]any{"expired": count, "older_than": olderThan.String()}); err != nil {
			return count, err
		}
		if err := e.repo.Apply(ctx, cs); err != nil {
			return count, err
		}
	}
	e.notifyOpen()
	return count, nil
}

// ErrBreakResolved is returned when resolving an already-resolved break.
var ErrBreakResolved = errors.New("break already resolved")

// ResolveBreak closes a break with an audited reason and actor.
func (e *Engine) ResolveBreak(ctx context.Context, id, reason, actor string) (Break, error) {
	if reason == "" || actor == "" {
		return Break{}, errors.New("reason and actor are required")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	b, err := e.repo.GetBreak(ctx, id)
	if err != nil {
		return Break{}, err
	}
	if b.Status == BreakResolved {
		return b, ErrBreakResolved
	}
	now := e.clock().UTC()
	b.Status, b.ResolvedAt, b.Reason, b.Actor = BreakResolved, &now, reason, actor
	var cs ChangeSet
	cs.Breaks = append(cs.Breaks, b)
	if err := cs.addEvent(EvBreakResolved, now, b.ID, ResolutionPayload{BreakID: b.ID, Reason: reason, Actor: actor, At: now}); err != nil {
		return Break{}, err
	}
	if err := e.repo.Apply(ctx, cs); err != nil {
		return Break{}, err
	}
	return b, nil
}

// ReplayResult summarises a replay.
type ReplayResult struct {
	FromSeq      int64 `json:"from_seq"`
	ThroughSeq   int64 `json:"through_seq"`
	EventsRead   int   `json:"events_read"`
	LegsReplayed int   `json:"legs_replayed"`
	Matched      int   `json:"matched"`
	Breaks       int   `json:"breaks"`
	Open         int   `json:"open"`
}

// Replay rebuilds all projections from the event log: it clears legs/matches/
// breaks, then re-feeds every leg.ingested event with seq >= from through the
// current rules. Events appended during replay carry the replay marker; the
// original leg.ingested events are never duplicated.
func (e *Engine) Replay(ctx context.Context, from int64) (ReplayResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if from < 1 {
		from = 1
	}
	through, err := e.repo.LastSeq(ctx)
	if err != nil {
		return ReplayResult{}, fmt.Errorf("last seq: %w", err)
	}
	now := e.clock().UTC()
	var start ChangeSet
	if err := start.addEvent(EvReplayStarted, now, "replay", map[string]any{"from": from, "through": through}); err != nil {
		return ReplayResult{}, err
	}
	if err := e.repo.Apply(ctx, start); err != nil {
		return ReplayResult{}, err
	}
	if err := e.repo.ResetProjections(ctx); err != nil {
		return ReplayResult{}, fmt.Errorf("reset projections: %w", err)
	}
	e.ix = NewIndex(e.ix.ttl)
	e.replaying = true
	defer func() { e.replaying = false }()

	res := ReplayResult{FromSeq: from, ThroughSeq: through}
	cursor := from
	const page = 500
	for cursor <= through {
		evs, err := e.repo.EventsFrom(ctx, cursor, page)
		if err != nil {
			return res, fmt.Errorf("read events: %w", err)
		}
		if len(evs) == 0 {
			break
		}
		for _, ev := range evs {
			if ev.Seq > through {
				break
			}
			cursor = ev.Seq + 1
			res.EventsRead++
			if ev.Type == EvBreakResolved {
				if err := e.reapplyResolution(ctx, ev); err != nil {
					return res, fmt.Errorf("event %d: %w", ev.Seq, err)
				}
				continue
			}
			if ev.Type != EvLegIngested {
				continue
			}
			var l Leg
			if err := unmarshalLeg(ev.Payload, &l); err != nil {
				return res, fmt.Errorf("event %d: %w", ev.Seq, err)
			}
			r, err := e.ingestOne(ctx, l)
			if err != nil {
				return res, fmt.Errorf("event %d: %w", ev.Seq, err)
			}
			res.LegsReplayed++
			switch r.Outcome {
			case OutcomeMatched, OutcomeResolvedBreak:
				res.Matched++
			case OutcomeBreak:
				res.Breaks++
			case OutcomeOpen:
				res.Open++
			}
		}
		if len(evs) < page {
			break
		}
	}
	e.notifyOpen()
	return res, nil
}

// reapplyResolution replays a manual break.resolved event: break IDs are
// derived from leg IDs, so the re-derived break carries the same ID and the
// operator's decision is restored without appending a second audit event.
// System resolutions (late arrivals) are re-derived from the legs themselves.
func (e *Engine) reapplyResolution(ctx context.Context, ev Event) error {
	var p ResolutionPayload
	if err := json.Unmarshal(ev.Payload, &p); err != nil {
		return fmt.Errorf("decode resolution: %w", err)
	}
	if p.MatchID != "" || p.BreakID == "" {
		return nil
	}
	b, err := e.repo.GetBreak(ctx, p.BreakID)
	if err != nil || b.Status != BreakOpen {
		return nil // break not re-derived under current rules, or already closed
	}
	at := p.At
	b.Status, b.ResolvedAt, b.Reason, b.Actor = BreakResolved, &at, p.Reason, p.Actor
	return e.repo.Apply(ctx, ChangeSet{Breaks: []Break{b}})
}

func (e *Engine) fire(l Leg, o Outcome) {
	if e.hooks.OnLeg != nil {
		e.hooks.OnLeg(l, o)
	}
}

func (e *Engine) notifyOpen() {
	if e.hooks.OnOpen != nil {
		e.hooks.OnOpen(e.ix.Len())
	}
}

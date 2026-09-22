// Package memory provides in-memory adapters so the service runs with zero
// infrastructure (make run) and tests need no Docker.
package memory

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/udaykishore-resu/recon-stream/internal/domain/recon"
	"github.com/udaykishore-resu/recon-stream/internal/ports"
)

// Store is a goroutine-safe in-memory ports.Store.
type Store struct {
	mu      sync.RWMutex
	legs    map[string]recon.Leg
	byRef   map[string][]string
	matches map[string]recon.Match
	breaks  map[string]recon.Break
	events  []recon.Event
	now     func() time.Time
}

var _ ports.Store = (*Store)(nil)

// NewStore creates an empty store.
func NewStore() *Store {
	return &Store{
		legs:    make(map[string]recon.Leg),
		byRef:   make(map[string][]string),
		matches: make(map[string]recon.Match),
		breaks:  make(map[string]recon.Break),
		now:     time.Now,
	}
}

// Apply implements ports.Store. The whole change set lands under one lock.
func (s *Store) Apply(_ context.Context, cs recon.ChangeSet) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, l := range cs.Legs {
		if _, existed := s.legs[l.ID]; !existed {
			s.byRef[l.TxnRef] = append(s.byRef[l.TxnRef], l.ID)
		}
		s.legs[l.ID] = l
	}
	for _, m := range cs.Matches {
		s.matches[m.ID] = m
	}
	for _, b := range cs.Breaks {
		s.breaks[b.ID] = b
	}
	for _, ev := range cs.Events {
		ev.Seq = int64(len(s.events)) + 1
		s.events = append(s.events, ev)
	}
	return nil
}

// HasLeg implements ports.Store.
func (s *Store) HasLeg(_ context.Context, id string) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.legs[id]
	return ok, nil
}

// GetLeg implements ports.Store.
func (s *Store) GetLeg(_ context.Context, id string) (recon.Leg, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	l, ok := s.legs[id]
	if !ok {
		return recon.Leg{}, ports.ErrNotFound
	}
	return l, nil
}

// LegsByTxnRef implements ports.Store.
func (s *Store) LegsByTxnRef(_ context.Context, txnRef string) ([]recon.Leg, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ids := s.byRef[txnRef]
	out := make([]recon.Leg, 0, len(ids))
	for _, id := range ids {
		out = append(out, s.legs[id])
	}
	return out, nil
}

// OpenLegs implements ports.Store.
func (s *Store) OpenLegs(_ context.Context) ([]recon.Leg, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []recon.Leg
	for _, l := range s.legs {
		if l.Status == recon.LegOpen {
			out = append(out, l)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

// GetMatch implements ports.Store.
func (s *Store) GetMatch(_ context.Context, id string) (recon.Match, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m, ok := s.matches[id]
	if !ok {
		return recon.Match{}, ports.ErrNotFound
	}
	return m, nil
}

// GetBreak implements ports.Store.
func (s *Store) GetBreak(_ context.Context, id string) (recon.Break, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, ok := s.breaks[id]
	if !ok {
		return recon.Break{}, ports.ErrNotFound
	}
	return b, nil
}

// ListBreaks implements ports.Store: filtered, ordered oldest-first, paged.
func (s *Store) ListBreaks(_ context.Context, f ports.BreakFilter) ([]recon.Break, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := s.now().UTC()
	out := make([]recon.Break, 0, len(s.breaks))
	for _, b := range s.breaks {
		if f.Status != "" && b.Status != f.Status {
			continue
		}
		if f.Category != "" && b.Category != f.Category {
			continue
		}
		if f.Currency != "" && b.Currency != f.Currency {
			continue
		}
		if f.Counterparty != "" && b.Counterparty != f.Counterparty {
			continue
		}
		if f.MinAge > 0 && b.Age(now) < f.MinAge {
			continue
		}
		out = append(out, b)
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].OpenedAt.Equal(out[j].OpenedAt) {
			return out[i].OpenedAt.Before(out[j].OpenedAt)
		}
		return out[i].ID < out[j].ID
	})
	if f.Offset > 0 {
		if f.Offset >= len(out) {
			return []recon.Break{}, nil
		}
		out = out[f.Offset:]
	}
	if f.Limit > 0 && len(out) > f.Limit {
		out = out[:f.Limit]
	}
	return out, nil
}

// EventsFrom implements ports.Store.
func (s *Store) EventsFrom(_ context.Context, from int64, limit int) ([]recon.Event, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if from < 1 {
		from = 1
	}
	if from > int64(len(s.events)) {
		return []recon.Event{}, nil
	}
	out := s.events[from-1:]
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return append([]recon.Event(nil), out...), nil
}

// LastSeq implements recon.Repository.
func (s *Store) LastSeq(_ context.Context) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return int64(len(s.events)), nil
}

// ResetProjections implements ports.Store.
func (s *Store) ResetProjections(_ context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.legs = make(map[string]recon.Leg)
	s.byRef = make(map[string][]string)
	s.matches = make(map[string]recon.Match)
	s.breaks = make(map[string]recon.Break)
	return nil
}

// Stats implements ports.Store.
func (s *Store) Stats(_ context.Context) (ports.Stats, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	st := ports.Stats{
		MatchesByTier: map[string]int64{},
		MatchesByRule: map[string]int64{},
		BreaksByCat:   map[string]int64{},
		LegsBySource:  map[string]int64{},
	}
	for _, l := range s.legs {
		st.LegsTotal++
		st.LegsBySource[l.Source]++
		switch l.Status {
		case recon.LegOpen:
			st.LegsOpen++
		case recon.LegMatched:
			st.LegsMatched++
		case recon.LegBroken:
			st.LegsInBreak++
		}
	}
	for _, m := range s.matches {
		st.MatchesTotal++
		st.MatchesByTier[tierName(m.Tier)]++
		st.MatchesByRule[m.RuleID]++
	}
	for _, b := range s.breaks {
		if b.Status == recon.BreakOpen {
			st.BreaksOpen++
			st.BreaksByCat[string(b.Category)]++
			if st.OldestOpenBreak == nil || b.OpenedAt.Before(*st.OldestOpenBreak) {
				t := b.OpenedAt
				st.OldestOpenBreak = &t
			}
		} else {
			st.BreaksResolved++
		}
	}
	st.EventsTotal = int64(len(s.events))
	if den := st.LegsMatched + st.LegsInBreak; den > 0 {
		st.AutoMatchRate = float64(st.LegsMatched) / float64(den)
	}
	return st, nil
}

// Ping implements ports.Store.
func (s *Store) Ping(context.Context) error { return nil }

// Close implements ports.Store.
func (s *Store) Close() error { return nil }

func tierName(t recon.Tier) string {
	switch t {
	case recon.TierExact:
		return "T1"
	case recon.TierTolerant:
		return "T2"
	case recon.TierManyToOne:
		return "T3"
	}
	return "unknown"
}

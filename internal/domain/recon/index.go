package recon

import (
	"sort"
	"time"
)

// Index is the in-memory open-leg index. Legs are bucketed by WindowKey and
// source, with secondary lookups by txn_ref and batch_ref, so the common
// tiers (T1/T2 by ref, hinted T3 by batch) never scan a window. It is not
// goroutine-safe; the Engine serialises access. Entries expire after TTL
// measured from IngestedAt.
type Index struct {
	ttl     time.Duration
	byKey   map[WindowKey]map[string]map[string]*Leg // window -> source -> id
	byRef   map[string]map[string]*Leg               // txn_ref -> id
	byBatch map[string]map[string]*Leg               // attrs[batch_ref] -> id
	byID    map[string]*Leg
}

// NewIndex creates an empty index with the given TTL.
func NewIndex(ttl time.Duration) *Index {
	return &Index{
		ttl:     ttl,
		byKey:   make(map[WindowKey]map[string]map[string]*Leg),
		byRef:   make(map[string]map[string]*Leg),
		byBatch: make(map[string]map[string]*Leg),
		byID:    make(map[string]*Leg),
	}
}

// Len returns the number of open legs.
func (ix *Index) Len() int { return len(ix.byID) }

// Add inserts a leg. Adding an existing ID is a no-op.
func (ix *Index) Add(l Leg) {
	if _, ok := ix.byID[l.ID]; ok {
		return
	}
	lp := &l
	k := l.Key()
	if ix.byKey[k] == nil {
		ix.byKey[k] = make(map[string]map[string]*Leg)
	}
	if ix.byKey[k][l.Source] == nil {
		ix.byKey[k][l.Source] = make(map[string]*Leg)
	}
	ix.byKey[k][l.Source][l.ID] = lp
	put(ix.byRef, l.TxnRef, lp)
	if b := l.Attrs[AttrBatchRef]; b != "" {
		put(ix.byBatch, b, lp)
	}
	ix.byID[l.ID] = lp
}

// Remove deletes legs by ID; unknown IDs are ignored.
func (ix *Index) Remove(ids ...string) {
	for _, id := range ids {
		lp, ok := ix.byID[id]
		if !ok {
			continue
		}
		k := lp.Key()
		delete(ix.byKey[k][lp.Source], id)
		if len(ix.byKey[k][lp.Source]) == 0 {
			delete(ix.byKey[k], lp.Source)
		}
		if len(ix.byKey[k]) == 0 {
			delete(ix.byKey, k)
		}
		del(ix.byRef, lp.TxnRef, id)
		if b := lp.Attrs[AttrBatchRef]; b != "" {
			del(ix.byBatch, b, id)
		}
		delete(ix.byID, id)
	}
}

// Get returns an open leg by ID.
func (ix *Index) Get(id string) (Leg, bool) {
	lp, ok := ix.byID[id]
	if !ok {
		return Leg{}, false
	}
	return *lp, true
}

// Candidates returns open legs in the same window from any other source, in
// deterministic order (value date, txn_ref, id). This scans the window and is
// used only by the ref-less rules.
func (ix *Index) Candidates(l Leg) []Leg {
	var out []Leg
	for src, m := range ix.byKey[l.Key()] {
		if src == l.Source {
			continue
		}
		for _, c := range m {
			out = append(out, *c)
		}
	}
	sortLegs(out)
	return out
}

// CandidatesByRef returns open legs in the same window, from another source,
// with the same txn_ref. O(k) in the number of legs sharing the ref.
func (ix *Index) CandidatesByRef(l Leg) []Leg {
	k := l.Key()
	return ix.collect(ix.byRef[l.TxnRef], func(c *Leg) bool { return c.Source != l.Source && c.Key() == k })
}

// SameSourceInWindow returns open legs in the same window from the same source,
// excluding the leg itself.
func (ix *Index) SameSourceInWindow(l Leg) []Leg {
	return ix.collect(ix.byKey[l.Key()][l.Source], func(c *Leg) bool { return c.ID != l.ID })
}

// ByRef returns open legs with the same txn_ref (any window), excluding the leg itself.
func (ix *Index) ByRef(l Leg) []Leg {
	return ix.collect(ix.byRef[l.TxnRef], func(c *Leg) bool { return c.ID != l.ID })
}

// RefLegs returns every open leg with the given txn_ref, any window.
func (ix *Index) RefLegs(txnRef string) []Leg {
	return ix.collect(ix.byRef[txnRef], func(*Leg) bool { return true })
}

// ByBatch returns open legs whose batch_ref attribute equals batch, in the
// same window as l, excluding l itself.
func (ix *Index) ByBatch(l Leg, batch string) []Leg {
	k := l.Key()
	return ix.collect(ix.byBatch[batch], func(c *Leg) bool { return c.ID != l.ID && c.Key() == k })
}

// Expired returns legs whose TTL has elapsed at now (or every leg if
// olderThan >= 0 overrides TTL), in deterministic order.
func (ix *Index) Expired(now time.Time, olderThan time.Duration) []Leg {
	cutoff := now.Add(-ix.ttl)
	if olderThan >= 0 {
		cutoff = now.Add(-olderThan)
	}
	return ix.collect(ix.byID, func(c *Leg) bool { return !c.IngestedAt.After(cutoff) })
}

// All returns every open leg in deterministic order.
func (ix *Index) All() []Leg {
	return ix.collect(ix.byID, func(*Leg) bool { return true })
}

func (ix *Index) collect(m map[string]*Leg, keep func(*Leg) bool) []Leg {
	if len(m) == 0 {
		return nil
	}
	out := make([]Leg, 0, len(m))
	for _, lp := range m {
		if keep(lp) {
			out = append(out, *lp)
		}
	}
	sortLegs(out)
	return out
}

func put(m map[string]map[string]*Leg, key string, lp *Leg) {
	if m[key] == nil {
		m[key] = make(map[string]*Leg)
	}
	m[key][lp.ID] = lp
}

func del(m map[string]map[string]*Leg, key, id string) {
	delete(m[key], id)
	if len(m[key]) == 0 {
		delete(m, key)
	}
}

func sortLegs(ls []Leg) {
	sort.Slice(ls, func(i, j int) bool {
		if !ls[i].ValueDate.Equal(ls[j].ValueDate.Time) {
			return ls[i].ValueDate.Before(ls[j].ValueDate.Time)
		}
		if ls[i].TxnRef != ls[j].TxnRef {
			return ls[i].TxnRef < ls[j].TxnRef
		}
		return ls[i].ID < ls[j].ID
	})
}

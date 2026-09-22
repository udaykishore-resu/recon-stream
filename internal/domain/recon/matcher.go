package recon

import (
	"sort"
)

// matcher runs the ordered, deterministic tiers against the open-leg index.
// It never mutates the index; the engine applies the outcome.
type matcher struct {
	rules Rules
}

// refExists reports whether a persisted leg from source `src` exists with the
// given txn_ref (used to keep the ref-less T2 fallback conservative).
type refExists func(txnRef, source string) bool

// find returns the best match for l, trying T1, T2, T3 in order. The first
// tier that yields a candidate wins; tiers never compete. Ref-based tiers use
// O(1) lookups; only the ref-less rules scan the window.
func (m matcher) find(l Leg, ix *Index, exists refExists) (Match, bool) {
	byRef := m.inWindow(l, ix.CandidatesByRef(l))
	if mt, ok := m.tier1(l, byRef); ok {
		return mt, true
	}
	if mt, ok := m.tier2Ref(l, byRef); ok {
		return mt, true
	}
	var scanned []Leg
	scan := func() []Leg {
		if scanned == nil {
			scanned = m.inWindow(l, ix.Candidates(l))
		}
		return scanned
	}
	if mt, ok := m.tier2AmountDate(l, scan, ix, exists); ok {
		return mt, true
	}
	if mt, ok := m.tier3(l, scan, ix); ok {
		return mt, true
	}
	return Match{}, false
}

// inWindow keeps candidates whose value date is within the window of l.
func (m matcher) inWindow(l Leg, cands []Leg) []Leg {
	out := cands[:0:0]
	for _, c := range cands {
		if m.rules.WithinDateWindow(c.ValueDate, l.ValueDate) {
			out = append(out, c)
		}
	}
	return out
}

// tier1: same txn_ref, identical amount (currency is implied by the window).
func (m matcher) tier1(l Leg, cands []Leg) (Match, bool) {
	for _, c := range cands {
		if c.TxnRef == l.TxnRef && c.AmountMinor == l.AmountMinor {
			return m.pair(l, c, TierExact, RuleT1Exact, 1.0), true
		}
	}
	return Match{}, false
}

// tier2Ref: same txn_ref, amount within tolerance; picks the smallest residual.
func (m matcher) tier2Ref(l Leg, cands []Leg) (Match, bool) {
	best, found := Leg{}, false
	var bestDiff int64
	for _, c := range cands {
		if c.TxnRef != l.TxnRef || !m.rules.WithinTolerance(l.AmountMinor, c.AmountMinor) {
			continue
		}
		d := abs64(c.AmountMinor - l.AmountMinor)
		if !found || d < bestDiff {
			best, bestDiff, found = c, d, true
		}
	}
	if !found {
		return Match{}, false
	}
	conf := tolerantConfidence(bestDiff, m.rules.Tolerance(l.AmountMinor))
	return m.pair(l, best, TierTolerant, RuleT2Tolerant, conf), true
}

// tier2AmountDate: no shared ref, exact amount, in window, and unique on BOTH
// sides (exactly one candidate; l is the only open same-source leg with that
// amount). Additionally the candidate's ref must not already be known on l's
// source, otherwise its true partner is merely late.
func (m matcher) tier2AmountDate(l Leg, scan func() []Leg, ix *Index, exists refExists) (Match, bool) {
	var hit Leg
	n := 0
	for _, c := range scan() {
		if c.AmountMinor == l.AmountMinor {
			hit = c
			n++
		}
	}
	if n != 1 {
		return Match{}, false
	}
	for _, s := range ix.SameSourceInWindow(l) {
		if s.AmountMinor == l.AmountMinor && m.rules.WithinDateWindow(s.ValueDate, hit.ValueDate) {
			return Match{}, false // ambiguous on our side
		}
	}
	if exists != nil && (exists(hit.TxnRef, l.Source) || exists(l.TxnRef, hit.Source)) {
		return Match{}, false
	}
	return m.pair(l, hit, TierTolerant, RuleT2AmountDate, 0.75), true
}

// tier3: bounded many-to-one subset sum, both orientations:
//
//	(a) l is the single settlement leg; N opposite legs (same source) sum to it.
//	(b) l is one of the N; some opposite single leg S is summed to by l + others.
//
// With T3RequireBatchHint the pools come from the batch_ref index, so only legs
// sharing a batch key are considered and no window scan happens. Without it
// every in-window opposite leg is a candidate (capped at T3MaxCandidates).
func (m matcher) tier3(l Leg, scan func() []Leg, ix *Index) (Match, bool) {
	hint := m.rules.T3RequireBatchHint

	// (a) l is the "one" side.
	var pool []Leg
	if hint {
		pool = m.inWindow(l, filterLegs(ix.ByBatch(l, settlementKey(l)), func(c Leg) bool { return c.Source != l.Source }))
	} else {
		pool = scan()
	}
	bySource := groupBySource(pool)
	for _, src := range sortedKeys(bySource) {
		p := capCandidates(bySource[src], m.rules.T3MaxCandidates)
		tol := m.rules.Tolerance(l.AmountMinor)
		if subset, ok := subsetSum(p, l.AmountMinor, tol, 2, m.rules.T3MaxSubset, nil); ok {
			return m.many(l, subset, TierManyToOne, RuleT3Subset, tol), true
		}
	}

	// (b) l is part of the "many" side completing an opposite settlement leg.
	myBatch := l.Attrs[AttrBatchRef]
	if hint && myBatch == "" {
		return Match{}, false
	}
	var settlements, same []Leg
	if hint {
		// Settlement legs carry the batch as their txn_ref or as batch_ref.
		settlements = filterLegs(ix.RefLegs(myBatch), func(s Leg) bool { return s.Source != l.Source && s.Key() == l.Key() })
		settlements = append(settlements, filterLegs(ix.ByBatch(l, myBatch), func(s Leg) bool { return s.Source != l.Source })...)
		sortLegs(settlements)
		same = filterLegs(ix.ByBatch(l, myBatch), func(o Leg) bool { return o.Source == l.Source })
	} else {
		settlements = scan()
		same = ix.SameSourceInWindow(l)
	}
	for _, s := range settlements {
		if s.AmountMinor <= l.AmountMinor || !m.rules.WithinDateWindow(s.ValueDate, l.ValueDate) {
			continue // l alone cannot be a strict part of s
		}
		p := filterLegs(same, func(o Leg) bool {
			return m.rules.WithinDateWindow(o.ValueDate, s.ValueDate) && o.AmountMinor < s.AmountMinor
		})
		p = capCandidates(p, m.rules.T3MaxCandidates)
		tol := m.rules.Tolerance(s.AmountMinor)
		rest, ok := subsetSum(p, s.AmountMinor-l.AmountMinor, tol, 1, m.rules.T3MaxSubset-1, nil)
		if !ok {
			continue
		}
		return m.many(s, append([]Leg{l}, rest...), TierManyToOne, RuleT3Subset, tol), true
	}
	return Match{}, false
}

// settlementKey is the batch key of a leg acting as the "one" side: an explicit
// batch_ref attribute, else its own txn_ref.
func settlementKey(l Leg) string {
	if b := l.Attrs[AttrBatchRef]; b != "" {
		return b
	}
	return l.TxnRef
}

func filterLegs(ls []Leg, keep func(Leg) bool) []Leg {
	out := make([]Leg, 0, len(ls))
	for _, l := range ls {
		if keep(l) {
			out = append(out, l)
		}
	}
	return out
}

func (m matcher) pair(l, c Leg, tier Tier, rule string, conf float64) Match {
	ids := []string{l.ID, c.ID}
	sort.Strings(ids)
	return Match{
		ID:            DeriveID("m", ids),
		LegIDs:        ids,
		Tier:          tier,
		RuleID:        rule,
		Confidence:    conf,
		ResidualMinor: c.AmountMinor - l.AmountMinor,
		Currency:      l.Currency,
	}
}

// many builds a T3 match; residual is sum(many) - anchor.
func (m matcher) many(anchor Leg, many []Leg, tier Tier, rule string, tol int64) Match {
	ids := []string{anchor.ID}
	var sum int64
	for _, x := range many {
		ids = append(ids, x.ID)
		sum += x.AmountMinor
	}
	sort.Strings(ids)
	residual := sum - anchor.AmountMinor
	conf := tolerantConfidence(residual, tol) - 0.10
	return Match{
		ID:            DeriveID("m", ids),
		LegIDs:        ids,
		Tier:          tier,
		RuleID:        rule,
		Confidence:    conf,
		ResidualMinor: residual,
		Currency:      anchor.Currency,
	}
}

// subsetSum finds the first (in deterministic order) subset of pool with
// size in [minSize,maxSize] whose amounts sum to target ± tol. Amounts are
// positive, so the search prunes when the running sum exceeds target+tol.
// Complexity is bounded by len(pool) <= T3MaxCandidates and depth <= maxSize.
func subsetSum(pool []Leg, target, tol int64, minSize, maxSize int, chosen []Leg) ([]Leg, bool) {
	if maxSize <= 0 || len(pool) == 0 {
		return nil, false
	}
	// Sort by amount descending so large legs are placed first and pruning bites early.
	sorted := append([]Leg(nil), pool...)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].AmountMinor > sorted[j].AmountMinor })

	var dfs func(start int, remaining int64, depth int) bool
	var out []Leg
	budget := 200_000 // hard cap on explored nodes; deterministic
	dfs = func(start int, remaining int64, depth int) bool {
		if budget <= 0 {
			return false
		}
		budget--
		if depth >= minSize && abs64(remaining) <= tol {
			return true
		}
		if depth == maxSize {
			return false
		}
		for i := start; i < len(sorted); i++ {
			a := sorted[i].AmountMinor
			if a > remaining+tol {
				continue // too big; try a smaller one (sorted desc)
			}
			out = append(out, sorted[i])
			if dfs(i+1, remaining-a, depth+1) {
				return true
			}
			out = out[:len(out)-1]
		}
		return false
	}
	if dfs(0, target, 0) {
		return append(chosen, out...), true
	}
	return nil, false
}

func groupBySource(ls []Leg) map[string][]Leg {
	g := make(map[string][]Leg)
	for _, l := range ls {
		g[l.Source] = append(g[l.Source], l)
	}
	return g
}

func sortedKeys(m map[string][]Leg) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func capCandidates(ls []Leg, n int) []Leg {
	if len(ls) <= n {
		return ls
	}
	return ls[:n]
}

func abs64(x int64) int64 {
	if x < 0 {
		return -x
	}
	return x
}

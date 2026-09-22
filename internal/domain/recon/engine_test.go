package recon_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/udaykishore-resu/recon-stream/internal/adapters/memory"
	"github.com/udaykishore-resu/recon-stream/internal/domain/classify"
	"github.com/udaykishore-resu/recon-stream/internal/domain/recon"
	"github.com/udaykishore-resu/recon-stream/internal/ports"
)

type fixture struct {
	t     *testing.T
	ctx   context.Context
	store *memory.Store
	eng   *recon.Engine
	now   time.Time
}

func newFixture(t *testing.T, mutate func(*recon.Rules)) *fixture {
	t.Helper()
	rules := recon.DefaultRules()
	if mutate != nil {
		mutate(&rules)
	}
	st := memory.NewStore()
	eng, err := recon.NewEngine(st, classify.NewHeuristic(), rules, 24*time.Hour, recon.Hooks{})
	if err != nil {
		t.Fatal(err)
	}
	f := &fixture{t: t, ctx: context.Background(), store: st, eng: eng, now: time.Date(2026, 9, 15, 9, 0, 0, 0, time.UTC)}
	eng.SetClock(func() time.Time { return f.now })
	return f
}

func leg(src, ref string, amt int64, ccy string, date recon.Date, attrs map[string]string) recon.Leg {
	dir := recon.Debit
	if src != "ledger" {
		dir = recon.Credit
	}
	return recon.Leg{Source: src, TxnRef: ref, AmountMinor: amt, Currency: ccy, ValueDate: date, Direction: dir, Counterparty: "VISA", Attrs: attrs}
}

var (
	d15 = recon.NewDate(2026, 9, 15)
	b1  = map[string]string{recon.AttrBatchRef: "BATCH-1"}
	b2  = map[string]string{recon.AttrBatchRef: "BATCH-2"}
)

func TestT3HintBlocksCoincidentalSums(t *testing.T) {
	f := newFixture(t, nil)
	res := f.ingest(
		leg("ledger", "C1", 1_000, "USD", d15, nil), leg("ledger", "C2", 2_500, "USD", d15, nil),
		leg("ledger", "C3", 4_000, "USD", d15, nil), leg("rail", "STL-9", 7_500, "USD", d15, nil),
	)
	if res.Matched != 0 || res.Open != 4 {
		t.Fatalf("unhinted sum must not match by default: %+v", res)
	}
}

func (f *fixture) ingest(legs ...recon.Leg) recon.BatchResult {
	f.t.Helper()
	res, err := f.eng.Ingest(f.ctx, legs)
	if err != nil {
		f.t.Fatalf("ingest: %v", err)
	}
	return res
}

func (f *fixture) stats() ports.Stats {
	st, err := f.store.Stats(f.ctx)
	if err != nil {
		f.t.Fatal(err)
	}
	return st
}

func TestMatchingTiers(t *testing.T) {
	cases := []struct {
		name     string
		rules    func(*recon.Rules)
		legs     []recon.Leg
		wantRule string
		wantLegs int
		wantConf float64
		wantOpen int
	}{
		{
			name:     "T1 exact",
			legs:     []recon.Leg{leg("ledger", "A", 10_000, "USD", d15, nil), leg("rail", "A", 10_000, "USD", d15, nil)},
			wantRule: recon.RuleT1Exact, wantLegs: 2, wantConf: 1.0,
		},
		{
			name:     "T2 ref tolerant within 10 bps",
			legs:     []recon.Leg{leg("ledger", "B", 10_000, "USD", d15, nil), leg("rail", "B", 10_007, "USD", recon.NewDate(2026, 9, 17), nil)},
			wantRule: recon.RuleT2Tolerant, wantLegs: 2, wantConf: 0.845,
		},
		{
			name:     "T2 amount+date unique, refs differ",
			legs:     []recon.Leg{leg("ledger", "INT-1", 4_242, "USD", d15, nil), leg("rail", "ARN-9", 4_242, "USD", d15, nil)},
			wantRule: recon.RuleT2AmountDate, wantLegs: 2, wantConf: 0.75,
		},
		{
			name: "T3 hinted, settlement arrives last, extra unrelated legs ignored",
			legs: []recon.Leg{
				leg("ledger", "L1", 1_000, "USD", d15, b1), leg("ledger", "L2", 2_500, "USD", d15, b1),
				leg("ledger", "NOISE", 4_000, "USD", d15, nil), // same amount as L3 but not in the batch
				leg("ledger", "L3", 4_000, "USD", d15, b1), leg("rail", "BATCH-1", 7_500, "USD", d15, nil),
			},
			wantRule: recon.RuleT3Subset, wantLegs: 4, wantConf: 0.85, wantOpen: 1,
		},
		{
			name: "T3 hinted via settlement batch_ref, partial batch settled",
			legs: []recon.Leg{
				leg("ledger", "P1", 1_000, "USD", d15, b1), leg("ledger", "P2", 2_500, "USD", d15, b1),
				leg("ledger", "P3", 4_000, "USD", d15, b1), leg("rail", "STL-77", 3_500, "USD", d15, b1),
			},
			wantRule: recon.RuleT3Subset, wantLegs: 3, wantConf: 0.85, wantOpen: 1,
		},
		{
			name: "T3 hinted, settlement arrives first, last ledger leg completes it",
			legs: []recon.Leg{
				leg("rail", "BATCH-2", 7_500, "USD", d15, nil),
				leg("ledger", "M1", 1_000, "USD", d15, b2), leg("ledger", "M2", 2_500, "USD", d15, b2),
				leg("ledger", "M3", 4_000, "USD", d15, b2),
			},
			wantRule: recon.RuleT3Subset, wantLegs: 4, wantConf: 0.85,
		},
		{
			name:  "T3 unhinted (opt-in) settlement arrives last",
			rules: func(r *recon.Rules) { r.T3RequireBatchHint = false },
			legs: []recon.Leg{
				leg("ledger", "U1", 1_000, "USD", d15, nil), leg("ledger", "U2", 2_500, "USD", d15, nil),
				leg("ledger", "U3", 4_000, "USD", d15, nil), leg("rail", "BATCH-3", 7_500, "USD", d15, nil),
			},
			wantRule: recon.RuleT3Subset, wantLegs: 4, wantConf: 0.85,
		},
		{
			name:  "T3 unhinted, settlement first",
			rules: func(r *recon.Rules) { r.T3RequireBatchHint = false },
			legs: []recon.Leg{
				leg("rail", "BATCH-4", 7_500, "USD", d15, nil),
				leg("ledger", "V1", 1_000, "USD", d15, nil), leg("ledger", "V2", 2_500, "USD", d15, nil),
				leg("ledger", "V3", 4_000, "USD", d15, nil),
			},
			wantRule: recon.RuleT3Subset, wantLegs: 4, wantConf: 0.85,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, tc.rules)
			res := f.ingest(tc.legs...)
			last := res.Results[len(res.Results)-1]
			if last.Outcome != recon.OutcomeMatched {
				t.Fatalf("last outcome=%s (%+v)", last.Outcome, res)
			}
			m, err := f.store.GetMatch(f.ctx, last.MatchID)
			if err != nil {
				t.Fatal(err)
			}
			if m.RuleID != tc.wantRule || len(m.LegIDs) != tc.wantLegs {
				t.Fatalf("match=%+v", m)
			}
			if m.Confidence != tc.wantConf {
				t.Fatalf("confidence=%v want %v", m.Confidence, tc.wantConf)
			}
			if f.eng.OpenLegs() != tc.wantOpen {
				t.Fatalf("open legs=%d want %d", f.eng.OpenLegs(), tc.wantOpen)
			}
			st := f.stats()
			if st.LegsMatched != int64(tc.wantLegs) || st.AutoMatchRate != 1 {
				t.Fatalf("stats=%+v", st)
			}
		})
	}
}

func TestAmountDateFallbackIsConservative(t *testing.T) {
	f := newFixture(t, nil)
	// Two ledger legs with the same amount: ambiguous -> nobody matches by amount alone.
	res := f.ingest(
		leg("ledger", "X1", 999, "USD", d15, nil),
		leg("ledger", "X2", 999, "USD", d15, nil),
		leg("rail", "ARN-77", 999, "USD", d15, nil),
	)
	if res.Matched != 0 || res.Open != 3 {
		t.Fatalf("expected ambiguity to block matching: %+v", res)
	}
	// Our own ref is already known on the other side (its leg expired into a
	// break): the amount-only fallback must not grab an unrelated open leg with
	// the same amount; the late-arrival rule heals the real partner instead.
	g := newFixture(t, nil)
	g.ingest(leg("rail", "R-1", 500, "USD", d15, nil))
	if n, _ := g.eng.CloseWindow(g.ctx, 0); n != 1 {
		t.Fatal("expected R-1 to expire")
	}
	g.ingest(leg("rail", "R-9", 500, "USD", d15, nil)) // unrelated, same amount, open
	r := g.ingest(leg("ledger", "R-1", 500, "USD", d15, nil))
	if r.Results[0].Outcome != recon.OutcomeResolvedBreak || r.Results[0].RuleID != recon.RuleT2Late {
		t.Fatalf("fallback should be blocked in favour of the real partner: %+v", r.Results[0])
	}
	if g.eng.OpenLegs() != 1 {
		t.Fatalf("R-9 must remain open, got %d open legs", g.eng.OpenLegs())
	}
}

func TestBreaksAreOpenedAndClassified(t *testing.T) {
	cases := []struct {
		name        string
		legs        []recon.Leg
		wantCat     recon.Category
		wantTrigger string
		wantLegs    int
	}{
		{
			name:        "fee deducted by rail",
			legs:        []recon.Leg{leg("ledger", "F1", 100_000, "USD", d15, nil), leg("rail", "F1", 97_500, "USD", d15, nil)},
			wantCat:     recon.CatFee,
			wantTrigger: recon.TriggerRefConflict, wantLegs: 2,
		},
		{
			name:        "fee with explicit attribute",
			legs:        []recon.Leg{leg("ledger", "F2", 100_000, "USD", d15, nil), leg("rail", "F2", 92_000, "USD", d15, map[string]string{"fee_minor": "8000"})},
			wantCat:     recon.CatFee,
			wantTrigger: recon.TriggerRefConflict, wantLegs: 2,
		},
		{
			name:        "fx drift across currencies",
			legs:        []recon.Leg{leg("ledger", "X1", 100_000, "USD", d15, nil), leg("rail", "X1", 91_800, "EUR", d15, map[string]string{"fx_rate": "0.918"})},
			wantCat:     recon.CatFXDrift,
			wantTrigger: recon.TriggerRefConflict, wantLegs: 2,
		},
		{
			name:        "timing outside value-date window",
			legs:        []recon.Leg{leg("ledger", "T1", 5_000, "USD", d15, nil), leg("rail", "T1", 5_000, "USD", recon.NewDate(2026, 9, 22), nil)},
			wantCat:     recon.CatTiming,
			wantTrigger: recon.TriggerRefConflict, wantLegs: 2,
		},
		{
			name: "duplicate from same source",
			legs: []recon.Leg{
				leg("ledger", "D1", 5_000, "USD", d15, nil), leg("rail", "D1", 5_000, "USD", d15, nil),
				leg("rail", "D1", 5_000, "USD", d15, map[string]string{"batch": "B2"}),
			},
			wantCat:     recon.CatDuplicate,
			wantTrigger: recon.TriggerDuplicate, wantLegs: 1,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, nil)
			res := f.ingest(tc.legs...)
			last := res.Results[len(res.Results)-1]
			if last.Outcome != recon.OutcomeBreak {
				t.Fatalf("outcome=%+v", res)
			}
			b, err := f.store.GetBreak(f.ctx, last.BreakID)
			if err != nil {
				t.Fatal(err)
			}
			if b.Category != tc.wantCat || b.Trigger != tc.wantTrigger || len(b.LegIDs) != tc.wantLegs || b.Status != recon.BreakOpen {
				t.Fatalf("break=%+v", b)
			}
			if b.ClassifierID != classify.HeuristicID || b.Confidence <= 0 || b.Reason == "" {
				t.Fatalf("classification not auditable: %+v", b)
			}
			if f.eng.OpenLegs() != 0 {
				t.Fatalf("legs left in index: %d", f.eng.OpenLegs())
			}
			for _, id := range b.LegIDs {
				l, _ := f.store.GetLeg(f.ctx, id)
				if l.Status != recon.LegBroken || l.BreakID != b.ID {
					t.Fatalf("leg not linked to break: %+v", l)
				}
			}
		})
	}
}

func TestIdempotentRedelivery(t *testing.T) {
	f := newFixture(t, nil)
	a := leg("ledger", "A", 10_000, "USD", d15, nil)
	b := leg("rail", "A", 10_000, "USD", d15, nil)
	f.ingest(a, b)
	before := f.stats()
	res := f.ingest(a, b, a)
	if res.Duplicates != 3 {
		t.Fatalf("expected 3 duplicates: %+v", res)
	}
	after := f.stats()
	if after.LegsTotal != before.LegsTotal || after.MatchesTotal != before.MatchesTotal {
		t.Fatalf("redelivery changed state: %+v -> %+v", before, after)
	}
	if after.EventsTotal != before.EventsTotal+3 {
		t.Fatalf("duplicates must be audited as events: %d -> %d", before.EventsTotal, after.EventsTotal)
	}
}

func TestExpiryAndLateArrival(t *testing.T) {
	f := newFixture(t, nil)
	f.ingest(leg("ledger", "LATE", 3_000, "USD", d15, nil))
	if n, _ := f.eng.Sweep(f.ctx); n != 0 {
		t.Fatalf("nothing should expire yet, got %d", n)
	}
	f.now = f.now.Add(25 * time.Hour)
	n, err := f.eng.Sweep(f.ctx)
	if err != nil || n != 1 {
		t.Fatalf("sweep=%d err=%v", n, err)
	}
	open, _ := f.store.ListBreaks(f.ctx, ports.BreakFilter{Status: recon.BreakOpen})
	if len(open) != 1 || open[0].Category != recon.CatMissingCounterparty || open[0].Trigger != recon.TriggerWindowExpired {
		t.Fatalf("breaks=%+v", open)
	}
	// Partner finally arrives (within tolerance): break resolved, match created.
	res := f.ingest(leg("rail", "LATE", 3_001, "USD", d15, nil))
	if res.Results[0].Outcome != recon.OutcomeResolvedBreak || res.Results[0].RuleID != recon.RuleT2Late {
		t.Fatalf("late arrival not healed: %+v", res.Results[0])
	}
	b, _ := f.store.GetBreak(f.ctx, open[0].ID)
	if b.Status != recon.BreakResolved || b.Actor != "system" {
		t.Fatalf("break not resolved: %+v", b)
	}
	st := f.stats()
	if st.BreaksOpen != 0 || st.BreaksResolved != 1 || st.MatchesByRule[recon.RuleT2Late] != 1 || st.AutoMatchRate != 1 {
		t.Fatalf("stats=%+v", st)
	}
}

func TestLateArrivalOutsideToleranceOpensOwnBreak(t *testing.T) {
	f := newFixture(t, nil)
	f.ingest(leg("ledger", "LATE2", 100_000, "USD", d15, nil))
	if n, _ := f.eng.CloseWindow(f.ctx, 0); n != 1 {
		t.Fatal("close window should expire the leg")
	}
	res := f.ingest(leg("rail", "LATE2", 97_000, "USD", d15, nil))
	if res.Results[0].Outcome != recon.OutcomeBreak {
		t.Fatalf("expected break: %+v", res.Results[0])
	}
	b, _ := f.store.GetBreak(f.ctx, res.Results[0].BreakID)
	if b.Category != recon.CatFee {
		t.Fatalf("classifier should see the broken partner via Related: %+v", b)
	}
}

func TestResolveBreakAudit(t *testing.T) {
	f := newFixture(t, nil)
	res := f.ingest(leg("ledger", "F1", 100_000, "USD", d15, nil), leg("rail", "F1", 97_500, "USD", d15, nil))
	id := res.Results[1].BreakID
	if _, err := f.eng.ResolveBreak(f.ctx, id, "", "ops"); err == nil {
		t.Fatal("reason required")
	}
	b, err := f.eng.ResolveBreak(f.ctx, id, "interchange fee booked to 4210", "ops@bank")
	if err != nil || b.Status != recon.BreakResolved || b.Actor != "ops@bank" || b.ResolvedAt == nil {
		t.Fatalf("resolve: %+v %v", b, err)
	}
	if _, err := f.eng.ResolveBreak(f.ctx, id, "again", "ops"); !errors.Is(err, recon.ErrBreakResolved) {
		t.Fatalf("want ErrBreakResolved, got %v", err)
	}
	if _, err := f.eng.ResolveBreak(f.ctx, "brk_missing", "x", "y"); !errors.Is(err, ports.ErrNotFound) {
		t.Fatalf("want not found, got %v", err)
	}
	evs, _ := f.store.EventsFrom(f.ctx, 1, 100)
	var sawResolved bool
	for _, ev := range evs {
		if ev.Type == recon.EvBreakResolved && ev.AggregateID == id {
			sawResolved = true
		}
	}
	if !sawResolved {
		t.Fatal("break.resolved event missing")
	}
	if b.Age(f.now) != 0 {
		t.Fatalf("age should be measured to resolution: %v", b.Age(f.now))
	}
}

func TestListBreaksFilters(t *testing.T) {
	f := newFixture(t, nil)
	f.ingest(leg("ledger", "F1", 100_000, "USD", d15, nil), leg("rail", "F1", 97_500, "USD", d15, nil))
	f.now = f.now.Add(time.Hour)
	f.ingest(leg("ledger", "X1", 100_000, "GBP", d15, nil), leg("rail", "X1", 100_000, "EUR", d15, nil))
	all, _ := f.store.ListBreaks(f.ctx, ports.BreakFilter{})
	if len(all) != 2 || all[0].Category != recon.CatFee {
		t.Fatalf("ordering/all: %+v", all)
	}
	fx, _ := f.store.ListBreaks(f.ctx, ports.BreakFilter{Category: recon.CatFXDrift})
	if len(fx) != 1 || fx[0].Currency != "EUR" { // break carries the incoming leg's currency
		t.Fatalf("fx filter: %+v", fx)
	}
	aged, _ := f.store.ListBreaks(f.ctx, ports.BreakFilter{MinAge: 30 * time.Minute})
	if len(aged) != 2 { // store clock is wall time; both were opened in the past
		t.Fatalf("aged: %d", len(aged))
	}
	page, _ := f.store.ListBreaks(f.ctx, ports.BreakFilter{Limit: 1, Offset: 1})
	if len(page) != 1 || page[0].Category != recon.CatFXDrift {
		t.Fatalf("paging: %+v", page)
	}
	if none, _ := f.store.ListBreaks(f.ctx, ports.BreakFilter{Offset: 10}); len(none) != 0 {
		t.Fatal("offset past end should be empty")
	}
}

func TestValidationIsAllOrNothing(t *testing.T) {
	f := newFixture(t, nil)
	_, err := f.eng.Ingest(f.ctx, []recon.Leg{leg("ledger", "OK", 1, "USD", d15, nil), leg("rail", "", 1, "USD", d15, nil)})
	if !errors.Is(err, recon.ErrInvalidLeg) {
		t.Fatalf("want ErrInvalidLeg, got %v", err)
	}
	if st := f.stats(); st.LegsTotal != 0 {
		t.Fatal("nothing should have been persisted")
	}
}

func TestRebuildRestoresIndex(t *testing.T) {
	f := newFixture(t, nil)
	f.ingest(leg("ledger", "OPEN-1", 1_000, "USD", d15, nil), leg("ledger", "OPEN-2", 2_000, "USD", d15, nil))
	eng2, err := recon.NewEngine(f.store, classify.NewHeuristic(), recon.DefaultRules(), time.Hour, recon.Hooks{})
	if err != nil {
		t.Fatal(err)
	}
	if err := eng2.Rebuild(f.ctx); err != nil {
		t.Fatal(err)
	}
	if eng2.OpenLegs() != 2 {
		t.Fatalf("open=%d", eng2.OpenLegs())
	}
	res, err := eng2.Ingest(f.ctx, []recon.Leg{leg("rail", "OPEN-1", 1_000, "USD", d15, nil)})
	if err != nil || res.Matched != 1 {
		t.Fatalf("rebuilt engine should match: %+v %v", res, err)
	}
}

// Golden: replaying the event log must reproduce the identical set of match
// and break IDs (they are derived from leg IDs, never random).
func TestReplayIsDeterministic(t *testing.T) {
	f := newFixture(t, nil)
	f.ingest(
		leg("ledger", "A", 10_000, "USD", d15, nil), leg("rail", "A", 10_000, "USD", d15, nil),
		leg("ledger", "F1", 100_000, "USD", d15, nil), leg("rail", "F1", 97_500, "USD", d15, nil),
		leg("ledger", "L1", 1_000, "USD", d15, map[string]string{recon.AttrBatchRef: "B1"}),
		leg("ledger", "L2", 2_500, "USD", d15, map[string]string{recon.AttrBatchRef: "B1"}),
		leg("rail", "B1", 3_500, "USD", d15, nil),
		leg("ledger", "ORPHAN", 77, "USD", d15, nil),
	)
	f.ingest(leg("rail", "A", 10_000, "USD", d15, nil)) // duplicate delivery
	feeBreaks, _ := f.store.ListBreaks(f.ctx, ports.BreakFilter{Category: recon.CatFee})
	if _, err := f.eng.ResolveBreak(f.ctx, feeBreaks[0].ID, "fee booked", "ops"); err != nil {
		t.Fatal(err)
	}
	before := f.stats()
	beforeBreaks, _ := f.store.ListBreaks(f.ctx, ports.BreakFilter{})
	beforeEvents := before.EventsTotal

	res, err := f.eng.Replay(f.ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	// Per-leg outcomes: rail A and rail B1 matched, rail F1 broke, five legs first waited.
	if res.LegsReplayed != 8 || res.Matched != 2 || res.Breaks != 1 || res.Open != 5 {
		t.Fatalf("replay=%+v", res)
	}
	after := f.stats()
	afterBreaks, _ := f.store.ListBreaks(f.ctx, ports.BreakFilter{})
	if after.LegsTotal != before.LegsTotal || after.MatchesTotal != before.MatchesTotal || after.BreaksOpen != before.BreaksOpen {
		t.Fatalf("projection differs after replay: %+v vs %+v", before, after)
	}
	if len(afterBreaks) != len(beforeBreaks) || afterBreaks[0].ID != beforeBreaks[0].ID {
		t.Fatalf("break ids differ: %+v vs %+v", beforeBreaks, afterBreaks)
	}
	if after.BreaksResolved != 1 {
		t.Fatalf("manual resolution must survive replay: %+v", after)
	}
	if rb, _ := f.store.GetBreak(f.ctx, feeBreaks[0].ID); rb.Status != recon.BreakResolved || rb.Actor != "ops" || rb.Reason != "fee booked" {
		t.Fatalf("resolution not restored: %+v", rb)
	}
	// leg.ingested events are not duplicated; only derived events + replay marker are appended.
	evs, _ := f.store.EventsFrom(f.ctx, beforeEvents+1, 1000)
	if evs[0].Type != recon.EvReplayStarted {
		t.Fatalf("first new event should be replay.started, got %s", evs[0].Type)
	}
	for _, ev := range evs {
		if ev.Type == recon.EvLegIngested || ev.Type == recon.EvLegDuplicate {
			t.Fatalf("replay must not re-append %s", ev.Type)
		}
	}
	if f.eng.OpenLegs() != 1 {
		t.Fatalf("open after replay=%d", f.eng.OpenLegs())
	}
	// Replay from a later checkpoint drops earlier legs from the projection.
	res2, err := f.eng.Replay(f.ctx, beforeEvents)
	if err != nil {
		t.Fatal(err)
	}
	if res2.LegsReplayed != 0 {
		t.Fatalf("checkpoint replay should skip earlier legs: %+v", res2)
	}
}

func TestNewEngineRejectsBadConfig(t *testing.T) {
	st := memory.NewStore()
	if _, err := recon.NewEngine(st, classify.NewHeuristic(), recon.DefaultRules(), 0, recon.Hooks{}); err == nil {
		t.Fatal("ttl must be validated")
	}
	if _, err := recon.NewEngine(nil, classify.NewHeuristic(), recon.DefaultRules(), time.Hour, recon.Hooks{}); err == nil {
		t.Fatal("repo required")
	}
	bad := recon.DefaultRules()
	bad.ToleranceBps = -1
	if _, err := recon.NewEngine(st, classify.NewHeuristic(), bad, time.Hour, recon.Hooks{}); err == nil {
		t.Fatal("rules must be validated")
	}
}

func TestHooksFire(t *testing.T) {
	var matches, breaks, legs int
	var open int
	rules := recon.DefaultRules()
	st := memory.NewStore()
	eng, _ := recon.NewEngine(st, classify.NewHeuristic(), rules, time.Hour, recon.Hooks{
		OnLeg:   func(recon.Leg, recon.Outcome) { legs++ },
		OnMatch: func(recon.Match) { matches++ },
		OnBreak: func(recon.Break) { breaks++ },
		OnOpen:  func(n int) { open = n },
	})
	_, err := eng.Ingest(context.Background(), []recon.Leg{
		leg("ledger", "A", 100, "USD", d15, nil), leg("rail", "A", 100, "USD", d15, nil),
		leg("ledger", "F", 100_000, "USD", d15, nil), leg("rail", "F", 97_500, "USD", d15, nil),
		leg("ledger", "O", 5, "USD", d15, nil),
	})
	if err != nil {
		t.Fatal(err)
	}
	if legs != 5 || matches != 1 || breaks != 1 || open != 1 {
		t.Fatalf("hooks: legs=%d matches=%d breaks=%d open=%d", legs, matches, breaks, open)
	}
}

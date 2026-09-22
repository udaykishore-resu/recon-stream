package classify

import (
	"testing"

	"github.com/udaykishore-resu/recon-stream/internal/domain/recon"
)

func mk(src, ref string, amt int64, ccy string, date recon.Date, attrs map[string]string) recon.Leg {
	l := recon.Leg{Source: src, TxnRef: ref, AmountMinor: amt, Currency: ccy, ValueDate: date, Direction: recon.Debit, Counterparty: "VISA", Attrs: attrs}
	l.AssignID()
	return l
}

func TestHeuristicClassify(t *testing.T) {
	d := recon.NewDate(2026, 9, 15)
	late := recon.NewDate(2026, 9, 25)
	rules := recon.DefaultRules()
	h := NewHeuristic()
	cases := []struct {
		name    string
		in      recon.ClassifyInput
		wantCat recon.Category
		minConf float64
	}{
		{
			name: "duplicate wins over everything",
			in: recon.ClassifyInput{
				Legs:    []recon.Leg{mk("rail", "R1", 100, "USD", d, map[string]string{"batch": "2"})},
				Related: []recon.Leg{mk("rail", "R1", 100, "USD", d, nil), mk("ledger", "R1", 100, "USD", d, nil)},
				Trigger: recon.TriggerDuplicate, Rules: rules,
			},
			wantCat: recon.CatDuplicate, minConf: 0.9,
		},
		{
			name:    "fx by currency mismatch",
			in:      recon.ClassifyInput{Legs: []recon.Leg{mk("ledger", "X", 100_000, "USD", d, nil), mk("rail", "X", 91_000, "EUR", d, nil)}, Trigger: recon.TriggerRefConflict, Rules: rules},
			wantCat: recon.CatFXDrift, minConf: 0.9,
		},
		{
			name:    "fx by attribute, same currency",
			in:      recon.ClassifyInput{Legs: []recon.Leg{mk("ledger", "X", 100_000, "USD", d, nil), mk("rail", "X", 99_100, "USD", d, map[string]string{"fx_rate": "1.009"})}, Trigger: recon.TriggerRefConflict, Rules: rules},
			wantCat: recon.CatFXDrift, minConf: 0.8,
		},
		{
			name:    "timing: amounts agree, dates far apart",
			in:      recon.ClassifyInput{Legs: []recon.Leg{mk("ledger", "T", 5_000, "USD", d, nil), mk("rail", "T", 5_000, "USD", late, nil)}, Trigger: recon.TriggerRefConflict, Rules: rules},
			wantCat: recon.CatTiming, minConf: 0.85,
		},
		{
			name:    "fee by attribute",
			in:      recon.ClassifyInput{Legs: []recon.Leg{mk("ledger", "F", 100_000, "USD", d, nil), mk("rail", "F", 90_000, "USD", d, map[string]string{"interchange_minor": "10000"})}, Trigger: recon.TriggerRefConflict, Rules: rules},
			wantCat: recon.CatFee, minConf: 0.9,
		},
		{
			name:    "fee by band (2.5%)",
			in:      recon.ClassifyInput{Legs: []recon.Leg{mk("ledger", "F", 100_000, "USD", d, nil), mk("rail", "F", 97_500, "USD", d, nil)}, Trigger: recon.TriggerRefConflict, Rules: rules},
			wantCat: recon.CatFee, minConf: 0.6,
		},
		{
			name:    "difference too large for a fee",
			in:      recon.ClassifyInput{Legs: []recon.Leg{mk("ledger", "F", 100_000, "USD", d, nil), mk("rail", "F", 50_000, "USD", d, nil)}, Trigger: recon.TriggerRefConflict, Rules: rules},
			wantCat: recon.CatUnclassified, minConf: 0.3,
		},
		{
			name:    "expired ledger leg alone",
			in:      recon.ClassifyInput{Legs: []recon.Leg{mk("ledger", "M", 100, "USD", d, nil)}, Trigger: recon.TriggerWindowExpired, Rules: rules},
			wantCat: recon.CatMissingCounterparty, minConf: 0.8,
		},
		{
			name:    "expired rail leg alone",
			in:      recon.ClassifyInput{Legs: []recon.Leg{mk("rail", "M", 100, "USD", d, nil)}, Trigger: recon.TriggerWindowExpired, Rules: rules},
			wantCat: recon.CatMissingCounterparty, minConf: 0.7,
		},
		{
			name:    "partner found via related broken leg",
			in:      recon.ClassifyInput{Legs: []recon.Leg{mk("rail", "F", 97_500, "USD", d, nil)}, Related: []recon.Leg{mk("ledger", "F", 100_000, "USD", d, nil)}, Trigger: recon.TriggerRefConflict, Rules: rules},
			wantCat: recon.CatFee, minConf: 0.6,
		},
		{
			name:    "single leg, unknown trigger",
			in:      recon.ClassifyInput{Legs: []recon.Leg{mk("rail", "Z", 100, "USD", d, nil)}, Trigger: "other", Rules: rules},
			wantCat: recon.CatUnclassified, minConf: 0.2,
		},
		{
			name:    "no legs",
			in:      recon.ClassifyInput{Rules: rules},
			wantCat: recon.CatUnclassified, minConf: 0,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := h.Classify(tc.in)
			if got.Category != tc.wantCat {
				t.Fatalf("category=%s want %s (%s)", got.Category, tc.wantCat, got.Evidence)
			}
			if got.Confidence < tc.minConf || got.Confidence > 1 {
				t.Fatalf("confidence=%v", got.Confidence)
			}
			if got.ClassifierID != HeuristicID || got.Evidence == "" {
				t.Fatalf("not auditable: %+v", got)
			}
		})
	}
}

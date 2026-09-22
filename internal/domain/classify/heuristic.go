// Package classify holds BreakClassifier implementations. The heuristic
// classifier is deterministic and explainable; a learned model can be dropped
// in behind the same interface without touching the matching engine.
package classify

import (
	"fmt"
	"math"
	"strings"

	"github.com/udaykishore-resu/recon-stream/internal/domain/recon"
)

// HeuristicID is the auditable identifier recorded on every break.
const HeuristicID = "heuristic@v1"

// Heuristic labels breaks with fee / fx_drift / timing / duplicate /
// missing_counterparty using ordered rules. Earlier rules win.
type Heuristic struct {
	// FeeMinBps/FeeMaxBps bound the typical scheme/interchange fee band.
	FeeMinBps, FeeMaxBps float64
	// FeeAttrKeys are attribute keys whose presence strongly suggests a fee.
	FeeAttrKeys []string
	// FXAttrKeys are attribute keys whose presence strongly suggests an FX conversion.
	FXAttrKeys []string
}

// NewHeuristic returns the classifier with production defaults
// (fee band 20 bps .. 400 bps, i.e. 0.2% .. 4%).
func NewHeuristic() *Heuristic {
	return &Heuristic{
		FeeMinBps:   20,
		FeeMaxBps:   400,
		FeeAttrKeys: []string{"fee_minor", "fee", "interchange_minor", "scheme_fee_minor"},
		FXAttrKeys:  []string{"fx_rate", "original_currency", "settlement_currency"},
	}
}

// Classify implements recon.BreakClassifier.
func (h *Heuristic) Classify(in recon.ClassifyInput) recon.Classification {
	out := func(cat recon.Category, conf float64, evidence string) recon.Classification {
		return recon.Classification{Category: cat, Confidence: round(conf), ClassifierID: HeuristicID, Evidence: evidence}
	}
	if len(in.Legs) == 0 {
		return out(recon.CatUnclassified, 0, "no legs")
	}
	primary := in.Legs[0]

	// Rule D1: duplicate — another leg from the same source re-used the txn_ref.
	for _, r := range in.Related {
		if r.Source == primary.Source && r.ID != primary.ID {
			return out(recon.CatDuplicate, 0.95, fmt.Sprintf("same source %q re-used txn_ref %q (existing leg %s)", primary.Source, primary.TxnRef, r.ID))
		}
	}

	// Find the counterparty leg: a break partner, else a related opposite-source leg.
	partner, hasPartner := opposite(primary, in.Legs[1:])
	if !hasPartner {
		partner, hasPartner = opposite(primary, in.Related)
	}

	if hasPartner {
		// Rule X1: fx_drift — currencies differ, or FX attributes present.
		if partner.Currency != primary.Currency {
			return out(recon.CatFXDrift, 0.92, fmt.Sprintf("currency mismatch %s vs %s on txn_ref %q", primary.Currency, partner.Currency, primary.TxnRef))
		}
		if k, ok := anyAttr(h.FXAttrKeys, primary, partner); ok {
			return out(recon.CatFXDrift, 0.88, fmt.Sprintf("fx attribute %q present; amounts %d vs %d", k, primary.AmountMinor, partner.AmountMinor))
		}

		diff := primary.AmountMinor - partner.AmountMinor
		if diff < 0 {
			diff = -diff
		}
		// Rule T1: timing — amounts agree but value dates are outside the window.
		if in.Rules.WithinTolerance(primary.AmountMinor, partner.AmountMinor) && !in.Rules.WithinDateWindow(primary.ValueDate, partner.ValueDate) {
			return out(recon.CatTiming, 0.90, fmt.Sprintf("amounts agree, value dates %s vs %s exceed ±%dd window", primary.ValueDate, partner.ValueDate, in.Rules.ValueDateWindowDays))
		}
		// Rule F1: fee — explicit fee attribute equals the difference (or is present).
		if k, ok := anyAttr(h.FeeAttrKeys, primary, partner); ok {
			return out(recon.CatFee, 0.95, fmt.Sprintf("fee attribute %q present; amount difference %d", k, diff))
		}
		// Rule F2: fee — difference within the typical fee band.
		base := math.Max(float64(primary.AmountMinor), float64(partner.AmountMinor))
		if base > 0 {
			bps := float64(diff) / base * 10_000
			if bps >= h.FeeMinBps && bps <= h.FeeMaxBps {
				conf := 0.80 - 0.10*math.Abs(bps-150)/250 // most confident around 1.5%
				return out(recon.CatFee, clamp(conf, 0.60, 0.80), fmt.Sprintf("amount difference %d minor = %.1f bps, inside fee band [%.0f,%.0f] bps", diff, bps, h.FeeMinBps, h.FeeMaxBps))
			}
		}
		return out(recon.CatUnclassified, 0.40, fmt.Sprintf("same ref, amount difference %d minor outside every known pattern", diff))
	}

	// Single leg, no counterparty anywhere.
	if in.Trigger == recon.TriggerWindowExpired {
		conf := 0.75
		if primary.Source == "ledger" {
			conf = 0.80 // rails settle late more often than ledgers invent entries
		}
		return out(recon.CatMissingCounterparty, conf, fmt.Sprintf("no leg with txn_ref %q from any other source within the window", primary.TxnRef))
	}
	return out(recon.CatUnclassified, 0.30, "no counterparty and no recognisable pattern")
}

func opposite(primary recon.Leg, legs []recon.Leg) (recon.Leg, bool) {
	for _, l := range legs {
		if l.Source != primary.Source {
			return l, true
		}
	}
	return recon.Leg{}, false
}

func anyAttr(keys []string, legs ...recon.Leg) (string, bool) {
	for _, l := range legs {
		for _, k := range keys {
			if v, ok := l.Attrs[k]; ok && strings.TrimSpace(v) != "" {
				return k, true
			}
		}
	}
	return "", false
}

func clamp(v, lo, hi float64) float64 { return math.Min(hi, math.Max(lo, v)) }

func round(v float64) float64 { return math.Round(v*1000) / 1000 }

package recon

import (
	"errors"
	"fmt"
	"math"
)

// Rule identifiers. Every Match records exactly one of these as rule_id@version
// so that any decision can be audited against the rule text in docs/adr/0002.
const (
	RuleT1Exact      = "T1.exact@v1"        // ref, amount, ccy equal; value date within window
	RuleT2Tolerant   = "T2.ref-tolerant@v1" // ref equal; amount within tolerance; date within window
	RuleT2AmountDate = "T2.amount-date@v1"  // no ref; amount exact; date within window; unique candidate
	RuleT2Late       = "T2.late-arrival@v1" // counterparty leg arrived after its partner had expired into a break
	RuleT3Subset     = "T3.subset-sum@v1"   // one leg vs N opposite legs summing within tolerance
)

// Rules are the tunable parameters of the deterministic matcher.
type Rules struct {
	// ToleranceBps is the relative tolerance in basis points (1 bps = 0.01%).
	ToleranceBps int64
	// ToleranceAbsMinor is the absolute tolerance in minor units. The effective
	// tolerance is max(bps-derived, absolute).
	ToleranceAbsMinor int64
	// ValueDateWindowDays is the ±N day window for value dates.
	ValueDateWindowDays int
	// T3MaxCandidates caps how many opposite-source legs the subset-sum considers.
	T3MaxCandidates int
	// T3MaxSubset caps the number of legs on the "many" side.
	T3MaxSubset int
	// T3RequireBatchHint restricts T3 to legs that share a batch reference
	// (attrs["batch_ref"], or the settlement leg's txn_ref). Unhinted subset
	// sums over a wide window produce spurious matches; leave this on unless
	// the feeds carry no batch identifiers at all.
	T3RequireBatchHint bool
}

// AttrBatchRef is the attribute that links N legs to their settlement batch.
const AttrBatchRef = "batch_ref"

// DefaultRules are conservative production defaults: 10 bps or 2 minor units,
// ±2 value days (T+2 settlement), and a bounded T3 search.
func DefaultRules() Rules {
	return Rules{
		ToleranceBps:        10,
		ToleranceAbsMinor:   2,
		ValueDateWindowDays: 2,
		T3MaxCandidates:     24,
		T3MaxSubset:         6,
		T3RequireBatchHint:  true,
	}
}

// Validate rejects parameters that would make matching unsafe or unbounded.
func (r Rules) Validate() error {
	var errs []error
	if r.ToleranceBps < 0 || r.ToleranceBps > 10_000 {
		errs = append(errs, fmt.Errorf("tolerance_bps must be within [0,10000], got %d", r.ToleranceBps))
	}
	if r.ToleranceAbsMinor < 0 {
		errs = append(errs, fmt.Errorf("tolerance_abs_minor must be >= 0, got %d", r.ToleranceAbsMinor))
	}
	if r.ValueDateWindowDays < 0 || r.ValueDateWindowDays > 365 {
		errs = append(errs, fmt.Errorf("value_date_window_days must be within [0,365], got %d", r.ValueDateWindowDays))
	}
	if r.T3MaxCandidates < 2 || r.T3MaxCandidates > 64 {
		errs = append(errs, fmt.Errorf("t3_max_candidates must be within [2,64], got %d", r.T3MaxCandidates))
	}
	if r.T3MaxSubset < 2 || r.T3MaxSubset > 12 {
		errs = append(errs, fmt.Errorf("t3_max_subset must be within [2,12], got %d", r.T3MaxSubset))
	}
	return errors.Join(errs...)
}

// Tolerance returns the allowed absolute deviation for a reference amount.
func (r Rules) Tolerance(amount int64) int64 {
	if amount < 0 {
		amount = -amount
	}
	rel := int64(math.Round(float64(amount) * float64(r.ToleranceBps) / 10_000))
	if rel > r.ToleranceAbsMinor {
		return rel
	}
	return r.ToleranceAbsMinor
}

// WithinTolerance reports whether b is within tolerance of a.
func (r Rules) WithinTolerance(a, b int64) bool {
	d := a - b
	if d < 0 {
		d = -d
	}
	return d <= r.Tolerance(a)
}

// WithinDateWindow reports whether two value dates are at most N days apart.
func (r Rules) WithinDateWindow(a, b Date) bool {
	return DaysBetween(a, b) <= r.ValueDateWindowDays
}

// tolerantConfidence maps a residual to a confidence in [0.80, 0.95]: a zero
// residual scores 0.95, a residual at the edge of tolerance scores 0.80.
func tolerantConfidence(residual, tolerance int64) float64 {
	if residual < 0 {
		residual = -residual
	}
	if tolerance <= 0 {
		return 0.95
	}
	f := float64(residual) / float64(tolerance)
	if f > 1 {
		f = 1
	}
	return math.Round((0.95-0.15*f)*1000) / 1000
}

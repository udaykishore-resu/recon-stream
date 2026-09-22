package recon

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"
	"time"
)

// Tier is the deterministic matching tier that produced a Match.
type Tier int

const (
	TierExact     Tier = 1 // T1: ref + amount + currency exact, value date in window
	TierTolerant  Tier = 2 // T2: amount within tolerance, value date in window
	TierManyToOne Tier = 3 // T3: one settlement leg vs N legs summing within tolerance
)

// Match is a set of legs the engine considers to be the same economic event.
type Match struct {
	ID            string    `json:"id"`
	LegIDs        []string  `json:"leg_ids"`
	Tier          Tier      `json:"tier"`
	RuleID        string    `json:"rule_id"` // rule@version, e.g. "T1.exact@v1"
	Confidence    float64   `json:"confidence"`
	ResidualMinor int64     `json:"residual_minor"` // signed: sum(other side) - anchor
	Currency      string    `json:"currency"`
	MatchedAt     time.Time `json:"matched_at"`
}

// Category is the classifier-assigned reason for a Break.
type Category string

const (
	CatFee                 Category = "fee"
	CatFXDrift             Category = "fx_drift"
	CatTiming              Category = "timing"
	CatDuplicate           Category = "duplicate"
	CatMissingCounterparty Category = "missing_counterparty"
	CatUnclassified        Category = "unclassified"
)

// BreakStatus is the workflow state of a Break.
type BreakStatus string

const (
	BreakOpen     BreakStatus = "open"
	BreakResolved BreakStatus = "resolved"
)

// Break is one or more legs the engine could not match, with a classification.
type Break struct {
	ID           string      `json:"id"`
	LegIDs       []string    `json:"leg_ids"`
	Currency     string      `json:"currency"`
	Counterparty string      `json:"counterparty"`
	Category     Category    `json:"category"`
	Confidence   float64     `json:"confidence"`
	ClassifierID string      `json:"classifier_id"` // classifier@version
	Trigger      string      `json:"trigger"`       // ref_conflict | duplicate | window_expired
	Status       BreakStatus `json:"status"`
	OpenedAt     time.Time   `json:"opened_at"`
	ResolvedAt   *time.Time  `json:"resolved_at,omitempty"`
	Reason       string      `json:"reason,omitempty"`
	Actor        string      `json:"actor,omitempty"`
	AgeSeconds   int64       `json:"age_seconds"` // computed at read time
}

// Age returns how long the break has been (or was) open.
func (b Break) Age(now time.Time) time.Duration {
	end := now
	if b.ResolvedAt != nil {
		end = *b.ResolvedAt
	}
	return end.Sub(b.OpenedAt)
}

// DeriveID builds a deterministic identifier from a prefix and leg IDs so
// that replaying the same event log produces the same matches and breaks.
func DeriveID(prefix string, legIDs []string) string {
	ids := append([]string(nil), legIDs...)
	sort.Strings(ids)
	sum := sha256.Sum256([]byte(strings.Join(ids, "\x00")))
	return prefix + "_" + hex.EncodeToString(sum[:])[:20]
}

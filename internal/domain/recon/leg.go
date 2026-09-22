// Package recon holds the pure reconciliation domain: legs, matches, breaks,
// the versioned matching rules and the windowed matching engine. It performs
// no I/O; persistence is delegated to a ports.Store implementation.
package recon

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Direction is the accounting direction of a leg as seen by its source.
type Direction string

const (
	Debit  Direction = "debit"
	Credit Direction = "credit"
)

// LegStatus is the lifecycle state of a leg inside the engine.
type LegStatus string

const (
	LegOpen    LegStatus = "open"    // waiting in the window for a counterparty leg
	LegMatched LegStatus = "matched" // part of a Match
	LegBroken  LegStatus = "break"   // part of a Break
)

// Date is a calendar date (UTC midnight) serialised as YYYY-MM-DD.
type Date struct{ time.Time }

// NewDate builds a Date from y/m/d.
func NewDate(y int, m time.Month, d int) Date {
	return Date{time.Date(y, m, d, 0, 0, 0, 0, time.UTC)}
}

// ParseDate parses YYYY-MM-DD.
func ParseDate(s string) (Date, error) {
	t, err := time.Parse("2006-01-02", s)
	if err != nil {
		return Date{}, fmt.Errorf("parse value_date %q: %w", s, err)
	}
	return Date{t.UTC()}, nil
}

// String formats the date as YYYY-MM-DD.
func (d Date) String() string { return d.Time.Format("2006-01-02") }

// MarshalJSON implements json.Marshaler.
func (d Date) MarshalJSON() ([]byte, error) { return json.Marshal(d.String()) }

// UnmarshalJSON implements json.Unmarshaler.
func (d *Date) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("value_date must be a string: %w", err)
	}
	p, err := ParseDate(s)
	if err != nil {
		return err
	}
	*d = p
	return nil
}

// DaysBetween returns the absolute number of calendar days between two dates.
func DaysBetween(a, b Date) int {
	diff := a.Time.Sub(b.Time).Hours() / 24
	if diff < 0 {
		diff = -diff
	}
	return int(diff + 0.5)
}

// Leg is a single money movement observed by one source system (a ledger,
// a payment rail, a card-network settlement file, ...). Amounts are in minor
// units (cents, pence) and always positive; Direction carries the sign.
type Leg struct {
	// ID is derived: source:txn_ref:hash12. It is the idempotency key.
	ID           string            `json:"id,omitempty"`
	Source       string            `json:"source"`
	TxnRef       string            `json:"txn_ref"`
	AmountMinor  int64             `json:"amount_minor"`
	Currency     string            `json:"currency"`
	ValueDate    Date              `json:"value_date"`
	Direction    Direction         `json:"direction"`
	Counterparty string            `json:"counterparty"`
	Attrs        map[string]string `json:"attrs,omitempty"`
	IngestedAt   time.Time         `json:"ingested_at,omitzero"`
	Status       LegStatus         `json:"status,omitempty"`
	MatchID      string            `json:"match_id,omitempty"`
	BreakID      string            `json:"break_id,omitempty"`
}

// ErrInvalidLeg wraps all leg validation failures.
var ErrInvalidLeg = errors.New("invalid leg")

// Validate checks required fields and normalises currency/direction.
func (l *Leg) Validate() error {
	var problems []string
	if strings.TrimSpace(l.Source) == "" {
		problems = append(problems, "source is required")
	}
	if strings.TrimSpace(l.TxnRef) == "" {
		problems = append(problems, "txn_ref is required")
	}
	if l.AmountMinor <= 0 {
		problems = append(problems, "amount_minor must be > 0")
	}
	l.Currency = strings.ToUpper(strings.TrimSpace(l.Currency))
	if len(l.Currency) != 3 || strings.Trim(l.Currency, "ABCDEFGHIJKLMNOPQRSTUVWXYZ") != "" {
		problems = append(problems, "currency must be a 3-letter ISO 4217 code")
	}
	if l.ValueDate.IsZero() {
		problems = append(problems, "value_date is required")
	}
	l.Direction = Direction(strings.ToLower(string(l.Direction)))
	if l.Direction != Debit && l.Direction != Credit {
		problems = append(problems, "direction must be debit or credit")
	}
	if strings.TrimSpace(l.Counterparty) == "" {
		problems = append(problems, "counterparty is required")
	}
	if len(problems) > 0 {
		return fmt.Errorf("%w: %s", ErrInvalidLeg, strings.Join(problems, "; "))
	}
	return nil
}

// ContentHash hashes every business field of the leg (not ingestion metadata),
// so a re-delivered identical record yields the same ID while a record with the
// same txn_ref but different content does not.
func (l Leg) ContentHash() string {
	h := sha256.New()
	w := func(s string) { h.Write([]byte(s)); h.Write([]byte{0}) }
	w(l.Source)
	w(l.TxnRef)
	w(strconv.FormatInt(l.AmountMinor, 10))
	w(l.Currency)
	w(l.ValueDate.String())
	w(string(l.Direction))
	w(l.Counterparty)
	keys := make([]string, 0, len(l.Attrs))
	for k := range l.Attrs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		w(k)
		w(l.Attrs[k])
	}
	return hex.EncodeToString(h.Sum(nil))
}

// AssignID sets the derived idempotency key. Idempotent.
func (l *Leg) AssignID() {
	l.ID = fmt.Sprintf("%s:%s:%s", l.Source, l.TxnRef, l.ContentHash()[:12])
}

// WindowKey is the partition key of the open-leg index: (currency, counterparty).
type WindowKey struct {
	Currency     string
	Counterparty string
}

// Key returns the leg's window key.
func (l Leg) Key() WindowKey { return WindowKey{Currency: l.Currency, Counterparty: l.Counterparty} }

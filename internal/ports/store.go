// Package ports declares the interfaces through which the domain reaches
// external systems. Adapters live in internal/adapters.
package ports

import (
	"context"
	"errors"
	"time"

	"github.com/udaykishore-resu/recon-stream/internal/domain/recon"
)

// ErrNotFound is returned by lookups that find nothing.
var ErrNotFound = errors.New("not found")

// BreakFilter narrows ListBreaks.
type BreakFilter struct {
	Status       recon.BreakStatus
	Category     recon.Category
	Currency     string
	Counterparty string
	MinAge       time.Duration // only breaks open at least this long
	Limit        int
	Offset       int
}

// Stats is the aggregate view served by GET /v1/stats.
type Stats struct {
	LegsTotal       int64            `json:"legs_total"`
	LegsOpen        int64            `json:"legs_open"`
	LegsMatched     int64            `json:"legs_matched"`
	LegsInBreak     int64            `json:"legs_in_break"`
	MatchesTotal    int64            `json:"matches_total"`
	MatchesByTier   map[string]int64 `json:"matches_by_tier"`
	MatchesByRule   map[string]int64 `json:"matches_by_rule"`
	BreaksOpen      int64            `json:"breaks_open"`
	BreaksResolved  int64            `json:"breaks_resolved"`
	BreaksByCat     map[string]int64 `json:"breaks_open_by_category"`
	EventsTotal     int64            `json:"events_total"`
	AutoMatchRate   float64          `json:"auto_match_rate"` // matched / (matched + in_break)
	LegsBySource    map[string]int64 `json:"legs_by_source"`
	OldestOpenBreak *time.Time       `json:"oldest_open_break,omitempty"`
}

// Store is the durable projection + event log. Apply is the only write path
// used by the engine and must be atomic.
type Store interface {
	// Apply persists a change set atomically and assigns event sequence numbers.
	Apply(ctx context.Context, cs recon.ChangeSet) error

	HasLeg(ctx context.Context, id string) (bool, error)
	GetLeg(ctx context.Context, id string) (recon.Leg, error)
	// LegsByTxnRef returns every persisted leg with this txn_ref, any status.
	LegsByTxnRef(ctx context.Context, txnRef string) ([]recon.Leg, error)
	// OpenLegs returns legs with status open, used to rebuild the index on start.
	OpenLegs(ctx context.Context) ([]recon.Leg, error)

	GetMatch(ctx context.Context, id string) (recon.Match, error)
	GetBreak(ctx context.Context, id string) (recon.Break, error)
	ListBreaks(ctx context.Context, f BreakFilter) ([]recon.Break, error)

	// EventsFrom streams events with seq >= from, ascending, at most limit.
	EventsFrom(ctx context.Context, from int64, limit int) ([]recon.Event, error)
	// LastSeq returns the highest event sequence number (0 when empty).
	LastSeq(ctx context.Context) (int64, error)
	// ResetProjections clears legs/matches/breaks but keeps recon_events.
	ResetProjections(ctx context.Context) error

	Stats(ctx context.Context) (Stats, error)
	Ping(ctx context.Context) error
	Close() error
}

// LegHandler processes one batch of legs. Returning an error must prevent the
// source from acknowledging (committing) the batch.
type LegHandler func(ctx context.Context, legs []recon.Leg) error

// LegSource is a stream of incoming legs (Kafka topics, in-memory channel).
type LegSource interface {
	// Run blocks, delivering batches to h until ctx is cancelled.
	Run(ctx context.Context, h LegHandler) error
	Close() error
}

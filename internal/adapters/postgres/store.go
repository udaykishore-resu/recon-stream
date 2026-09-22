// Package postgres implements ports.Store on PostgreSQL with pgx. Every
// ChangeSet is applied in one transaction, so a Kafka offset is only
// committed once the legs, matches, breaks and events of that batch are
// durable.
package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/udaykishore-resu/recon-stream/internal/domain/recon"
	"github.com/udaykishore-resu/recon-stream/internal/ports"
	"github.com/udaykishore-resu/recon-stream/migrations"
)

// Store is a ports.Store backed by a pgx connection pool.
type Store struct {
	pool *pgxpool.Pool
}

var _ ports.Store = (*Store)(nil)

// Open connects, pings and applies pending migrations.
func Open(ctx context.Context, dsn string) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse dsn: %w", err)
	}
	cfg.MaxConns = 16
	cfg.MaxConnLifetime = 30 * time.Minute
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	s := &Store{pool: pool}
	if err := s.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	if err := s.migrate(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) migrate(ctx context.Context) error {
	ms, err := migrations.All()
	if err != nil {
		return err
	}
	// The first migration creates schema_migrations, so it must always be safe to re-run (IF NOT EXISTS).
	for _, m := range ms {
		err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
			if m.Version != ms[0].Version {
				var applied bool
				if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version=$1)`, m.Version).Scan(&applied); err != nil {
					return err
				}
				if applied {
					return nil
				}
			}
			if _, err := tx.Exec(ctx, m.SQL); err != nil {
				return fmt.Errorf("apply %s: %w", m.Version, err)
			}
			_, err := tx.Exec(ctx, `INSERT INTO schema_migrations(version) VALUES($1) ON CONFLICT DO NOTHING`, m.Version)
			return err
		})
		if err != nil {
			return fmt.Errorf("migrate: %w", err)
		}
	}
	return nil
}

// Apply implements ports.Store.
func (s *Store) Apply(ctx context.Context, cs recon.ChangeSet) error {
	if cs.Empty() {
		return nil
	}
	return pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		b := &pgx.Batch{}
		for _, l := range cs.Legs {
			attrs, err := json.Marshal(nonNil(l.Attrs))
			if err != nil {
				return fmt.Errorf("marshal attrs: %w", err)
			}
			b.Queue(`INSERT INTO legs (id, source, txn_ref, amount_minor, currency, value_date, direction, counterparty, attrs, ingested_at, status, match_id, break_id)
			         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,NULLIF($12,''),NULLIF($13,''))
			         ON CONFLICT (id) DO UPDATE SET status=EXCLUDED.status, match_id=EXCLUDED.match_id, break_id=EXCLUDED.break_id`,
				l.ID, l.Source, l.TxnRef, l.AmountMinor, l.Currency, l.ValueDate.Time, string(l.Direction), l.Counterparty, attrs, l.IngestedAt, string(l.Status), l.MatchID, l.BreakID)
		}
		for _, m := range cs.Matches {
			b.Queue(`INSERT INTO matches (id, leg_ids, tier, rule_id, confidence, residual_minor, currency, matched_at)
			         VALUES ($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT (id) DO NOTHING`,
				m.ID, m.LegIDs, int16(m.Tier), m.RuleID, m.Confidence, m.ResidualMinor, m.Currency, m.MatchedAt)
		}
		for _, br := range cs.Breaks {
			b.Queue(`INSERT INTO breaks (id, leg_ids, currency, counterparty, category, confidence, classifier_id, trigger, status, opened_at, resolved_at, reason, actor)
			         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
			         ON CONFLICT (id) DO UPDATE SET status=EXCLUDED.status, resolved_at=EXCLUDED.resolved_at, reason=EXCLUDED.reason, actor=EXCLUDED.actor`,
				br.ID, br.LegIDs, br.Currency, br.Counterparty, string(br.Category), br.Confidence, br.ClassifierID, br.Trigger, string(br.Status), br.OpenedAt, br.ResolvedAt, br.Reason, br.Actor)
		}
		for _, ev := range cs.Events {
			b.Queue(`INSERT INTO recon_events (type, at, aggregate_id, payload) VALUES ($1,$2,$3,$4)`,
				ev.Type, ev.At, ev.AggregateID, []byte(ev.Payload))
		}
		res := tx.SendBatch(ctx, b)
		defer res.Close()
		for i := 0; i < b.Len(); i++ {
			if _, err := res.Exec(); err != nil {
				return fmt.Errorf("apply change set (stmt %d): %w", i, err)
			}
		}
		return nil
	})
}

const legCols = `id, source, txn_ref, amount_minor, currency, value_date, direction, counterparty, attrs, ingested_at, status, COALESCE(match_id,''), COALESCE(break_id,'')`

func scanLeg(row pgx.Row) (recon.Leg, error) {
	var l recon.Leg
	var attrs []byte
	var vd time.Time
	var dir, status string
	if err := row.Scan(&l.ID, &l.Source, &l.TxnRef, &l.AmountMinor, &l.Currency, &vd, &dir, &l.Counterparty, &attrs, &l.IngestedAt, &status, &l.MatchID, &l.BreakID); err != nil {
		return recon.Leg{}, err
	}
	l.ValueDate = recon.Date{Time: vd.UTC()}
	l.Direction, l.Status = recon.Direction(dir), recon.LegStatus(status)
	l.Currency = strings.TrimSpace(l.Currency)
	if len(attrs) > 0 {
		if err := json.Unmarshal(attrs, &l.Attrs); err != nil {
			return recon.Leg{}, fmt.Errorf("decode attrs: %w", err)
		}
	}
	return l, nil
}

func (s *Store) queryLegs(ctx context.Context, sql string, args ...any) ([]recon.Leg, error) {
	rows, err := s.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []recon.Leg
	for rows.Next() {
		l, err := scanLeg(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, l)
	}
	return out, rows.Err()
}

// HasLeg implements ports.Store.
func (s *Store) HasLeg(ctx context.Context, id string) (bool, error) {
	var ok bool
	err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM legs WHERE id=$1)`, id).Scan(&ok)
	return ok, err
}

// GetLeg implements ports.Store.
func (s *Store) GetLeg(ctx context.Context, id string) (recon.Leg, error) {
	l, err := scanLeg(s.pool.QueryRow(ctx, `SELECT `+legCols+` FROM legs WHERE id=$1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return recon.Leg{}, ports.ErrNotFound
	}
	return l, err
}

// LegsByTxnRef implements ports.Store.
func (s *Store) LegsByTxnRef(ctx context.Context, txnRef string) ([]recon.Leg, error) {
	return s.queryLegs(ctx, `SELECT `+legCols+` FROM legs WHERE txn_ref=$1 ORDER BY ingested_at, id`, txnRef)
}

// OpenLegs implements ports.Store.
func (s *Store) OpenLegs(ctx context.Context) ([]recon.Leg, error) {
	return s.queryLegs(ctx, `SELECT `+legCols+` FROM legs WHERE status='open' ORDER BY ingested_at, id`)
}

// GetMatch implements ports.Store.
func (s *Store) GetMatch(ctx context.Context, id string) (recon.Match, error) {
	var m recon.Match
	var tier int16
	err := s.pool.QueryRow(ctx, `SELECT id, leg_ids, tier, rule_id, confidence, residual_minor, currency, matched_at FROM matches WHERE id=$1`, id).
		Scan(&m.ID, &m.LegIDs, &tier, &m.RuleID, &m.Confidence, &m.ResidualMinor, &m.Currency, &m.MatchedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return recon.Match{}, ports.ErrNotFound
	}
	m.Tier = recon.Tier(tier)
	m.Currency = strings.TrimSpace(m.Currency)
	return m, err
}

const breakCols = `id, leg_ids, currency, counterparty, category, confidence, classifier_id, trigger, status, opened_at, resolved_at, reason, actor`

func scanBreak(row pgx.Row) (recon.Break, error) {
	var b recon.Break
	var cat, status string
	if err := row.Scan(&b.ID, &b.LegIDs, &b.Currency, &b.Counterparty, &cat, &b.Confidence, &b.ClassifierID, &b.Trigger, &status, &b.OpenedAt, &b.ResolvedAt, &b.Reason, &b.Actor); err != nil {
		return recon.Break{}, err
	}
	b.Category, b.Status = recon.Category(cat), recon.BreakStatus(status)
	b.Currency = strings.TrimSpace(b.Currency)
	return b, nil
}

// GetBreak implements ports.Store.
func (s *Store) GetBreak(ctx context.Context, id string) (recon.Break, error) {
	b, err := scanBreak(s.pool.QueryRow(ctx, `SELECT `+breakCols+` FROM breaks WHERE id=$1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return recon.Break{}, ports.ErrNotFound
	}
	return b, err
}

// ListBreaks implements ports.Store.
func (s *Store) ListBreaks(ctx context.Context, f ports.BreakFilter) ([]recon.Break, error) {
	var where []string
	var args []any
	add := func(clause string, v any) {
		args = append(args, v)
		where = append(where, fmt.Sprintf(clause, len(args)))
	}
	if f.Status != "" {
		add("status=$%d", string(f.Status))
	}
	if f.Category != "" {
		add("category=$%d", string(f.Category))
	}
	if f.Currency != "" {
		add("currency=$%d", f.Currency)
	}
	if f.Counterparty != "" {
		add("counterparty=$%d", f.Counterparty)
	}
	if f.MinAge > 0 {
		add("COALESCE(resolved_at, now()) - opened_at >= $%d", f.MinAge)
	}
	sql := `SELECT ` + breakCols + ` FROM breaks`
	if len(where) > 0 {
		sql += " WHERE " + strings.Join(where, " AND ")
	}
	sql += " ORDER BY opened_at, id"
	if f.Limit > 0 {
		args = append(args, f.Limit)
		sql += fmt.Sprintf(" LIMIT $%d", len(args))
	}
	if f.Offset > 0 {
		args = append(args, f.Offset)
		sql += fmt.Sprintf(" OFFSET $%d", len(args))
	}
	rows, err := s.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []recon.Break{}
	for rows.Next() {
		b, err := scanBreak(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

// EventsFrom implements ports.Store.
func (s *Store) EventsFrom(ctx context.Context, from int64, limit int) ([]recon.Event, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, `SELECT seq, type, at, aggregate_id, payload FROM recon_events WHERE seq >= $1 ORDER BY seq LIMIT $2`, from, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []recon.Event{}
	for rows.Next() {
		var ev recon.Event
		var payload []byte
		if err := rows.Scan(&ev.Seq, &ev.Type, &ev.At, &ev.AggregateID, &payload); err != nil {
			return nil, err
		}
		ev.Payload = json.RawMessage(payload)
		out = append(out, ev)
	}
	return out, rows.Err()
}

// LastSeq implements ports.Store.
func (s *Store) LastSeq(ctx context.Context) (int64, error) {
	var seq int64
	err := s.pool.QueryRow(ctx, `SELECT COALESCE(MAX(seq),0) FROM recon_events`).Scan(&seq)
	return seq, err
}

// ResetProjections implements ports.Store (recon_events is untouched).
func (s *Store) ResetProjections(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, `TRUNCATE legs, matches, breaks`)
	return err
}

// Stats implements ports.Store.
func (s *Store) Stats(ctx context.Context) (ports.Stats, error) {
	st := ports.Stats{MatchesByTier: map[string]int64{}, MatchesByRule: map[string]int64{}, BreaksByCat: map[string]int64{}, LegsBySource: map[string]int64{}}
	if err := s.pool.QueryRow(ctx, `SELECT count(*), count(*) FILTER (WHERE status='open'), count(*) FILTER (WHERE status='matched'), count(*) FILTER (WHERE status='break') FROM legs`).
		Scan(&st.LegsTotal, &st.LegsOpen, &st.LegsMatched, &st.LegsInBreak); err != nil {
		return st, err
	}
	if err := groupCount(ctx, s.pool, `SELECT source, count(*) FROM legs GROUP BY source`, st.LegsBySource); err != nil {
		return st, err
	}
	if err := groupCount(ctx, s.pool, `SELECT 'T'||tier::text, count(*) FROM matches GROUP BY tier`, st.MatchesByTier); err != nil {
		return st, err
	}
	if err := groupCount(ctx, s.pool, `SELECT rule_id, count(*) FROM matches GROUP BY rule_id`, st.MatchesByRule); err != nil {
		return st, err
	}
	for _, v := range st.MatchesByTier {
		st.MatchesTotal += v
	}
	if err := groupCount(ctx, s.pool, `SELECT category, count(*) FROM breaks WHERE status='open' GROUP BY category`, st.BreaksByCat); err != nil {
		return st, err
	}
	var oldest *time.Time
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE status='open'), count(*) FILTER (WHERE status='resolved'), min(opened_at) FILTER (WHERE status='open') FROM breaks`).
		Scan(&st.BreaksOpen, &st.BreaksResolved, &oldest); err != nil {
		return st, err
	}
	st.OldestOpenBreak = oldest
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM recon_events`).Scan(&st.EventsTotal); err != nil {
		return st, err
	}
	if den := st.LegsMatched + st.LegsInBreak; den > 0 {
		st.AutoMatchRate = float64(st.LegsMatched) / float64(den)
	}
	return st, nil
}

func groupCount(ctx context.Context, q interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}, sql string, into map[string]int64) error {
	rows, err := q.Query(ctx, sql)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var k string
		var n int64
		if err := rows.Scan(&k, &n); err != nil {
			return err
		}
		into[k] = n
	}
	return rows.Err()
}

// Ping implements ports.Store.
func (s *Store) Ping(ctx context.Context) error {
	if err := s.pool.Ping(ctx); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) {
			return fmt.Errorf("postgres: %s (%s)", pgErr.Message, pgErr.Code)
		}
		return fmt.Errorf("postgres: %w", err)
	}
	return nil
}

// Close implements ports.Store.
func (s *Store) Close() error {
	s.pool.Close()
	return nil
}

func nonNil(m map[string]string) map[string]string {
	if m == nil {
		return map[string]string{}
	}
	return m
}

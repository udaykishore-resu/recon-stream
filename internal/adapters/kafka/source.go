// Package kafka consumes leg records from Kafka with franz-go. Offsets are
// committed manually and only after the engine has persisted every leg of
// the polled batch, giving at-least-once delivery; the engine's content-hash
// idempotency key turns redeliveries into no-ops.
package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/udaykishore-resu/recon-stream/internal/domain/recon"
	"github.com/udaykishore-resu/recon-stream/internal/ports"
)

// Config for the consumer.
type Config struct {
	Brokers []string
	Group   string
	Topics  []string
	// MaxPollRecords bounds a poll (and therefore one engine batch).
	MaxPollRecords int
}

// Source is a ports.LegSource over a Kafka consumer group.
type Source struct {
	cl       *kgo.Client
	log      *slog.Logger
	onCommit func(ok bool)
	// sourceOf maps a topic to the leg source name when the record omits it.
	sourceOf map[string]string
}

var _ ports.LegSource = (*Source)(nil)

// New dials the cluster. Consumption starts with Run.
func New(cfg Config, log *slog.Logger, onCommit func(ok bool)) (*Source, error) {
	if len(cfg.Brokers) == 0 || len(cfg.Topics) == 0 || cfg.Group == "" {
		return nil, errors.New("kafka: brokers, topics and group are required")
	}
	if cfg.MaxPollRecords <= 0 {
		cfg.MaxPollRecords = 1000
	}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeTopics(cfg.Topics...),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.FetchMaxWait(500*time.Millisecond),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka client: %w", err)
	}
	if onCommit == nil {
		onCommit = func(bool) {}
	}
	sourceOf := make(map[string]string, len(cfg.Topics))
	for _, t := range cfg.Topics {
		sourceOf[t] = topicSource(t)
	}
	return &Source{cl: cl, log: log, onCommit: onCommit, sourceOf: sourceOf}, nil
}

// topicSource derives a default source name from a topic like "ledger.legs".
func topicSource(topic string) string {
	for i, c := range topic {
		if c == '.' {
			return topic[:i]
		}
	}
	return topic
}

// Ping checks broker connectivity (readiness).
func (s *Source) Ping(ctx context.Context) error {
	return s.cl.Ping(ctx)
}

// Run implements ports.LegSource.
func (s *Source) Run(ctx context.Context, h ports.LegHandler) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		fetches := s.cl.PollRecords(ctx, 1000)
		if fetches.IsClientClosed() {
			return errors.New("kafka client closed")
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		fetches.EachError(func(topic string, partition int32, err error) {
			s.log.ErrorContext(ctx, "kafka fetch error", "topic", topic, "partition", partition, "error", err)
		})
		var legs []recon.Leg
		var bad int
		fetches.EachRecord(func(r *kgo.Record) {
			l, err := decode(r, s.sourceOf[r.Topic])
			if err != nil {
				bad++
				s.log.WarnContext(ctx, "skipping undecodable record", "topic", r.Topic, "partition", r.Partition, "offset", r.Offset, "error", err)
				return
			}
			legs = append(legs, l)
		})
		if len(legs) == 0 && bad == 0 {
			s.cl.AllowRebalance()
			continue
		}
		if len(legs) > 0 {
			if err := h(ctx, legs); err != nil {
				// Do not commit; the batch will be redelivered after restart/rebalance.
				s.log.ErrorContext(ctx, "batch failed, offsets not committed", "legs", len(legs), "error", err)
				s.cl.AllowRebalance()
				if ctx.Err() != nil {
					return ctx.Err()
				}
				time.Sleep(time.Second) // crude back-off; the record stays uncommitted
				continue
			}
		}
		if err := s.cl.CommitUncommittedOffsets(ctx); err != nil {
			s.onCommit(false)
			s.log.ErrorContext(ctx, "offset commit failed", "error", err)
		} else {
			s.onCommit(true)
		}
		s.cl.AllowRebalance()
	}
}

// Close implements ports.LegSource.
func (s *Source) Close() error {
	s.cl.Close()
	return nil
}

// decode parses a record value as a Leg; the topic supplies a default source
// and the record key a default txn_ref.
func decode(r *kgo.Record, defaultSource string) (recon.Leg, error) {
	var l recon.Leg
	if err := json.Unmarshal(r.Value, &l); err != nil {
		return recon.Leg{}, fmt.Errorf("decode leg: %w", err)
	}
	if l.Source == "" {
		l.Source = defaultSource
	}
	if l.TxnRef == "" && len(r.Key) > 0 {
		l.TxnRef = string(r.Key)
	}
	if l.IngestedAt.IsZero() && !r.Timestamp.IsZero() {
		l.IngestedAt = r.Timestamp.UTC()
	}
	if err := l.Validate(); err != nil {
		return recon.Leg{}, err
	}
	return l, nil
}

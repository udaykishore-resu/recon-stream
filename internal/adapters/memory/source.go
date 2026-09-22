package memory

import (
	"context"
	"errors"

	"github.com/udaykishore-resu/recon-stream/internal/domain/recon"
	"github.com/udaykishore-resu/recon-stream/internal/ports"
)

// Source is a channel-backed ports.LegSource used in tests and `make run`.
type Source struct {
	ch chan []recon.Leg
}

var _ ports.LegSource = (*Source)(nil)

// NewSource creates a source with the given buffer.
func NewSource(buffer int) *Source {
	return &Source{ch: make(chan []recon.Leg, buffer)}
}

// Publish enqueues a batch; it blocks when the buffer is full.
func (s *Source) Publish(ctx context.Context, legs []recon.Leg) error {
	select {
	case s.ch <- legs:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Run implements ports.LegSource. A handler error is returned to the caller
// (there is no redelivery in memory; Kafka provides it in production).
func (s *Source) Run(ctx context.Context, h ports.LegHandler) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case batch, ok := <-s.ch:
			if !ok {
				return errors.New("memory source closed")
			}
			if err := h(ctx, batch); err != nil {
				return err
			}
		}
	}
}

// Close implements ports.LegSource.
func (s *Source) Close() error {
	close(s.ch)
	return nil
}

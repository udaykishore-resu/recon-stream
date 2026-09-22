// Command recon-stream runs the streaming reconciliation engine: config →
// adapters → domain engine → HTTP API (+ optional Kafka consumer) → run until
// SIGTERM, then drain.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.opentelemetry.io/otel"
	"golang.org/x/sync/errgroup"

	kafkaadapter "github.com/udaykishore-resu/recon-stream/internal/adapters/kafka"
	"github.com/udaykishore-resu/recon-stream/internal/adapters/memory"
	"github.com/udaykishore-resu/recon-stream/internal/adapters/postgres"
	httpapi "github.com/udaykishore-resu/recon-stream/internal/api/http"
	"github.com/udaykishore-resu/recon-stream/internal/config"
	"github.com/udaykishore-resu/recon-stream/internal/domain/classify"
	"github.com/udaykishore-resu/recon-stream/internal/domain/recon"
	"github.com/udaykishore-resu/recon-stream/internal/observability"
	"github.com/udaykishore-resu/recon-stream/internal/ports"
)

// version is set at build time via -ldflags "-X main.version=...".
var version = "dev"

func main() {
	if err := run(); err != nil {
		slog.Error("fatal", "error", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load(nil)
	if err != nil {
		return err
	}
	log := observability.NewLogger(cfg.LogLevel, os.Stdout)
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	_, shutdownTracing, err := observability.SetupTracing(ctx, cfg.ServiceName, cfg.OTelEndpoint, cfg.OTelInsecure)
	if err != nil {
		return err
	}
	defer func() {
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = shutdownTracing(c)
	}()
	metrics := observability.NewMetrics()

	// --- store ---------------------------------------------------------------
	var store ports.Store
	switch cfg.Store {
	case config.StorePostgres:
		pg, err := postgres.Open(ctx, cfg.PostgresDSN)
		if err != nil {
			return fmt.Errorf("postgres: %w", err)
		}
		store = pg
	default:
		store = memory.NewStore()
	}
	defer func() { _ = store.Close() }()

	// --- engine --------------------------------------------------------------
	eng, err := recon.NewEngine(store, classify.NewHeuristic(), cfg.Rules, cfg.WindowTTL, metrics.Hooks())
	if err != nil {
		return err
	}
	if err := eng.Rebuild(ctx); err != nil {
		return err
	}
	log.Info("engine ready", "store", cfg.Store, "open_legs", eng.OpenLegs(), "window_ttl", cfg.WindowTTL.String(),
		"tolerance_bps", cfg.Rules.ToleranceBps, "tolerance_abs_minor", cfg.Rules.ToleranceAbsMinor,
		"value_date_window_days", cfg.Rules.ValueDateWindowDays, "version", version)

	// --- source --------------------------------------------------------------
	var source ports.LegSource
	var ready func(context.Context) error
	if cfg.KafkaEnabled {
		ks, err := kafkaadapter.New(kafkaadapter.Config{Brokers: cfg.KafkaBrokers, Group: cfg.KafkaGroup, Topics: cfg.KafkaTopics}, log, func(ok bool) {
			if ok {
				metrics.KafkaCommits.WithLabelValues("ok").Inc()
			} else {
				metrics.KafkaCommits.WithLabelValues("error").Inc()
			}
		})
		if err != nil {
			return err
		}
		source, ready = ks, ks.Ping
		log.Info("kafka consumer configured", "brokers", cfg.KafkaBrokers, "group", cfg.KafkaGroup, "topics", cfg.KafkaTopics)
	}

	// --- http ----------------------------------------------------------------
	api := httpapi.New(httpapi.Deps{
		Engine: eng, Store: store, Logger: log, Metrics: metrics,
		Tracer: otel.Tracer("recon-stream/http"), Ready: ready, Version: version,
	})
	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           api.Handler(),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       60 * time.Second,
		WriteTimeout:      60 * time.Second,
		IdleTimeout:       120 * time.Second,
		BaseContext:       func(net.Listener) context.Context { return ctx },
	}

	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		log.Info("http listening", "addr", cfg.HTTPAddr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("http: %w", err)
		}
		return nil
	})
	g.Go(func() error {
		<-gctx.Done()
		drain, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()
		log.Info("shutting down http", "timeout", cfg.ShutdownTimeout.String())
		return srv.Shutdown(drain)
	})
	g.Go(func() error { return sweeper(gctx, eng, cfg.SweepInterval, log) })
	if source != nil {
		g.Go(func() error {
			defer func() { _ = source.Close() }()
			err := source.Run(gctx, func(ctx context.Context, legs []recon.Leg) error {
				start := time.Now()
				res, err := eng.Ingest(ctx, legs)
				metrics.IngestDuration.Observe(time.Since(start).Seconds())
				if err != nil {
					return err
				}
				log.Debug("kafka batch processed", "legs", len(legs), "matched", res.Matched, "breaks", res.Breaks, "open", res.Open, "duplicates", res.Duplicates)
				return nil
			})
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		})
	}

	err = g.Wait()
	if errors.Is(err, context.Canceled) {
		err = nil
	}
	log.Info("stopped", "error", err)
	return err
}

// sweeper expires stale open legs into breaks on a fixed cadence.
func sweeper(ctx context.Context, eng *recon.Engine, every time.Duration, log *slog.Logger) error {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			n, err := eng.Sweep(ctx)
			if err != nil && !errors.Is(err, context.Canceled) {
				log.Error("sweep failed", "error", err)
				continue
			}
			if n > 0 {
				log.Info("window sweep expired legs into breaks", "expired", n)
			}
		}
	}
}

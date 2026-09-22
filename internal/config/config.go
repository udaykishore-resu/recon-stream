// Package config loads 12-factor configuration from environment variables
// with validation and defaults that make `make run` work with zero infra.
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/udaykishore-resu/recon-stream/internal/domain/recon"
)

// Store backends.
const (
	StoreMemory   = "memory"
	StorePostgres = "postgres"
)

// Config is the fully resolved service configuration.
type Config struct {
	HTTPAddr        string
	ShutdownTimeout time.Duration
	LogLevel        string
	ServiceName     string

	Store       string
	PostgresDSN string

	KafkaEnabled bool
	KafkaBrokers []string
	KafkaGroup   string
	KafkaTopics  []string

	WindowTTL     time.Duration
	SweepInterval time.Duration
	Rules         recon.Rules

	OTelEndpoint string // OTLP/HTTP traces endpoint, empty disables export
	OTelInsecure bool
}

// Load reads the environment. lookup defaults to os.LookupEnv.
func Load(lookup func(string) (string, bool)) (Config, error) {
	if lookup == nil {
		lookup = os.LookupEnv
	}
	var errs []error
	env := envReader{lookup: lookup, errs: &errs}

	cfg := Config{
		HTTPAddr:        env.str("RECON_HTTP_ADDR", ":8080"),
		ShutdownTimeout: env.dur("RECON_SHUTDOWN_TIMEOUT", 20*time.Second),
		LogLevel:        strings.ToLower(env.str("RECON_LOG_LEVEL", "info")),
		ServiceName:     env.str("RECON_SERVICE_NAME", "recon-stream"),
		Store:           strings.ToLower(env.str("RECON_STORE", StoreMemory)),
		PostgresDSN:     env.str("RECON_POSTGRES_DSN", ""),
		KafkaEnabled:    env.boolean("RECON_KAFKA_ENABLED", false),
		KafkaBrokers:    env.list("RECON_KAFKA_BROKERS", "localhost:9092"),
		KafkaGroup:      env.str("RECON_KAFKA_GROUP", "recon-stream"),
		KafkaTopics:     env.list("RECON_KAFKA_TOPICS", "ledger.legs,rail.legs"),
		WindowTTL:       env.dur("RECON_WINDOW_TTL", 48*time.Hour),
		SweepInterval:   env.dur("RECON_SWEEP_INTERVAL", 30*time.Second),
		Rules: recon.Rules{
			ToleranceBps:        env.i64("RECON_TOLERANCE_BPS", 10),
			ToleranceAbsMinor:   env.i64("RECON_TOLERANCE_ABS_MINOR", 2),
			ValueDateWindowDays: int(env.i64("RECON_VALUE_DATE_WINDOW_DAYS", 2)),
			T3MaxCandidates:     int(env.i64("RECON_T3_MAX_CANDIDATES", 24)),
			T3MaxSubset:         int(env.i64("RECON_T3_MAX_SUBSET", 6)),
			T3RequireBatchHint:  env.boolean("RECON_T3_REQUIRE_BATCH_HINT", true),
		},
		OTelEndpoint: env.str("RECON_OTEL_ENDPOINT", ""),
		OTelInsecure: env.boolean("RECON_OTEL_INSECURE", true),
	}

	switch cfg.Store {
	case StoreMemory:
	case StorePostgres:
		if cfg.PostgresDSN == "" {
			errs = append(errs, errors.New("RECON_POSTGRES_DSN is required when RECON_STORE=postgres"))
		}
	default:
		errs = append(errs, fmt.Errorf("RECON_STORE must be memory or postgres, got %q", cfg.Store))
	}
	switch cfg.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		errs = append(errs, fmt.Errorf("RECON_LOG_LEVEL must be debug|info|warn|error, got %q", cfg.LogLevel))
	}
	if cfg.KafkaEnabled {
		if len(cfg.KafkaBrokers) == 0 {
			errs = append(errs, errors.New("RECON_KAFKA_BROKERS is required when Kafka is enabled"))
		}
		if len(cfg.KafkaTopics) == 0 {
			errs = append(errs, errors.New("RECON_KAFKA_TOPICS is required when Kafka is enabled"))
		}
	}
	if cfg.WindowTTL <= 0 {
		errs = append(errs, errors.New("RECON_WINDOW_TTL must be > 0"))
	}
	if cfg.SweepInterval <= 0 {
		errs = append(errs, errors.New("RECON_SWEEP_INTERVAL must be > 0"))
	}
	if cfg.ShutdownTimeout <= 0 {
		errs = append(errs, errors.New("RECON_SHUTDOWN_TIMEOUT must be > 0"))
	}
	if err := cfg.Rules.Validate(); err != nil {
		errs = append(errs, err)
	}
	if err := errors.Join(errs...); err != nil {
		return Config{}, fmt.Errorf("config: %w", err)
	}
	return cfg, nil
}

type envReader struct {
	lookup func(string) (string, bool)
	errs   *[]error
}

func (e envReader) str(key, def string) string {
	if v, ok := e.lookup(key); ok && strings.TrimSpace(v) != "" {
		return strings.TrimSpace(v)
	}
	return def
}

func (e envReader) list(key, def string) []string {
	raw := e.str(key, def)
	var out []string
	for _, p := range strings.Split(raw, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func (e envReader) dur(key string, def time.Duration) time.Duration {
	v, ok := e.lookup(key)
	if !ok || strings.TrimSpace(v) == "" {
		return def
	}
	d, err := time.ParseDuration(strings.TrimSpace(v))
	if err != nil {
		*e.errs = append(*e.errs, fmt.Errorf("%s: %w", key, err))
		return def
	}
	return d
}

func (e envReader) i64(key string, def int64) int64 {
	v, ok := e.lookup(key)
	if !ok || strings.TrimSpace(v) == "" {
		return def
	}
	n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
	if err != nil {
		*e.errs = append(*e.errs, fmt.Errorf("%s: %w", key, err))
		return def
	}
	return n
}

func (e envReader) boolean(key string, def bool) bool {
	v, ok := e.lookup(key)
	if !ok || strings.TrimSpace(v) == "" {
		return def
	}
	b, err := strconv.ParseBool(strings.TrimSpace(v))
	if err != nil {
		*e.errs = append(*e.errs, fmt.Errorf("%s: %w", key, err))
		return def
	}
	return b
}

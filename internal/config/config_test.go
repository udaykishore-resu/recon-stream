package config

import (
	"strings"
	"testing"
	"time"
)

func mapLookup(m map[string]string) func(string) (string, bool) {
	return func(k string) (string, bool) {
		v, ok := m[k]
		return v, ok
	}
}

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load(mapLookup(nil))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.HTTPAddr != ":8080" || cfg.Store != StoreMemory || cfg.KafkaEnabled || cfg.WindowTTL != 48*time.Hour {
		t.Fatalf("defaults wrong: %+v", cfg)
	}
	if len(cfg.KafkaTopics) != 2 || cfg.Rules.ToleranceBps != 10 || !cfg.Rules.T3RequireBatchHint {
		t.Fatalf("defaults wrong: %+v", cfg)
	}
}

func TestLoadOverridesAndValidation(t *testing.T) {
	cases := []struct {
		name string
		env  map[string]string
		want string // substring of error, "" for success
	}{
		{"postgres without dsn", map[string]string{"RECON_STORE": "postgres"}, "RECON_POSTGRES_DSN"},
		{"postgres with dsn", map[string]string{"RECON_STORE": "postgres", "RECON_POSTGRES_DSN": "postgres://x"}, ""},
		{"unknown store", map[string]string{"RECON_STORE": "redis"}, "RECON_STORE"},
		{"bad duration", map[string]string{"RECON_WINDOW_TTL": "soon"}, "RECON_WINDOW_TTL"},
		{"negative ttl", map[string]string{"RECON_WINDOW_TTL": "-1s"}, "must be > 0"},
		{"bad int", map[string]string{"RECON_TOLERANCE_BPS": "ten"}, "RECON_TOLERANCE_BPS"},
		{"bad bool", map[string]string{"RECON_KAFKA_ENABLED": "yes please"}, "RECON_KAFKA_ENABLED"},
		{"bad log level", map[string]string{"RECON_LOG_LEVEL": "loud"}, "RECON_LOG_LEVEL"},
		{"kafka no brokers", map[string]string{"RECON_KAFKA_ENABLED": "true", "RECON_KAFKA_BROKERS": " , "}, "RECON_KAFKA_BROKERS"},
		{"rules validated", map[string]string{"RECON_T3_MAX_SUBSET": "99"}, "t3_max_subset"},
		{"list parsing", map[string]string{"RECON_KAFKA_TOPICS": " a.legs , b.legs,"}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := Load(mapLookup(tc.env))
			if tc.want == "" {
				if err != nil {
					t.Fatalf("unexpected: %v", err)
				}
				if tc.name == "list parsing" && (len(cfg.KafkaTopics) != 2 || cfg.KafkaTopics[1] != "b.legs") {
					t.Fatalf("topics=%v", cfg.KafkaTopics)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want error containing %q, got %v", tc.want, err)
			}
		})
	}
}

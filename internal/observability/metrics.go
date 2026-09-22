package observability

import (
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"

	"github.com/udaykishore-resu/recon-stream/internal/domain/recon"
)

// Metrics holds RED metrics for HTTP plus domain metrics for the engine.
type Metrics struct {
	registry *prometheus.Registry

	HTTPRequests *prometheus.CounterVec
	HTTPDuration *prometheus.HistogramVec

	LegsIngested   *prometheus.CounterVec
	Matches        *prometheus.CounterVec
	MatchResidual  *prometheus.HistogramVec
	BreaksOpened   *prometheus.CounterVec
	OpenLegs       prometheus.Gauge
	IngestDuration prometheus.Histogram
	KafkaCommits   *prometheus.CounterVec
}

// NewMetrics registers all collectors on a fresh registry.
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{
		registry: reg,
		HTTPRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "recon_http_requests_total", Help: "HTTP requests by route, method and status class.",
		}, []string{"route", "method", "status"}),
		HTTPDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "recon_http_request_duration_seconds", Help: "HTTP request latency.",
			Buckets: prometheus.DefBuckets,
		}, []string{"route", "method"}),
		LegsIngested: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "recon_legs_ingested_total", Help: "Legs processed by source and outcome.",
		}, []string{"source", "outcome"}),
		Matches: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "recon_matches_total", Help: "Matches created by tier and rule.",
		}, []string{"tier", "rule"}),
		MatchResidual: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "recon_match_residual_minor", Help: "Absolute residual of tolerant matches in minor units.",
			Buckets: []float64{0, 1, 2, 5, 10, 25, 50, 100, 500},
		}, []string{"tier"}),
		BreaksOpened: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "recon_breaks_opened_total", Help: "Breaks opened by category and trigger.",
		}, []string{"category", "trigger"}),
		OpenLegs: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "recon_open_legs", Help: "Legs currently waiting in the matching window.",
		}),
		IngestDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name: "recon_ingest_batch_duration_seconds", Help: "Engine time per ingested batch.",
			Buckets: prometheus.ExponentialBuckets(0.0005, 2, 14),
		}),
		KafkaCommits: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "recon_kafka_commits_total", Help: "Kafka offset commits by result.",
		}, []string{"result"}),
	}
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		m.HTTPRequests, m.HTTPDuration, m.LegsIngested, m.Matches, m.MatchResidual,
		m.BreaksOpened, m.OpenLegs, m.IngestDuration, m.KafkaCommits,
	)
	return m
}

// Handler serves the registry in Prometheus exposition format.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

// Registry exposes the registry (tests).
func (m *Metrics) Registry() *prometheus.Registry { return m.registry }

// Hooks adapts the metrics to the engine's callback interface.
func (m *Metrics) Hooks() recon.Hooks {
	return recon.Hooks{
		OnLeg: func(l recon.Leg, o recon.Outcome) {
			m.LegsIngested.WithLabelValues(l.Source, string(o)).Inc()
		},
		OnMatch: func(mt recon.Match) {
			tier := "T" + strconv.Itoa(int(mt.Tier))
			m.Matches.WithLabelValues(tier, mt.RuleID).Inc()
			r := mt.ResidualMinor
			if r < 0 {
				r = -r
			}
			m.MatchResidual.WithLabelValues(tier).Observe(float64(r))
		},
		OnBreak: func(b recon.Break) {
			m.BreaksOpened.WithLabelValues(string(b.Category), b.Trigger).Inc()
		},
		OnOpen: func(n int) { m.OpenLegs.Set(float64(n)) },
	}
}

// ObserveHTTP records one request.
func (m *Metrics) ObserveHTTP(route, method string, status int, d time.Duration) {
	class := strconv.Itoa(status/100) + "xx"
	m.HTTPRequests.WithLabelValues(route, method, class).Inc()
	m.HTTPDuration.WithLabelValues(route, method).Observe(d.Seconds())
}

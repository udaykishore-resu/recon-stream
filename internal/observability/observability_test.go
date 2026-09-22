package observability

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/udaykishore-resu/recon-stream/internal/domain/recon"
)

func TestLoggerInjectsRequestID(t *testing.T) {
	var buf bytes.Buffer
	log := NewLogger("debug", &buf)
	ctx := WithRequestID(context.Background(), "abc")
	log.With("k", "v").InfoContext(ctx, "hello", "n", 1)
	log.WithGroup("g").InfoContext(ctx, "grouped") // exercises WithGroup; correlation attrs follow the group
	var rec map[string]any
	first, _, _ := bytes.Cut(buf.Bytes(), []byte("\n"))
	if err := json.Unmarshal(first, &rec); err != nil {
		t.Fatalf("not JSON: %s", buf.String())
	}
	if rec["request_id"] != "abc" || rec["msg"] != "hello" || rec["k"] != "v" {
		t.Fatalf("record=%v", rec)
	}
	if RequestID(ctx) != "abc" || RequestID(context.Background()) != "" {
		t.Fatal("RequestID accessor")
	}
	for _, lvl := range []string{"info", "warn", "error", "nonsense"} {
		buf.Reset()
		NewLogger(lvl, &buf).Debug("hidden")
		if buf.Len() != 0 {
			t.Fatalf("level %s should hide debug", lvl)
		}
	}
}

func TestMetricsHooks(t *testing.T) {
	m := NewMetrics()
	h := m.Hooks()
	h.OnLeg(recon.Leg{Source: "ledger"}, recon.OutcomeMatched)
	h.OnMatch(recon.Match{Tier: recon.TierTolerant, RuleID: recon.RuleT2Tolerant, ResidualMinor: -3})
	h.OnBreak(recon.Break{Category: recon.CatFee, Trigger: recon.TriggerRefConflict})
	h.OnOpen(7)
	m.ObserveHTTP("GET /v1/stats", "GET", 200, 10*time.Millisecond)
	if v := testutil.ToFloat64(m.LegsIngested.WithLabelValues("ledger", "matched")); v != 1 {
		t.Fatalf("legs=%v", v)
	}
	if v := testutil.ToFloat64(m.Matches.WithLabelValues("T2", recon.RuleT2Tolerant)); v != 1 {
		t.Fatalf("matches=%v", v)
	}
	if v := testutil.ToFloat64(m.BreaksOpened.WithLabelValues("fee", "ref_conflict")); v != 1 {
		t.Fatalf("breaks=%v", v)
	}
	if v := testutil.ToFloat64(m.OpenLegs); v != 7 {
		t.Fatalf("open=%v", v)
	}
	if v := testutil.ToFloat64(m.HTTPRequests.WithLabelValues("GET /v1/stats", "GET", "2xx")); v != 1 {
		t.Fatalf("http=%v", v)
	}
	if n, err := testutil.GatherAndCount(m.Registry(), "recon_match_residual_minor"); err != nil || n != 1 {
		t.Fatalf("residual histogram: %d %v", n, err)
	}
}

func TestSetupTracingNoop(t *testing.T) {
	tp, shutdown, err := SetupTracing(context.Background(), "svc", "", true)
	if err != nil || tp == nil {
		t.Fatal(err)
	}
	if err := shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	tp2, shutdown2, err := SetupTracing(context.Background(), "svc", "127.0.0.1:1", true)
	if err != nil || tp2 == nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if err := shutdown2(ctx); err != nil && !strings.Contains(err.Error(), "context") && !strings.Contains(err.Error(), "connect") {
		t.Fatalf("unexpected shutdown error: %v", err)
	}
}

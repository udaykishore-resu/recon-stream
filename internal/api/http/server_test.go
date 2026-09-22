package http

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/udaykishore-resu/recon-stream/internal/adapters/memory"
	"github.com/udaykishore-resu/recon-stream/internal/domain/classify"
	"github.com/udaykishore-resu/recon-stream/internal/domain/recon"
	"github.com/udaykishore-resu/recon-stream/internal/observability"
	"github.com/udaykishore-resu/recon-stream/internal/ports"
)

type env struct {
	t     *testing.T
	srv   *httptest.Server
	store *memory.Store
	eng   *recon.Engine
	ready func(context.Context) error
}

func newEnv(t *testing.T) *env {
	t.Helper()
	st := memory.NewStore()
	m := observability.NewMetrics()
	eng, err := recon.NewEngine(st, classify.NewHeuristic(), recon.DefaultRules(), time.Hour, m.Hooks())
	if err != nil {
		t.Fatal(err)
	}
	e := &env{t: t, store: st, eng: eng}
	api := New(Deps{
		Engine: eng, Store: st, Metrics: m, Version: "test",
		Logger: observability.NewLogger("debug", io.Discard),
		Ready: func(ctx context.Context) error {
			if e.ready != nil {
				return e.ready(ctx)
			}
			return nil
		},
	})
	e.srv = httptest.NewServer(api.Handler())
	t.Cleanup(e.srv.Close)
	return e
}

func (e *env) do(method, path string, body string) (int, map[string]any, http.Header) {
	e.t.Helper()
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, e.srv.URL+path, rd)
	if err != nil {
		e.t.Fatal(err)
	}
	req.Header.Set("X-Request-ID", "req-123")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		e.t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var out map[string]any
	if len(raw) > 0 && bytes.HasPrefix(bytes.TrimSpace(raw), []byte("{")) {
		if err := json.Unmarshal(raw, &out); err != nil {
			e.t.Fatalf("decode %s: %v", raw, err)
		}
	}
	return resp.StatusCode, out, resp.Header
}

const sampleBatch = `{"legs":[
 {"source":"ledger","txn_ref":"A1","amount_minor":10000,"currency":"USD","value_date":"2026-09-15","direction":"debit","counterparty":"VISA"},
 {"source":"card_network","txn_ref":"A1","amount_minor":10000,"currency":"USD","value_date":"2026-09-15","direction":"credit","counterparty":"VISA"},
 {"source":"ledger","txn_ref":"F1","amount_minor":100000,"currency":"USD","value_date":"2026-09-15","direction":"debit","counterparty":"VISA"},
 {"source":"card_network","txn_ref":"F1","amount_minor":97500,"currency":"USD","value_date":"2026-09-15","direction":"credit","counterparty":"VISA","attrs":{"fee_minor":"2500"}},
 {"source":"ledger","txn_ref":"ORPHAN","amount_minor":42,"currency":"USD","value_date":"2026-09-15","direction":"debit","counterparty":"VISA"}
]}`

func TestEndToEndFlow(t *testing.T) {
	e := newEnv(t)

	code, body, hdr := e.do(http.MethodPost, "/v1/legs", sampleBatch)
	if code != http.StatusOK {
		t.Fatalf("ingest: %d %v", code, body)
	}
	if hdr.Get("X-Request-ID") != "req-123" {
		t.Fatal("request id not echoed")
	}
	if body["matched"].(float64) != 1 || body["breaks"].(float64) != 1 || body["open"].(float64) != 3 {
		t.Fatalf("batch result: %v", body)
	}
	results := body["results"].([]any)
	matchID := results[1].(map[string]any)["match_id"].(string)
	breakID := results[3].(map[string]any)["break_id"].(string)

	code, body, _ = e.do(http.MethodGet, "/v1/matches/"+matchID, "")
	if code != http.StatusOK || body["match"].(map[string]any)["rule_id"] != recon.RuleT1Exact || len(body["legs"].([]any)) != 2 {
		t.Fatalf("get match: %d %v", code, body)
	}
	if code, _, _ = e.do(http.MethodGet, "/v1/matches/nope", ""); code != http.StatusNotFound {
		t.Fatalf("missing match: %d", code)
	}

	code, body, _ = e.do(http.MethodGet, "/v1/breaks?status=open&category=fee", "")
	if code != http.StatusOK || body["count"].(float64) != 1 {
		t.Fatalf("list breaks: %d %v", code, body)
	}
	br := body["breaks"].([]any)[0].(map[string]any)
	if br["id"] != breakID || br["category"] != "fee" || br["classifier_id"] != classify.HeuristicID {
		t.Fatalf("break: %v", br)
	}
	code, body, _ = e.do(http.MethodGet, "/v1/breaks/"+breakID, "")
	if code != http.StatusOK || len(body["legs"].([]any)) != 2 {
		t.Fatalf("get break: %d %v", code, body)
	}

	// Resolve with audit trail.
	code, body, _ = e.do(http.MethodPost, "/v1/breaks/"+breakID+"/resolve", `{"reason":"interchange fee, booked to 4210","actor":"ops@bank"}`)
	if code != http.StatusOK || body["status"] != "resolved" || body["actor"] != "ops@bank" {
		t.Fatalf("resolve: %d %v", code, body)
	}
	if code, _, _ = e.do(http.MethodPost, "/v1/breaks/"+breakID+"/resolve", `{"reason":"x","actor":"y"}`); code != http.StatusConflict {
		t.Fatalf("second resolve: %d", code)
	}
	if code, _, _ = e.do(http.MethodPost, "/v1/breaks/missing/resolve", `{"reason":"x","actor":"y"}`); code != http.StatusNotFound {
		t.Fatalf("missing resolve: %d", code)
	}
	if code, _, _ = e.do(http.MethodPost, "/v1/breaks/"+breakID+"/resolve", `{"reason":"","actor":"y"}`); code != http.StatusBadRequest {
		t.Fatalf("empty reason: %d", code)
	}

	// Close the window: orphan becomes a missing_counterparty break.
	code, body, _ = e.do(http.MethodPost, "/v1/windows/close", "")
	if code != http.StatusOK || body["expired"].(float64) != 1 {
		t.Fatalf("close window: %d %v", code, body)
	}
	code, body, _ = e.do(http.MethodGet, "/v1/breaks?category=missing_counterparty", "")
	if code != http.StatusOK || body["count"].(float64) != 1 {
		t.Fatalf("missing cp: %d %v", code, body)
	}

	// Stats.
	code, body, _ = e.do(http.MethodGet, "/v1/stats", "")
	st := body["stats"].(map[string]any)
	if code != http.StatusOK || st["legs_total"].(float64) != 5 || st["breaks_resolved"].(float64) != 1 || body["version"] != "test" {
		t.Fatalf("stats: %d %v", code, body)
	}

	// Events and replay.
	code, body, _ = e.do(http.MethodGet, "/v1/events?from=1&limit=3", "")
	if code != http.StatusOK || body["count"].(float64) != 3 {
		t.Fatalf("events: %d %v", code, body)
	}
	code, body, _ = e.do(http.MethodPost, "/v1/replay?from=1", "")
	if code != http.StatusOK || body["legs_replayed"].(float64) != 5 {
		t.Fatalf("replay: %d %v", code, body)
	}
	code, body, _ = e.do(http.MethodGet, "/v1/stats", "")
	if code != http.StatusOK || body["stats"].(map[string]any)["legs_total"].(float64) != 5 {
		t.Fatalf("stats after replay: %v", body)
	}

	// Redelivery of the same batch is a no-op.
	code, body, _ = e.do(http.MethodPost, "/v1/legs", sampleBatch)
	if code != http.StatusOK || body["duplicates"].(float64) != 5 {
		t.Fatalf("redelivery: %d %v", code, body)
	}

	// Bare array body form.
	code, body, _ = e.do(http.MethodPost, "/v1/legs", `[{"source":"ledger","txn_ref":"Z","amount_minor":1,"currency":"EUR","value_date":"2026-09-15","direction":"debit","counterparty":"MC"}]`)
	if code != http.StatusOK || body["open"].(float64) != 1 {
		t.Fatalf("array body: %d %v", code, body)
	}
	legID := body["results"].([]any)[0].(map[string]any)["leg_id"].(string)
	if code, body, _ = e.do(http.MethodGet, "/v1/legs/"+legID, ""); code != http.StatusOK || body["status"] != "open" {
		t.Fatalf("get leg: %d %v", code, body)
	}
}

func TestValidationErrors(t *testing.T) {
	e := newEnv(t)
	cases := []struct {
		name, method, path, body string
		want                     int
	}{
		{"empty body", http.MethodPost, "/v1/legs", "", http.StatusBadRequest},
		{"malformed json", http.MethodPost, "/v1/legs", "{", http.StatusBadRequest},
		{"no legs", http.MethodPost, "/v1/legs", `{"legs":[]}`, http.StatusBadRequest},
		{"invalid leg", http.MethodPost, "/v1/legs", `[{"source":"ledger"}]`, http.StatusBadRequest},
		{"bad date", http.MethodPost, "/v1/legs", `[{"source":"ledger","txn_ref":"x","amount_minor":1,"currency":"USD","value_date":"15/09/2026","direction":"debit","counterparty":"V"}]`, http.StatusBadRequest},
		{"bad limit", http.MethodGet, "/v1/breaks?limit=0", "", http.StatusBadRequest},
		{"bad offset", http.MethodGet, "/v1/breaks?offset=-1", "", http.StatusBadRequest},
		{"bad min_age", http.MethodGet, "/v1/breaks?min_age=soon", "", http.StatusBadRequest},
		{"bad status", http.MethodGet, "/v1/breaks?status=weird", "", http.StatusBadRequest},
		{"bad replay from", http.MethodPost, "/v1/replay?from=0", "", http.StatusBadRequest},
		{"bad events from", http.MethodGet, "/v1/events?from=-1", "", http.StatusBadRequest},
		{"bad events limit", http.MethodGet, "/v1/events?limit=5000", "", http.StatusBadRequest},
		{"bad older_than", http.MethodPost, "/v1/windows/close?older_than=never", "", http.StatusBadRequest},
		{"resolve malformed", http.MethodPost, "/v1/breaks/x/resolve", "{", http.StatusBadRequest},
		{"unknown route", http.MethodGet, "/v1/nothing", "", http.StatusNotFound},
		{"wrong method", http.MethodDelete, "/v1/stats", "", http.StatusMethodNotAllowed},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, _, _ := e.do(tc.method, tc.path, tc.body)
			if code != tc.want {
				t.Fatalf("code=%d want %d", code, tc.want)
			}
		})
	}
	big := strings.Repeat(`{"source":"ledger","txn_ref":"x","amount_minor":1,"currency":"USD","value_date":"2026-09-15","direction":"debit","counterparty":"V"},`, 5001)
	code, _, _ := e.do(http.MethodPost, "/v1/legs", "["+strings.TrimSuffix(big, ",")+"]")
	if code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversized batch: %d", code)
	}
}

func TestHealthReadyMetrics(t *testing.T) {
	e := newEnv(t)
	if code, body, _ := e.do(http.MethodGet, "/healthz", ""); code != http.StatusOK || body["status"] != "ok" {
		t.Fatalf("healthz: %d %v", code, body)
	}
	if code, body, _ := e.do(http.MethodGet, "/readyz", ""); code != http.StatusOK || body["ready"] != true {
		t.Fatalf("readyz: %d %v", code, body)
	}
	e.ready = func(context.Context) error { return errors.New("kafka down") }
	if code, body, _ := e.do(http.MethodGet, "/readyz", ""); code != http.StatusServiceUnavailable || body["checks"].(map[string]any)["source"] != "kafka down" {
		t.Fatalf("readyz degraded: %d %v", code, body)
	}
	e.do(http.MethodPost, "/v1/legs", sampleBatch)
	resp, err := http.Get(e.srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	for _, want := range []string{"recon_http_requests_total", "recon_legs_ingested_total", "recon_matches_total{", "recon_breaks_opened_total{", "recon_open_legs"} {
		if !strings.Contains(string(raw), want) {
			t.Fatalf("metrics missing %s", want)
		}
	}
}

type failingStore struct{ ports.Store }

func (failingStore) Stats(context.Context) (ports.Stats, error) {
	return ports.Stats{}, errors.New("boom")
}
func (failingStore) ListBreaks(context.Context, ports.BreakFilter) ([]recon.Break, error) {
	return nil, errors.New("boom")
}
func (failingStore) GetMatch(context.Context, string) (recon.Match, error) {
	return recon.Match{}, errors.New("boom")
}
func (failingStore) EventsFrom(context.Context, int64, int) ([]recon.Event, error) {
	return nil, errors.New("boom")
}
func (failingStore) Ping(context.Context) error { return errors.New("db down") }

func TestStoreFailuresAre500(t *testing.T) {
	st := memory.NewStore()
	m := observability.NewMetrics()
	eng, _ := recon.NewEngine(st, classify.NewHeuristic(), recon.DefaultRules(), time.Hour, recon.Hooks{})
	api := New(Deps{Engine: eng, Store: failingStore{st}, Metrics: m, Logger: observability.NewLogger("error", io.Discard)})
	srv := httptest.NewServer(api.Handler())
	defer srv.Close()
	for _, path := range []string{"/v1/stats", "/v1/breaks", "/v1/matches/x", "/v1/events"} {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusInternalServerError {
			t.Fatalf("%s: %d", path, resp.StatusCode)
		}
	}
	resp, _ := http.Get(srv.URL + "/readyz")
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("readyz with dead store: %d", resp.StatusCode)
	}
}

func TestRecoverer(t *testing.T) {
	st := memory.NewStore()
	m := observability.NewMetrics()
	eng, _ := recon.NewEngine(st, classify.NewHeuristic(), recon.DefaultRules(), time.Hour, recon.Hooks{})
	api := New(Deps{Engine: eng, Store: st, Metrics: m, Logger: observability.NewLogger("error", io.Discard)})
	api.route("GET /boom", func(http.ResponseWriter, *http.Request) { panic("kaboom") })
	srv := httptest.NewServer(api.Handler())
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/boom")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("panic should be 500, got %d", resp.StatusCode)
	}
}

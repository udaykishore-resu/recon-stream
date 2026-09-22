// Package http exposes the reconciliation engine over a JSON HTTP API using
// Go 1.22+ ServeMux patterns. It contains no business rules.
package http

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/otel/trace"

	"github.com/udaykishore-resu/recon-stream/internal/domain/recon"
	"github.com/udaykishore-resu/recon-stream/internal/observability"
	"github.com/udaykishore-resu/recon-stream/internal/ports"
)

// MaxBodyBytes bounds request bodies (a 5k-leg batch is ~2 MB).
const MaxBodyBytes = 16 << 20

// Deps are the collaborators the server needs.
type Deps struct {
	Engine  *recon.Engine
	Store   ports.Store
	Logger  *slog.Logger
	Metrics *observability.Metrics
	Tracer  trace.Tracer
	// Ready is an extra readiness probe (e.g. Kafka client health); optional.
	Ready func(context.Context) error
	// Version is reported by GET /v1/stats and /healthz.
	Version string
	Now     func() time.Time
}

// Server is the HTTP façade.
type Server struct {
	deps Deps
	mux  *http.ServeMux
}

// New builds the router.
func New(d Deps) *Server {
	if d.Now == nil {
		d.Now = time.Now
	}
	if d.Logger == nil {
		d.Logger = slog.Default()
	}
	s := &Server{deps: d, mux: http.NewServeMux()}
	s.route("GET /healthz", s.healthz)
	s.route("GET /readyz", s.readyz)
	s.mux.Handle("GET /metrics", d.Metrics.Handler())

	s.route("POST /v1/legs", s.postLegs)
	s.route("GET /v1/breaks", s.listBreaks)
	s.route("GET /v1/breaks/{id}", s.getBreak)
	s.route("POST /v1/breaks/{id}/resolve", s.resolveBreak)
	s.route("GET /v1/matches/{id}", s.getMatch)
	s.route("GET /v1/legs/{id}", s.getLeg)
	s.route("GET /v1/stats", s.stats)
	s.route("GET /v1/events", s.events)
	s.route("POST /v1/replay", s.replay)
	s.route("POST /v1/windows/close", s.closeWindow)
	return s
}

// Handler returns the fully wrapped handler chain.
func (s *Server) Handler() http.Handler {
	return s.recoverer(s.requestID(s.tracing(s.observe(s.mux))))
}

func (s *Server) route(pattern string, h http.HandlerFunc) {
	s.mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
		if rw, ok := w.(*responseWriter); ok {
			rw.route = pattern
		}
		h(w, r)
	})
}

// ---- handlers -------------------------------------------------------------

func (s *Server) healthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "version": s.deps.Version})
}

func (s *Server) readyz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	checks := map[string]string{}
	status := http.StatusOK
	if err := s.deps.Store.Ping(ctx); err != nil {
		checks["store"] = err.Error()
		status = http.StatusServiceUnavailable
	} else {
		checks["store"] = "ok"
	}
	if s.deps.Ready != nil {
		if err := s.deps.Ready(ctx); err != nil {
			checks["source"] = err.Error()
			status = http.StatusServiceUnavailable
		} else {
			checks["source"] = "ok"
		}
	}
	writeJSON(w, status, map[string]any{"ready": status == http.StatusOK, "checks": checks})
}

type legsRequest struct {
	Legs []recon.Leg `json:"legs"`
}

func (s *Server) postLegs(w http.ResponseWriter, r *http.Request) {
	body, err := readBody(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	var req legsRequest
	trimmed := strings.TrimSpace(string(body))
	switch {
	case strings.HasPrefix(trimmed, "["):
		err = json.Unmarshal(body, &req.Legs)
	default:
		err = json.Unmarshal(body, &req)
	}
	if err != nil {
		writeError(w, http.StatusBadRequest, "malformed JSON: "+err.Error())
		return
	}
	if len(req.Legs) == 0 {
		writeError(w, http.StatusBadRequest, "no legs supplied")
		return
	}
	if len(req.Legs) > 5000 {
		writeError(w, http.StatusRequestEntityTooLarge, "batch limited to 5000 legs")
		return
	}
	start := s.deps.Now()
	res, err := s.deps.Engine.Ingest(r.Context(), req.Legs)
	s.deps.Metrics.IngestDuration.Observe(s.deps.Now().Sub(start).Seconds())
	if err != nil {
		if errors.Is(err, recon.ErrInvalidLeg) {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		s.deps.Logger.ErrorContext(r.Context(), "ingest failed", "error", err)
		writeError(w, http.StatusInternalServerError, "ingest failed")
		return
	}
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) listBreaks(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f := ports.BreakFilter{
		Status:       recon.BreakStatus(q.Get("status")),
		Category:     recon.Category(q.Get("category")),
		Currency:     strings.ToUpper(q.Get("currency")),
		Counterparty: q.Get("counterparty"),
		Limit:        100,
	}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 1000 {
			writeError(w, http.StatusBadRequest, "limit must be 1..1000")
			return
		}
		f.Limit = n
	}
	if v := q.Get("offset"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			writeError(w, http.StatusBadRequest, "offset must be >= 0")
			return
		}
		f.Offset = n
	}
	if v := q.Get("min_age"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d < 0 {
			writeError(w, http.StatusBadRequest, "min_age must be a duration like 30m or 2h")
			return
		}
		f.MinAge = d
	}
	if f.Status != "" && f.Status != recon.BreakOpen && f.Status != recon.BreakResolved {
		writeError(w, http.StatusBadRequest, "status must be open or resolved")
		return
	}
	breaks, err := s.deps.Store.ListBreaks(r.Context(), f)
	if err != nil {
		s.deps.Logger.ErrorContext(r.Context(), "list breaks", "error", err)
		writeError(w, http.StatusInternalServerError, "list breaks failed")
		return
	}
	now := s.deps.Now().UTC()
	for i := range breaks {
		breaks[i].AgeSeconds = int64(breaks[i].Age(now).Seconds())
	}
	writeJSON(w, http.StatusOK, map[string]any{"breaks": breaks, "count": len(breaks), "limit": f.Limit, "offset": f.Offset})
}

func (s *Server) getBreak(w http.ResponseWriter, r *http.Request) {
	b, err := s.deps.Store.GetBreak(r.Context(), r.PathValue("id"))
	if s.lookupFailed(w, r, err, "break") {
		return
	}
	b.AgeSeconds = int64(b.Age(s.deps.Now().UTC()).Seconds())
	legs := s.loadLegs(r.Context(), b.LegIDs)
	writeJSON(w, http.StatusOK, map[string]any{"break": b, "legs": legs})
}

type resolveRequest struct {
	Reason string `json:"reason"`
	Actor  string `json:"actor"`
}

func (s *Server) resolveBreak(w http.ResponseWriter, r *http.Request) {
	body, err := readBody(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	var req resolveRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeError(w, http.StatusBadRequest, "malformed JSON: "+err.Error())
		return
	}
	req.Reason, req.Actor = strings.TrimSpace(req.Reason), strings.TrimSpace(req.Actor)
	if req.Reason == "" || req.Actor == "" {
		writeError(w, http.StatusBadRequest, "reason and actor are required")
		return
	}
	b, err := s.deps.Engine.ResolveBreak(r.Context(), r.PathValue("id"), req.Reason, req.Actor)
	switch {
	case errors.Is(err, ports.ErrNotFound):
		writeError(w, http.StatusNotFound, "break not found")
		return
	case errors.Is(err, recon.ErrBreakResolved):
		writeError(w, http.StatusConflict, "break already resolved")
		return
	case err != nil:
		s.deps.Logger.ErrorContext(r.Context(), "resolve break", "error", err)
		writeError(w, http.StatusInternalServerError, "resolve failed")
		return
	}
	s.deps.Logger.InfoContext(r.Context(), "break resolved", "break_id", b.ID, "actor", req.Actor, "category", b.Category)
	b.AgeSeconds = int64(b.Age(s.deps.Now().UTC()).Seconds())
	writeJSON(w, http.StatusOK, b)
}

func (s *Server) getMatch(w http.ResponseWriter, r *http.Request) {
	m, err := s.deps.Store.GetMatch(r.Context(), r.PathValue("id"))
	if s.lookupFailed(w, r, err, "match") {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"match": m, "legs": s.loadLegs(r.Context(), m.LegIDs)})
}

func (s *Server) getLeg(w http.ResponseWriter, r *http.Request) {
	l, err := s.deps.Store.GetLeg(r.Context(), r.PathValue("id"))
	if s.lookupFailed(w, r, err, "leg") {
		return
	}
	writeJSON(w, http.StatusOK, l)
}

func (s *Server) stats(w http.ResponseWriter, r *http.Request) {
	st, err := s.deps.Store.Stats(r.Context())
	if err != nil {
		s.deps.Logger.ErrorContext(r.Context(), "stats", "error", err)
		writeError(w, http.StatusInternalServerError, "stats failed")
		return
	}
	rules := s.deps.Engine.Rules()
	writeJSON(w, http.StatusOK, map[string]any{
		"stats":           st,
		"open_legs_index": s.deps.Engine.OpenLegs(),
		"rules":           rules,
		"version":         s.deps.Version,
	})
}

func (s *Server) events(w http.ResponseWriter, r *http.Request) {
	from, limit := int64(1), 100
	if v := r.URL.Query().Get("from"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < 1 {
			writeError(w, http.StatusBadRequest, "from must be a positive sequence number")
			return
		}
		from = n
	}
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 1000 {
			writeError(w, http.StatusBadRequest, "limit must be 1..1000")
			return
		}
		limit = n
	}
	evs, err := s.deps.Store.EventsFrom(r.Context(), from, limit)
	if err != nil {
		s.deps.Logger.ErrorContext(r.Context(), "events", "error", err)
		writeError(w, http.StatusInternalServerError, "events failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"events": evs, "count": len(evs)})
}

func (s *Server) replay(w http.ResponseWriter, r *http.Request) {
	from := int64(1)
	if v := r.URL.Query().Get("from"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < 1 {
			writeError(w, http.StatusBadRequest, "from must be a positive event sequence number")
			return
		}
		from = n
	}
	res, err := s.deps.Engine.Replay(r.Context(), from)
	if err != nil {
		s.deps.Logger.ErrorContext(r.Context(), "replay failed", "error", err, "from", from)
		writeError(w, http.StatusInternalServerError, "replay failed: "+err.Error())
		return
	}
	s.deps.Logger.InfoContext(r.Context(), "replay complete", "from", res.FromSeq, "through", res.ThroughSeq, "legs", res.LegsReplayed)
	writeJSON(w, http.StatusOK, res)
}

func (s *Server) closeWindow(w http.ResponseWriter, r *http.Request) {
	olderThan := time.Duration(0)
	if v := r.URL.Query().Get("older_than"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d < 0 {
			writeError(w, http.StatusBadRequest, "older_than must be a duration like 1h")
			return
		}
		olderThan = d
	}
	n, err := s.deps.Engine.CloseWindow(r.Context(), olderThan)
	if err != nil {
		s.deps.Logger.ErrorContext(r.Context(), "close window", "error", err)
		writeError(w, http.StatusInternalServerError, "close window failed")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"expired": n, "older_than": olderThan.String()})
}

// ---- helpers --------------------------------------------------------------

func (s *Server) lookupFailed(w http.ResponseWriter, r *http.Request, err error, what string) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ports.ErrNotFound) {
		writeError(w, http.StatusNotFound, what+" not found")
		return true
	}
	s.deps.Logger.ErrorContext(r.Context(), "lookup "+what, "error", err)
	writeError(w, http.StatusInternalServerError, "lookup failed")
	return true
}

func (s *Server) loadLegs(ctx context.Context, ids []string) []recon.Leg {
	out := make([]recon.Leg, 0, len(ids))
	for _, id := range ids {
		if l, err := s.deps.Store.GetLeg(ctx, id); err == nil {
			out = append(out, l)
		}
	}
	return out
}

func readBody(r *http.Request) ([]byte, error) {
	if r.Body == nil {
		return nil, errors.New("empty body")
	}
	body, err := io.ReadAll(http.MaxBytesReader(nil, r.Body, MaxBodyBytes))
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}
	if len(body) == 0 {
		return nil, errors.New("empty body")
	}
	return body, nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]any{"error": msg, "status": status})
}

package http

import (
	"net/http"
	"runtime/debug"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/udaykishore-resu/recon-stream/internal/observability"
)

type responseWriter struct {
	http.ResponseWriter
	status int
	bytes  int
	route  string
}

func (rw *responseWriter) WriteHeader(code int) {
	if rw.status == 0 {
		rw.status = code
	}
	rw.ResponseWriter.WriteHeader(code)
}

func (rw *responseWriter) Write(b []byte) (int, error) {
	if rw.status == 0 {
		rw.status = http.StatusOK
	}
	n, err := rw.ResponseWriter.Write(b)
	rw.bytes += n
	return n, err
}

// requestID honours an inbound X-Request-ID or mints a UUID, echoes it back,
// and stores it on the context for log correlation.
func (s *Server) requestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-ID")
		if id == "" || len(id) > 128 {
			id = uuid.NewString()
		}
		w.Header().Set("X-Request-ID", id)
		next.ServeHTTP(w, r.WithContext(observability.WithRequestID(r.Context(), id)))
	})
}

// tracing starts a server span per request, continuing an inbound W3C context.
func (s *Server) tracing(next http.Handler) http.Handler {
	tracer := s.deps.Tracer
	if tracer == nil {
		tracer = otel.Tracer("recon-stream/http")
	}
	prop := propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}, propagation.Baggage{})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := prop.Extract(r.Context(), propagation.HeaderCarrier(r.Header))
		ctx, span := tracer.Start(ctx, r.Method+" "+r.URL.Path, trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(attribute.String("http.method", r.Method), attribute.String("http.target", r.URL.Path)))
		defer span.End()
		next.ServeHTTP(w, r.WithContext(ctx))
		if rw, ok := w.(*responseWriter); ok {
			span.SetAttributes(attribute.Int("http.status_code", rw.status), attribute.String("http.route", rw.route))
			if rw.status >= 500 {
				span.SetStatus(codes.Error, http.StatusText(rw.status))
			}
		}
	})
}

// observe wraps the writer, records RED metrics and emits one access log line.
func (s *Server) observe(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rw := &responseWriter{ResponseWriter: w}
		next.ServeHTTP(rw, r)
		if rw.status == 0 {
			rw.status = http.StatusOK
		}
		route := rw.route
		if route == "" {
			route = "unmatched"
		}
		d := time.Since(start)
		s.deps.Metrics.ObserveHTTP(route, r.Method, rw.status, d)
		if route == "GET /metrics" || route == "GET /healthz" || route == "GET /readyz" {
			return
		}
		s.deps.Logger.InfoContext(r.Context(), "http request",
			"method", r.Method, "path", r.URL.Path, "route", route, "status", rw.status,
			"bytes", rw.bytes, "duration_ms", float64(d.Microseconds())/1000, "remote", r.RemoteAddr)
	})
}

// recoverer converts panics into 500s so one bad request cannot take the process down.
func (s *Server) recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				s.deps.Logger.ErrorContext(r.Context(), "panic recovered", "panic", rec, "stack", string(debug.Stack()))
				writeError(w, http.StatusInternalServerError, "internal error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

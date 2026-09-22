package recon_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/udaykishore-resu/recon-stream/internal/adapters/memory"
	"github.com/udaykishore-resu/recon-stream/internal/domain/classify"
	"github.com/udaykishore-resu/recon-stream/internal/domain/recon"
)

// BenchmarkIngestPairs measures end-to-end engine throughput (validation,
// dedupe lookup, T1 match, persistence in the memory store) for ledger/rail
// pairs where the rail leg arrives 500 legs after its ledger partner.
func BenchmarkIngestPairs(b *testing.B) {
	const lag = 500
	st := memory.NewStore()
	eng, err := recon.NewEngine(st, classify.NewHeuristic(), recon.DefaultRules(), time.Hour, recon.Hooks{})
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	legs := make([]recon.Leg, 0, 2*b.N)
	for i := 0; i < b.N; i++ {
		ref := fmt.Sprintf("TXN-%09d", i)
		legs = append(legs, leg("ledger", ref, int64(1000+i%50_000), "USD", d15, nil))
		if i >= lag {
			pref := fmt.Sprintf("TXN-%09d", i-lag)
			legs = append(legs, leg("rail", pref, int64(1000+(i-lag)%50_000), "USD", d15, nil))
		}
	}
	b.ResetTimer()
	b.ReportAllocs()
	for start := 0; start < len(legs); start += 1000 {
		end := start + 1000
		if end > len(legs) {
			end = len(legs)
		}
		if _, err := eng.Ingest(ctx, legs[start:end]); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(len(legs))/b.Elapsed().Seconds(), "legs/s")
}

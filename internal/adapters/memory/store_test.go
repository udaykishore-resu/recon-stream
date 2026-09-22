package memory

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/udaykishore-resu/recon-stream/internal/domain/recon"
	"github.com/udaykishore-resu/recon-stream/internal/ports"
)

func mkLeg(src, ref string, status recon.LegStatus) recon.Leg {
	l := recon.Leg{Source: src, TxnRef: ref, AmountMinor: 100, Currency: "USD", ValueDate: recon.NewDate(2026, 9, 15), Direction: recon.Debit, Counterparty: "VISA", Status: status}
	l.AssignID()
	return l
}

func TestStoreRoundTrip(t *testing.T) {
	ctx := context.Background()
	s := NewStore()
	a := mkLeg("ledger", "A", recon.LegOpen)
	b := mkLeg("rail", "A", recon.LegMatched)
	m := recon.Match{ID: "m_1", LegIDs: []string{a.ID, b.ID}, Tier: recon.TierExact, RuleID: recon.RuleT1Exact, Confidence: 1}
	br := recon.Break{ID: "brk_1", LegIDs: []string{a.ID}, Category: recon.CatFee, Status: recon.BreakOpen, Currency: "USD", Counterparty: "VISA", OpenedAt: time.Now().Add(-time.Hour)}
	ev, _ := recon.NewEvent(recon.EvLegIngested, time.Now(), a.ID, a)
	if err := s.Apply(ctx, recon.ChangeSet{Legs: []recon.Leg{a, b}, Matches: []recon.Match{m}, Breaks: []recon.Break{br}, Events: []recon.Event{ev, ev}}); err != nil {
		t.Fatal(err)
	}
	if ok, _ := s.HasLeg(ctx, a.ID); !ok {
		t.Fatal("HasLeg")
	}
	if _, err := s.GetLeg(ctx, "nope"); !errors.Is(err, ports.ErrNotFound) {
		t.Fatal("GetLeg not found")
	}
	if ls, _ := s.LegsByTxnRef(ctx, "A"); len(ls) != 2 {
		t.Fatalf("by ref: %d", len(ls))
	}
	if ls, _ := s.OpenLegs(ctx); len(ls) != 1 || ls[0].ID != a.ID {
		t.Fatalf("open legs: %v", ls)
	}
	if got, err := s.GetMatch(ctx, "m_1"); err != nil || got.RuleID != recon.RuleT1Exact {
		t.Fatal("GetMatch")
	}
	if _, err := s.GetMatch(ctx, "x"); !errors.Is(err, ports.ErrNotFound) {
		t.Fatal("GetMatch not found")
	}
	if _, err := s.GetBreak(ctx, "x"); !errors.Is(err, ports.ErrNotFound) {
		t.Fatal("GetBreak not found")
	}
	if bs, _ := s.ListBreaks(ctx, ports.BreakFilter{Currency: "USD", Counterparty: "VISA", MinAge: 30 * time.Minute}); len(bs) != 1 {
		t.Fatalf("list: %v", bs)
	}
	if bs, _ := s.ListBreaks(ctx, ports.BreakFilter{Counterparty: "AMEX"}); len(bs) != 0 {
		t.Fatal("counterparty filter")
	}
	if seq, _ := s.LastSeq(ctx); seq != 2 {
		t.Fatalf("LastSeq=%d", seq)
	}
	if evs, _ := s.EventsFrom(ctx, 2, 10); len(evs) != 1 || evs[0].Seq != 2 {
		t.Fatalf("EventsFrom: %v", evs)
	}
	if evs, _ := s.EventsFrom(ctx, 0, 1); len(evs) != 1 || evs[0].Seq != 1 {
		t.Fatalf("EventsFrom clamps: %v", evs)
	}
	if evs, _ := s.EventsFrom(ctx, 99, 10); len(evs) != 0 {
		t.Fatal("EventsFrom past end")
	}
	st, _ := s.Stats(ctx)
	if st.LegsTotal != 2 || st.LegsOpen != 1 || st.LegsMatched != 1 || st.MatchesByTier["T1"] != 1 || st.BreaksOpen != 1 || st.OldestOpenBreak == nil || st.EventsTotal != 2 {
		t.Fatalf("stats=%+v", st)
	}
	if err := s.ResetProjections(ctx); err != nil {
		t.Fatal(err)
	}
	st, _ = s.Stats(ctx)
	if st.LegsTotal != 0 || st.EventsTotal != 2 {
		t.Fatalf("reset must keep events: %+v", st)
	}
	if err := s.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	_ = s.Close()
}

func TestStoreConcurrentApply(t *testing.T) {
	s := NewStore()
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			l := mkLeg("ledger", string(rune('A'+i)), recon.LegOpen)
			ev, _ := recon.NewEvent(recon.EvLegIngested, time.Now(), l.ID, l)
			_ = s.Apply(context.Background(), recon.ChangeSet{Legs: []recon.Leg{l}, Events: []recon.Event{ev}})
			_, _ = s.Stats(context.Background())
		}(i)
	}
	wg.Wait()
	if seq, _ := s.LastSeq(context.Background()); seq != 20 {
		t.Fatalf("seq=%d", seq)
	}
}

func TestSource(t *testing.T) {
	src := NewSource(1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var got int
	done := make(chan error, 1)
	go func() {
		done <- src.Run(ctx, func(_ context.Context, legs []recon.Leg) error {
			got += len(legs)
			if got >= 2 {
				return errors.New("stop")
			}
			return nil
		})
	}()
	if err := src.Publish(ctx, []recon.Leg{mkLeg("ledger", "1", "")}); err != nil {
		t.Fatal(err)
	}
	if err := src.Publish(ctx, []recon.Leg{mkLeg("ledger", "2", "")}); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err == nil || err.Error() != "stop" {
		t.Fatalf("handler error must propagate: %v", err)
	}
	if got != 2 {
		t.Fatalf("got=%d", got)
	}
	// Cancellation ends Run.
	src2 := NewSource(0)
	ctx2, cancel2 := context.WithCancel(context.Background())
	cancel2()
	if err := src2.Run(ctx2, nil); !errors.Is(err, context.Canceled) {
		t.Fatal("cancel")
	}
	if err := src2.Publish(ctx2, nil); !errors.Is(err, context.Canceled) {
		t.Fatal("publish cancel")
	}
	_ = src2.Close()
	if err := src2.Run(context.Background(), nil); err == nil {
		t.Fatal("closed source must error")
	}
}

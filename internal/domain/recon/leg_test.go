package recon

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func validLeg() Leg {
	return Leg{
		Source: "ledger", TxnRef: "TXN-1", AmountMinor: 1000, Currency: "usd",
		ValueDate: NewDate(2026, 9, 15), Direction: "DEBIT", Counterparty: "VISA",
	}
}

func TestLegValidate(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Leg)
		wantOK bool
		errHas string
	}{
		{"valid normalises", func(*Leg) {}, true, ""},
		{"missing source", func(l *Leg) { l.Source = " " }, false, "source"},
		{"missing ref", func(l *Leg) { l.TxnRef = "" }, false, "txn_ref"},
		{"zero amount", func(l *Leg) { l.AmountMinor = 0 }, false, "amount_minor"},
		{"negative amount", func(l *Leg) { l.AmountMinor = -5 }, false, "amount_minor"},
		{"bad currency", func(l *Leg) { l.Currency = "US" }, false, "currency"},
		{"numeric currency", func(l *Leg) { l.Currency = "U5D" }, false, "currency"},
		{"zero date", func(l *Leg) { l.ValueDate = Date{} }, false, "value_date"},
		{"bad direction", func(l *Leg) { l.Direction = "sideways" }, false, "direction"},
		{"missing counterparty", func(l *Leg) { l.Counterparty = "" }, false, "counterparty"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			l := validLeg()
			tc.mutate(&l)
			err := l.Validate()
			if tc.wantOK {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				if l.Currency != "USD" || l.Direction != Debit {
					t.Fatalf("not normalised: %+v", l)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.errHas) {
				t.Fatalf("want error containing %q, got %v", tc.errHas, err)
			}
		})
	}
}

func TestLegIDDeterministicAndContentSensitive(t *testing.T) {
	a := validLeg()
	_ = a.Validate()
	a.AssignID()
	b := validLeg()
	_ = b.Validate()
	b.IngestedAt = time.Now() // metadata must not affect the ID
	b.AssignID()
	if a.ID != b.ID {
		t.Fatalf("same content must give same ID: %s vs %s", a.ID, b.ID)
	}
	c := validLeg()
	c.Attrs = map[string]string{"batch": "B2"}
	_ = c.Validate()
	c.AssignID()
	if c.ID == a.ID {
		t.Fatal("different attrs must change the ID")
	}
	if !strings.HasPrefix(a.ID, "ledger:TXN-1:") {
		t.Fatalf("unexpected id format %s", a.ID)
	}
}

func TestDateJSONRoundTrip(t *testing.T) {
	type wrap struct {
		D Date `json:"d"`
	}
	var w wrap
	if err := json.Unmarshal([]byte(`{"d":"2026-02-28"}`), &w); err != nil {
		t.Fatal(err)
	}
	out, _ := json.Marshal(w)
	if string(out) != `{"d":"2026-02-28"}` {
		t.Fatalf("round trip = %s", out)
	}
	if err := json.Unmarshal([]byte(`{"d":"28/02/2026"}`), &w); err == nil {
		t.Fatal("expected parse error")
	}
	if err := json.Unmarshal([]byte(`{"d":5}`), &w); err == nil {
		t.Fatal("expected type error")
	}
	if DaysBetween(NewDate(2026, 3, 1), NewDate(2026, 2, 26)) != 3 {
		t.Fatal("DaysBetween wrong")
	}
}

func TestRulesTolerance(t *testing.T) {
	r := DefaultRules() // 10 bps, abs 2
	cases := []struct {
		amount, want int64
	}{
		{100, 2},     // 0.1 rounds to 0 -> abs floor 2
		{2000, 2},    // 2 == 2
		{10_000, 10}, // 10 bps of 100.00
		{1_234_567, 1235},
		{-5000, 5},
	}
	for _, tc := range cases {
		if got := r.Tolerance(tc.amount); got != tc.want {
			t.Errorf("Tolerance(%d)=%d want %d", tc.amount, got, tc.want)
		}
	}
	if !r.WithinTolerance(10_000, 10_010) || r.WithinTolerance(10_000, 10_011) {
		t.Fatal("WithinTolerance boundary wrong")
	}
	bad := DefaultRules()
	bad.T3MaxSubset = 1
	if bad.Validate() == nil {
		t.Fatal("expected validation error")
	}
	if got := tolerantConfidence(0, 10); got != 0.95 {
		t.Fatalf("conf(0)=%v", got)
	}
	if got := tolerantConfidence(10, 10); got != 0.80 {
		t.Fatalf("conf(edge)=%v", got)
	}
}

func TestIndexOperations(t *testing.T) {
	ix := NewIndex(time.Hour)
	now := time.Date(2026, 9, 15, 12, 0, 0, 0, time.UTC)
	mk := func(src, ref string, amt int64, age time.Duration) Leg {
		l := Leg{Source: src, TxnRef: ref, AmountMinor: amt, Currency: "USD", ValueDate: NewDate(2026, 9, 15), Direction: Debit, Counterparty: "VISA", IngestedAt: now.Add(-age)}
		l.AssignID()
		return l
	}
	a := mk("ledger", "R1", 100, 2*time.Hour)
	b := mk("rail", "R2", 200, time.Minute)
	c := mk("ledger", "R3", 300, time.Minute)
	ix.Add(a)
	ix.Add(a) // idempotent
	ix.Add(b)
	ix.Add(c)
	if ix.Len() != 3 {
		t.Fatalf("len=%d", ix.Len())
	}
	if got := ix.Candidates(a); len(got) != 1 || got[0].ID != b.ID {
		t.Fatalf("candidates=%v", got)
	}
	if got := ix.CandidatesByRef(mk("rail", "R1", 100, 0)); len(got) != 1 || got[0].ID != a.ID {
		t.Fatalf("candidates by ref=%v", got)
	}
	if got := ix.CandidatesByRef(mk("ledger", "R1", 100, 0)); len(got) != 0 {
		t.Fatalf("same source must not be a candidate: %v", got)
	}
	if got := ix.RefLegs("R2"); len(got) != 1 || got[0].ID != b.ID {
		t.Fatalf("ref legs=%v", got)
	}
	batched := mk("ledger", "R4", 400, time.Minute)
	batched.Attrs = map[string]string{AttrBatchRef: "B1"}
	batched.AssignID()
	ix.Add(batched)
	if got := ix.ByBatch(a, "B1"); len(got) != 1 || got[0].ID != batched.ID {
		t.Fatalf("by batch=%v", got)
	}
	ix.Remove(batched.ID)
	if got := ix.ByBatch(a, "B1"); len(got) != 0 {
		t.Fatalf("batch index not cleaned: %v", got)
	}
	if got := ix.SameSourceInWindow(a); len(got) != 1 || got[0].ID != c.ID {
		t.Fatalf("same source=%v", got)
	}
	if got := ix.Expired(now, -1); len(got) != 1 || got[0].ID != a.ID {
		t.Fatalf("expired=%v", got)
	}
	if got := ix.Expired(now, 0); len(got) != 3 {
		t.Fatalf("forced expiry=%d", len(got))
	}
	ix.Remove(a.ID, "unknown")
	if _, ok := ix.Get(a.ID); ok || ix.Len() != 2 {
		t.Fatal("remove failed")
	}
	if len(ix.All()) != 2 {
		t.Fatal("All wrong")
	}
}

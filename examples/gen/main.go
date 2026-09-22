// Command gen writes examples/legs.json: a deterministic 200-leg sample of a
// merchant-acquiring ledger reconciled against card-network settlement files
// (Visa in USD, Mastercard in GBP), including per-transaction fees, an FX
// settlement, a late settlement, a duplicated settlement record and unsettled
// ledger entries. Regenerate with `make gen`.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"time"

	"github.com/udaykishore-resu/recon-stream/internal/domain/recon"
)

type out struct {
	Description string      `json:"description"`
	Legs        []recon.Leg `json:"legs"`
}

func main() {
	path := flag.String("out", "examples/legs.json", "output file")
	flag.Parse()

	rng := rand.New(rand.NewSource(20260915))
	d14, d15, d16 := recon.NewDate(2026, 9, 14), recon.NewDate(2026, 9, 15), recon.NewDate(2026, 9, 16)
	merchants := []string{"M-1023", "M-2210", "M-3391", "M-4457", "M-5120", "M-6688", "M-7014", "M-8891"}
	usedAmounts := map[int64]bool{}
	amount := func(lo, hi int64) int64 {
		for {
			a := lo + rng.Int63n(hi-lo)
			if !usedAmounts[a] {
				usedAmounts[a] = true
				return a
			}
		}
	}
	seq := 0
	ref := func() string { seq++; return fmt.Sprintf("TXN-%06d", 240000+seq) }
	arn := func() string { return fmt.Sprintf("7451%019d", rng.Int63n(1e15)) }

	var ledger, rail []recon.Leg
	ledgerLeg := func(txn string, amt int64, ccy, cp string, d recon.Date, extra map[string]string) {
		attrs := map[string]string{"account": "1200-SETTLEMENT-" + cp, "merchant_id": merchants[rng.Intn(len(merchants))], "channel": "card_present"}
		for k, v := range extra {
			attrs[k] = v
		}
		ledger = append(ledger, recon.Leg{Source: "ledger", TxnRef: txn, AmountMinor: amt, Currency: ccy, ValueDate: d, Direction: recon.Debit, Counterparty: cp, Attrs: attrs})
	}
	railLeg := func(txn string, amt int64, ccy, cp string, d recon.Date, extra map[string]string) {
		attrs := map[string]string{"arn": arn(), "settlement_file": fmt.Sprintf("%s-STL-20260916-01", cp)}
		for k, v := range extra {
			attrs[k] = v
		}
		rail = append(rail, recon.Leg{Source: "card_network", TxnRef: txn, AmountMinor: amt, Currency: ccy, ValueDate: d, Direction: recon.Credit, Counterparty: cp, Attrs: attrs})
	}
	network := func(i int) (string, string) {
		if i%3 == 0 {
			return "GBP", "MASTERCARD"
		}
		return "USD", "VISA"
	}

	// 76 clean T1 pairs (152 legs): same ref, same amount, settled T+1.
	for i := 0; i < 76; i++ {
		ccy, cp := network(i)
		r, a := ref(), amount(500, 250_000)
		d := d14
		if i%2 == 0 {
			d = d15
		}
		ledgerLeg(r, a, ccy, cp, d, nil)
		railLeg(r, a, ccy, cp, d16, nil)
	}
	// 9 T2 pairs (18 legs): rounding differences of 1-2 minor units on FX-converted interchange.
	for i := 0; i < 9; i++ {
		ccy, cp := network(i)
		r, a := ref(), amount(20_000, 400_000)
		delta := int64(1 + rng.Intn(2))
		if i%2 == 0 {
			delta = -delta
		}
		ledgerLeg(r, a, ccy, cp, d15, nil)
		railLeg(r, a+delta, ccy, cp, d16, map[string]string{"rounding": "scheme"})
	}
	// 4 T3 batches (17 legs): one settlement leg vs 3 (or 4) ledger legs sharing batch_ref.
	for b := 0; b < 4; b++ {
		n := 3
		if b == 3 {
			n = 4
		}
		batch := fmt.Sprintf("VISA-BATCH-2026091%d-%02d", 5, b+1)
		var sum int64
		for i := 0; i < n; i++ {
			a := amount(1_000, 90_000)
			sum += a
			ledgerLeg(ref(), a, "USD", "VISA", d15, map[string]string{recon.AttrBatchRef: batch})
		}
		railLeg(batch, sum, "USD", "VISA", d16, map[string]string{"record_type": "batch_settlement"})
	}
	// 2 T2 amount+date pairs (4 legs): the network reports its ARN instead of our ref.
	for i := 0; i < 2; i++ {
		a := amount(300_000, 900_000)
		ledgerLeg(ref(), a, "USD", "VISA", d15, map[string]string{"note": "ref not echoed by network"})
		railLeg("ARN-"+arn()[:12], a, "USD", "VISA", d16, nil)
	}
	// Break: fee deducted at source (2 legs).
	{
		r, a := ref(), amount(100_000, 300_000)
		fee := a * 25 / 1000 // 2.5% interchange netted from the settlement
		ledgerLeg(r, a, "USD", "VISA", d15, nil)
		railLeg(r, a-fee, "USD", "VISA", d16, map[string]string{"interchange_minor": fmt.Sprint(fee), "record_type": "net_settlement"})
	}
	// Break: FX drift (2 legs): booked in USD, settled in EUR.
	{
		r, a := ref(), amount(50_000, 120_000)
		eur := int64(float64(a) * 0.9183)
		ledgerLeg(r, a, "USD", "VISA", d15, map[string]string{"original_currency": "EUR"})
		railLeg(r, eur, "EUR", "VISA", d16, map[string]string{"fx_rate": "0.9183", "settlement_currency": "EUR"})
	}
	// Break: timing (2 legs): settled 6 days later, outside the ±2 day window.
	{
		r, a := ref(), amount(10_000, 60_000)
		ledgerLeg(r, a, "GBP", "MASTERCARD", d14, nil)
		railLeg(r, a, "GBP", "MASTERCARD", recon.NewDate(2026, 9, 20), map[string]string{"settlement_file": "MASTERCARD-STL-20260920-01"})
	}
	// Break: unsettled ledger entries (2 legs): no network record at all.
	ledgerLeg(ref(), amount(5_000, 40_000), "USD", "VISA", d15, map[string]string{"note": "authorised, never presented"})
	ledgerLeg(ref(), amount(5_000, 40_000), "GBP", "MASTERCARD", d15, map[string]string{"note": "authorised, never presented"})

	// Break: duplicated settlement record (1 leg): the first clean pair's rail leg, re-sent in a second file.
	dup := rail[0]
	dup.Attrs = map[string]string{"arn": dup.Attrs["arn"], "settlement_file": dup.Counterparty + "-STL-20260917-01", "resend": "true"}

	sort.SliceStable(ledger, func(i, j int) bool { return ledger[i].TxnRef < ledger[j].TxnRef })
	// Rail arrives in scheme order: per-txn records, then batch records, then the re-sent duplicate.
	legs := append(append([]recon.Leg{}, ledger...), rail...)
	legs = append(legs, dup)
	for i := range legs {
		if err := legs[i].Validate(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		legs[i].ID, legs[i].IngestedAt = "", time.Time{}
	}
	doc := out{
		Description: fmt.Sprintf("%d legs: %d ledger postings vs %d card-network settlement records (Visa/USD, Mastercard/GBP) for value dates 2026-09-14..16. Expected: 191 legs auto-matched (T1/T2/T3), 9 legs in breaks (fee, fx_drift, timing, duplicate, 2x missing_counterparty).", len(legs), len(ledger), len(rail)+1),
		Legs:        legs,
	}
	b, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if err := os.WriteFile(*path, append(b, '\n'), 0o644); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Printf("wrote %s (%d legs: %d ledger, %d rail)\n", *path, len(legs), len(ledger), len(rail)+1)
}

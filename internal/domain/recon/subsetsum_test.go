package recon

import (
	"math/rand"
	"testing"
)

func legsFromAmounts(amts []int64) []Leg {
	out := make([]Leg, len(amts))
	for i, a := range amts {
		out[i] = Leg{ID: string(rune('a'+i%26)) + string(rune('0'+i/26)), AmountMinor: a}
	}
	return out
}

func TestSubsetSumTable(t *testing.T) {
	cases := []struct {
		name     string
		pool     []int64
		target   int64
		tol      int64
		min, max int
		wantSum  bool
	}{
		{"exact pair", []int64{100, 250, 400}, 650, 0, 2, 4, true},
		{"needs three", []int64{100, 250, 400}, 750, 0, 2, 4, true},
		{"within tolerance", []int64{100, 250, 400}, 652, 2, 2, 4, true},
		{"outside tolerance", []int64{100, 250, 400}, 653, 2, 2, 4, false},
		{"min size blocks singleton", []int64{650, 100}, 650, 0, 2, 4, false},
		{"max size blocks", []int64{1, 1, 1, 1}, 4, 0, 2, 3, false},
		{"empty pool", nil, 10, 0, 1, 3, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := subsetSum(legsFromAmounts(tc.pool), tc.target, tc.tol, tc.min, tc.max, nil)
			if ok != tc.wantSum {
				t.Fatalf("ok=%v want %v (%v)", ok, tc.wantSum, got)
			}
			if !ok {
				return
			}
			var sum int64
			for _, l := range got {
				sum += l.AmountMinor
			}
			if abs64(sum-tc.target) > tc.tol || len(got) < tc.min || len(got) > tc.max {
				t.Fatalf("bad subset %v sum=%d", got, sum)
			}
		})
	}
}

// Property: whenever a planted subset exists, the search finds *a* valid subset,
// and it never returns an invalid one.
func TestSubsetSumProperty(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	for iter := 0; iter < 500; iter++ {
		n := 2 + rng.Intn(20)
		amts := make([]int64, n)
		for i := range amts {
			amts[i] = 1 + rng.Int63n(50_000)
		}
		k := 2 + rng.Intn(4)
		if k > n {
			k = n
		}
		var target int64
		perm := rng.Perm(n)
		for _, i := range perm[:k] {
			target += amts[i]
		}
		tol := rng.Int63n(3)
		got, ok := subsetSum(legsFromAmounts(amts), target, tol, 2, 6, nil)
		if !ok {
			t.Fatalf("iter %d: planted subset of size %d not found (pool=%v target=%d)", iter, k, amts, target)
		}
		var sum int64
		for _, l := range got {
			sum += l.AmountMinor
		}
		if abs64(sum-target) > tol {
			t.Fatalf("iter %d: returned invalid subset sum %d target %d", iter, sum, target)
		}
	}
}

func FuzzSubsetSum(f *testing.F) {
	f.Add([]byte{10, 20, 30, 40}, uint16(60), uint8(0))
	f.Add([]byte{5, 5, 5, 5, 5}, uint16(15), uint8(1))
	f.Fuzz(func(t *testing.T, raw []byte, target uint16, tol uint8) {
		if len(raw) > 24 {
			raw = raw[:24]
		}
		amts := make([]int64, 0, len(raw))
		for _, b := range raw {
			if b == 0 {
				continue
			}
			amts = append(amts, int64(b))
		}
		got, ok := subsetSum(legsFromAmounts(amts), int64(target), int64(tol), 2, 6, nil)
		if !ok {
			return
		}
		var sum int64
		seen := map[string]bool{}
		for _, l := range got {
			sum += l.AmountMinor
			if seen[l.ID] {
				t.Fatalf("leg %s used twice", l.ID)
			}
			seen[l.ID] = true
		}
		if abs64(sum-int64(target)) > int64(tol) || len(got) < 2 || len(got) > 6 {
			t.Fatalf("invalid subset: sum=%d target=%d tol=%d size=%d", sum, target, tol, len(got))
		}
	})
}

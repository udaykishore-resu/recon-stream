# ADR 0002: Deterministic, versioned, ordered matching tiers

- Status: accepted
- Date: 2026-09-15

## Context

Auto-match rate is the headline KPI, but a false match is far more expensive
than a break: it hides a real discrepancy behind a green tick. Matching must be
predictable enough that an auditor can re-derive any match by hand, and stable
enough that the same inputs always give the same output.

## Decision

Tiers run in a fixed order per incoming leg; the first tier that yields a
candidate wins and the search stops. Each rule has an identifier and version
that is stored on the match (`rule_id`) so the rule text can be looked up here.

| Rule | Condition | Confidence |
| --- | --- | --- |
| `T1.exact@v1` | same `txn_ref`, same amount, same currency (window key), value date within ±N days | 1.00 |
| `T2.ref-tolerant@v1` | same `txn_ref`, amount within `max(bps, abs)` tolerance, date in window; smallest residual wins | 0.80–0.95 by residual |
| `T2.amount-date@v1` | different refs, exact amount, date in window, **exactly one** candidate on the other side and **no** same-amount open leg on ours, and neither ref already known on the other source | 0.75 |
| `T2.late-arrival@v1` | partner already expired into a single-leg break, amount within tolerance: heals the break | T2 score − 0.05 |
| `T3.subset-sum@v1` | one leg vs 2..`T3MaxSubset` opposite legs (same source) summing within tolerance; bounded DFS over at most `T3MaxCandidates` legs, 200k-node budget; both orientations | 0.70–0.85 by residual |

Tolerance is `max(round(amount × bps / 10 000), abs_minor)` (defaults 10 bps, 2
minor units). Windows are keyed by `(currency, counterparty)`; matching never
crosses a window, so currency mismatches always become breaks.

**T3 requires a batch hint by default** (`T3RequireBatchHint=true`): the N legs
must carry `attrs.batch_ref` equal to the settlement leg's `batch_ref` or
`txn_ref`. With ~24 candidates and a ±10 bps tolerance, unhinted subset sums of
4–6 legs hit the target by coincidence often enough to be dangerous. Unhinted
mode remains available for feeds that carry no batch identifiers, and is
tested, but it is an explicit opt-in.

Rule changes are made by adding a new version (`@v2`), never by editing a
version in place, so historical matches keep pointing at the rule that produced
them. Replaying the event log applies the current versions.

## Consequences

- The auto-match rate on the sample feed is 95.5% with zero false matches; the
  remaining legs become classified breaks rather than optimistic matches.
- Ref-less matching (`T2.amount-date`) is deliberately conservative and will
  leave legitimate pairs open when amounts collide; that is preferred to a
  wrong match and is visible as `missing_counterparty` breaks after the window
  closes.
- All candidate iteration is over sorted slices (value date, txn_ref, id) so
  ties resolve identically on every run.

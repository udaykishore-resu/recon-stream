# ADR 0004: Deterministic heuristic break classifier behind a pluggable interface

- Status: accepted
- Date: 2026-09-15

## Context

The spec calls for an "ML slot" that labels breaks (fee, FX drift, timing,
duplicate, missing counterparty). Operations teams need those labels to route
work, but a regulator needs to know *why* a break was labelled, and a label
must never change whether two legs match.

## Decision

- `recon.BreakClassifier` is the only extension point:
  `Classify(ClassifyInput) Classification`. The input carries the break's legs,
  every persisted leg sharing a `txn_ref` (`Related`), the trigger and the
  active rules. The output carries category, confidence, `classifier_id@version`
  and a one-line evidence string that is stored on the break as `reason` until
  an operator overwrites it on resolution.
- The classifier is **advisory**: the engine decides match-vs-break
  deterministically first, then asks the classifier only for the label.
  Swapping in a learned model cannot alter matching outcomes.
- The shipped `heuristic@v1` applies ordered rules: same-source ref reuse →
  `duplicate`; currency mismatch or FX attributes → `fx_drift`; amounts agree
  but value dates outside the window → `timing`; fee attributes or a difference
  inside a 20–400 bps band → `fee`; a lone expired leg → `missing_counterparty`;
  otherwise `unclassified` with low confidence. Confidence is a fixed score per
  rule, shaded by how central the evidence is (fee band peaks at 1.5%).

## Alternatives considered

- Training a classifier now: no labelled corpus exists on day one; the
  heuristic produces the labelled history a model would be trained on.
- Letting the classifier influence matching (e.g. auto-matching "fee" breaks):
  rejected; a fee is a *reconciling item*, and booking it is an accounting
  decision that belongs to the resolver, recorded via `POST /v1/breaks/{id}/resolve`.

## Consequences

- Every break is explainable from its `reason` and `classifier_id`.
- A model-backed classifier can be introduced as `model@v1` behind the same
  interface, with the heuristic kept as a fallback when the model's confidence
  is low.

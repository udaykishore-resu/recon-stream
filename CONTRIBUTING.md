# Contributing

Thanks for taking the time. This project values small, explicit, well-tested
changes over clever ones.

## Workflow

1. Open an issue or a short design note for anything beyond a bug fix. Matching
   rule changes need a new rule version (`T2.ref-tolerant@v2`), never an edit
   to an existing one, and an ADR update.
2. Branch from `main`, keep commits focused, write commit messages in the
   imperative ("Add T2 late-arrival rule").
3. Before pushing:

   ```sh
   make tidy vet test lint
   make gen && git diff --exit-code examples/legs.json   # sample must stay reproducible
   ```

4. Pull requests need green CI and one approving review from a code owner.

## Ground rules

- Domain packages (`internal/domain/...`) do no I/O and stay above 70 percent
  coverage. Every matching or classification behaviour gets a table-driven
  test; anything algorithmic gets a property or fuzz test.
- Decisions must be auditable: matches record `rule_id@version`, breaks record
  `classifier_id@version` and a one-line evidence string.
- Prefer the standard library. New third-party dependencies need a sentence of
  justification in the PR.
- Public behaviour changes update `api/openapi.yaml`, the README configuration
  table and, if operational, `docs/runbook.md`.

## Running locally

```sh
make run                    # in-memory, no dependencies
make run-full               # Kafka + Postgres + OTel collector via docker compose
RECON_URL=http://localhost:8080 make demo
```

# Security policy

## Supported versions

Only the latest minor release on `main` receives security fixes.

## Reporting a vulnerability

Please do not open public issues for security problems. Use GitHub's private
vulnerability reporting on this repository ("Security" tab, "Report a
vulnerability"). You will receive an acknowledgement within 3 business days and
a remediation plan or fix within 30 days for confirmed issues.

## Design notes relevant to security

- The service has no authentication layer of its own; deploy it behind a
  mesh/gateway that terminates TLS and enforces identity. The Helm chart ships a
  default-deny NetworkPolicy and a read-only, non-root container.
- Secrets (Postgres DSN) are never rendered by the chart; they are read from an
  externally managed Secret.
- Request bodies are capped at 16 MiB and 5000 legs; all inputs are validated
  before any state changes.
- The event log is append-only. `POST /v1/replay` and `POST /v1/windows/close`
  are operational endpoints and should be restricted at the gateway.
- Dependencies are scanned with `govulncheck` in CI.

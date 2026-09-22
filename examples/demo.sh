#!/usr/bin/env bash
# End-to-end demo against a running recon-stream (default: make run on :8080).
#   RECON_URL=http://localhost:8080 ./examples/demo.sh
# Requires curl and python3. Exits non-zero if the auto-match rate is < 95%.
set -euo pipefail

RECON_URL="${RECON_URL:-http://localhost:8080}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LEGS="$HERE/legs.json"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

say() { printf '\n\033[1m== %s\033[0m\n' "$*"; }
get()  { curl -sS "$RECON_URL$1" -o "$TMP/$2"; }
post() { curl -sS -X POST "$RECON_URL$1" -H 'Content-Type: application/json' ${3:+--data-binary "$3"} -o "$TMP/$2"; }

say "1. Ingest 200 legs (105 ledger postings + 95 card-network settlement records)"
post /v1/legs ingest.json "@$LEGS"
python3 - "$TMP/ingest.json" <<'PY'
import json, sys
d = json.load(open(sys.argv[1]))
print(f"accepted={d['accepted']} matched={d['matched']} breaks={d['breaks']} open={d['open']} duplicates={d['duplicates']}")
PY

say "2. Close the recon window (EOD cut-off): unmatched legs become breaks"
post /v1/windows/close close.json
python3 - "$TMP/close.json" <<'PY'
import json, sys
print(f"expired={json.load(open(sys.argv[1]))['expired']}")
PY

say "3. Stats: auto-match rate and match/break distribution"
get /v1/stats stats.json
python3 - "$TMP/stats.json" <<'PY'
import json, sys
s = json.load(open(sys.argv[1]))["stats"]
print(f"legs_total={s['legs_total']} matched={s['legs_matched']} in_break={s['legs_in_break']} open={s['legs_open']}")
print(f"auto_match_rate={s['auto_match_rate']:.1%}")
print("matches_by_rule:", json.dumps(s["matches_by_rule"], sort_keys=True))
print("breaks_open_by_category:", json.dumps(s["breaks_open_by_category"], sort_keys=True))
PY

say "4. Open breaks, oldest first"
get "/v1/breaks?status=open&limit=50" breaks.json
python3 - "$TMP/breaks.json" <<'PY'
import json, sys
for b in json.load(open(sys.argv[1]))["breaks"]:
    print(f"{b['id']}  {b['category']:<21} conf={b['confidence']:.2f} {b['currency']}/{b['counterparty']:<10} trigger={b['trigger']:<15} legs={len(b['leg_ids'])}")
    print(f"    {b['reason']}")
PY

say "5. Resolve the fee break (or the oldest open break) with an audited reason + actor"
FEE_ID=$(python3 - "$TMP/breaks.json" <<'PY'
import json, sys
breaks = json.load(open(sys.argv[1]))["breaks"]
print(next((b["id"] for b in breaks if b["category"] == "fee"), breaks[0]["id"] if breaks else ""))
PY
)
post "/v1/breaks/$FEE_ID/resolve" resolve.json '{"reason":"interchange netted by scheme; fee booked to 4210-INTERCHANGE","actor":"ops@example.bank"}'
python3 - "$TMP/resolve.json" <<'PY'
import json, sys
d = json.load(open(sys.argv[1]))
print(f"{d['id']} status={d['status']} actor={d['actor']} resolved_at={d['resolved_at']}")
PY

say "6. Inspect one T3 many-to-one match (batch settlement vs ledger postings)"
get "/v1/events?from=1&limit=1000" events.json
MATCH_ID=$(python3 - "$TMP/events.json" <<'PY'
import json, sys
evs = json.load(open(sys.argv[1]))["events"]
print(next(e["aggregate_id"] for e in evs if e["type"] == "match.created" and e["payload"]["rule_id"].startswith("T3")))
PY
)
get "/v1/matches/$MATCH_ID" match.json
python3 - "$TMP/match.json" <<'PY'
import json, sys
d = json.load(open(sys.argv[1])); m = d["match"]
print(f"{m['id']} rule={m['rule_id']} confidence={m['confidence']} residual={m['residual_minor']} legs={len(m['leg_ids'])}")
for l in d["legs"]:
    print(f"    {l['source']:<13} {l['txn_ref']:<28} {l['amount_minor']:>8} {l['currency']} {l['value_date']}")
PY

say "7. Audit trail: the break.resolved event"
get "/v1/events?from=1&limit=1000" events.json
python3 - "$TMP/events.json" <<'PY'
import json, sys
ev = [e for e in json.load(open(sys.argv[1]))["events"] if e["type"] == "break.resolved"][-1]
print(f"seq={ev['seq']} type={ev['type']} aggregate={ev['aggregate_id']} payload={json.dumps(ev['payload'])}")
PY

say "8. Replay the event log from seq 1 (rebuilds legs/matches/breaks deterministically)"
post "/v1/replay?from=1" replay.json
python3 - "$TMP/replay.json" <<'PY'
import json, sys
d = json.load(open(sys.argv[1]))
print(f"events_read={d['events_read']} legs_replayed={d['legs_replayed']} matched={d['matched']} breaks={d['breaks']} open={d['open']}")
PY

python3 - "$TMP/stats.json" <<'PY'
import json, sys
rate = json.load(open(sys.argv[1]))["stats"]["auto_match_rate"]
print(f"\nauto-match rate {rate:.1%} -> {'OK (>= 95%)' if rate >= 0.95 else 'FAIL (< 95%)'}")
sys.exit(0 if rate >= 0.95 else 1)
PY

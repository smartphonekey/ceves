#!/usr/bin/env bash
# End-to-end exercise of the BankAccount example against the NATS runtime.
# Requires: the app running (npm run nats:start) and curl + node on PATH.
set -euo pipefail

BASE="${BASE:-http://localhost:8788}"
ORG='X-Org-Id: acme'
ACC="${ACC:-acc-$(date +%s)}"
pass=0; fail=0

check() { # check <name> <expected> <actual>
  if [ "$2" = "$3" ]; then pass=$((pass+1)); echo "  ok: $1"; else fail=$((fail+1)); echo "  FAIL: $1 — expected [$2] got [$3]"; fi
}

json() { node -e "const d=JSON.parse(require('fs').readFileSync(0,'utf8')); console.log(eval('d.'+process.argv[1]) ?? 'null')" "$1"; }

echo "== account: $ACC =="

echo "-- OpenAccount (create → 201, event in response)"
r=$(curl -s -w '\n%{http_code}' -X POST "$BASE/accounts/$ACC/OpenAccount" -H "$ORG" -H 'Content-Type: application/json' -d '{"owner":"Ada Lovelace","initialDeposit":100}')
code=${r##*$'\n'}; body=${r%$'\n'*}
check "status 201" 201 "$code"
check "event type" "AccountOpened" "$(echo "$body" | json 'event.type')"
check "version 1" 1 "$(echo "$body" | json 'version')"

echo "-- duplicate OpenAccount (idempotent no-op → 200, event null)"
r=$(curl -s -w '\n%{http_code}' -X POST "$BASE/accounts/$ACC/OpenAccount" -H "$ORG" -H 'Content-Type: application/json' -d '{"owner":"Eve","initialDeposit":9999}')
code=${r##*$'\n'}; body=${r%$'\n'*}
check "status 200" 200 "$code"
check "event null" "null" "$(echo "$body" | json 'event')"

echo "-- Deposit 50 → version 2"
r=$(curl -s -w '\n%{http_code}' -X POST "$BASE/accounts/$ACC/Deposit" -H "$ORG" -H 'Content-Type: application/json' -d '{"amount":50}')
code=${r##*$'\n'}; body=${r%$'\n'*}
check "status 200" 200 "$code"
check "version 2" 2 "$(echo "$body" | json 'version')"

echo "-- Withdraw 30 → version 3"
r=$(curl -s -w '\n%{http_code}' -X POST "$BASE/accounts/$ACC/Withdraw" -H "$ORG" -H 'Content-Type: application/json' -d '{"amount":30}')
code=${r##*$'\n'}; body=${r%$'\n'*}
check "status 200" 200 "$code"

echo "-- balance (public route, no auth) → 120"
r=$(curl -s -w '\n%{http_code}' "$BASE/accounts/$ACC/balance")
code=${r##*$'\n'}; body=${r%$'\n'*}
check "status 200" 200 "$code"
check "balance 120" 120 "$(echo "$body" | json 'balance')"
check "owner" "Ada Lovelace" "$(echo "$body" | json 'owner')"
check "version 3" 3 "$(echo "$body" | json 'version')"

echo "-- command without X-Org-Id → 401"
code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$BASE/accounts/$ACC/Deposit" -H 'Content-Type: application/json' -d '{"amount":1}')
check "status 401" 401 "$code"

echo "-- command from another org → 401"
code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$BASE/accounts/$ACC/Deposit" -H 'X-Org-Id: rival' -H 'Content-Type: application/json' -d '{"amount":1}')
check "status 401" 401 "$code"

echo "-- overdraft: Withdraw 1000 → 4xx, balance unchanged"
code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$BASE/accounts/$ACC/Withdraw" -H "$ORG" -H 'Content-Type: application/json' -d '{"amount":1000}')
case "$code" in 4*) check "status 4xx" "$code" "$code";; *) check "status 4xx" "4xx" "$code";; esac
bal=$(curl -s "$BASE/accounts/$ACC/balance" | json 'balance')
check "balance still 120" 120 "$bal"

echo "-- update command on unknown account → 404"
code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$BASE/accounts/never-opened/Deposit" -H "$ORG" -H 'Content-Type: application/json' -d '{"amount":1}')
check "status 404" 404 "$code"

echo "-- invalid body (negative deposit? missing field) → 400"
code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$BASE/accounts/$ACC/Deposit" -H "$ORG" -H 'Content-Type: application/json' -d '{}')
check "status 400" 400 "$code"

echo
echo "PASS=$pass FAIL=$fail"
[ "$fail" -eq 0 ]

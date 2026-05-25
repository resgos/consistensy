#!/usr/bin/env bash
# Diverges cluster-2 on a single REGISTER record so that the next consistency
# run reports a MISMATCH for it.
set -euo pipefail

PY="${PY:-python3}"

$PY - <<'PY'
from pyignite import Client
client = Client()
client.connect("127.0.0.1", 10802)  # cluster-2

# Change currency for R001 only on cluster-2 — the others still say RUB.
client.sql("UPDATE REGISTER SET CURRENCY = 'EUR' WHERE OBJECTID = 'R001'")
client.close()
print("cluster-2 diverged: R001 currency -> EUR")
PY

echo "Now trigger: curl -X POST http://localhost:8080/api/consistency/run"
echo "Expect 1 mismatch on REGISTER, businessKey=R001"

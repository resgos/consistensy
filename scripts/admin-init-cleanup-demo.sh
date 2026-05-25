#!/usr/bin/env bash
# End-to-end demo of admin fan-out:
#   1. seed 3 clusters with identical past data
#   2. cleanup old data (before today - 100 days)
#   3. init register for last 180 days with opening balance
#   4. run consistency check — expect OK
#   5. diverge one cluster
#   6. consistency check — expect MISMATCH on the diverged record
#
# Prereqs:  docker compose up -d   AND   pip install pyignite requests
set -euo pipefail

PY=${PY:-python3}
BASE=${BASE:-http://localhost:8080}

echo "============================================================"
echo "Step 1: seed all 3 clusters with 200 days of data for R001"
echo "============================================================"
$PY - <<'PY'
from pyignite import Client
from datetime import date, timedelta
for port in (10801, 10802, 10803):
    c = Client(); c.connect("127.0.0.1", port)
    c.sql("""CREATE TABLE IF NOT EXISTS REGISTER (
                OBJECTID VARCHAR PRIMARY KEY, CCRQTM TIMESTAMP,
                CCOPENDATE DATE, CCCLOSEDATE DATE,
                CURRENCY VARCHAR, CCBALANCERECALCDATE DATE
             ) WITH "CACHE_NAME=REGISTER, VALUE_TYPE=Register" """)
    c.sql("DELETE FROM REGISTER")
    c.sql("""INSERT INTO REGISTER(OBJECTID,CCOPENDATE,CURRENCY,CCBALANCERECALCDATE)
             VALUES ('R001', DATE '2020-01-01', 'RUB', DATE '2026-05-24')""")
    today = date.today()
    c.sql("""CREATE TABLE IF NOT EXISTS TURNDOCCUR (
                OBJECTID VARCHAR PRIMARY KEY, REGISTER VARCHAR, CCTYPEOPER DECIMAL(3),
                CCOPERATIONDAY DATE, CCDT VARCHAR(1), CCSUM DECIMAL(20,6),
                CCSUMNAT DECIMAL(20,6), CCIDEKS VARCHAR, CCDATE TIMESTAMP,
                CCRQUID VARCHAR, CCTRANSACTIONID VARCHAR, EXPROP5 VARCHAR
             ) WITH "CACHE_NAME=TURN_DOC_CUR, VALUE_TYPE=TurnDocCur, AFFINITY_KEY=REGISTER" """)
    c.sql("DELETE FROM TURNDOCCUR")
    for i in range(200):
        d = today - timedelta(days=200 - i)
        # one DT and one KT op per day
        c.sql("INSERT INTO TURNDOCCUR(OBJECTID,REGISTER,CCTYPEOPER,CCOPERATIONDAY,CCDT,CCSUM,CCSUMNAT,CCIDEKS,CCDATE,CCRQUID,CCTRANSACTIONID,EXPROP5) "
              "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
              query_args=[f"T{i}D", "R001", 0, d, "1", 50.0, 50.0, f"ID{i}D",
                          d.isoformat()+" 10:00:00", f"R{i}D", f"TX{i}D", None])
        c.sql("INSERT INTO TURNDOCCUR(OBJECTID,REGISTER,CCTYPEOPER,CCOPERATIONDAY,CCDT,CCSUM,CCSUMNAT,CCIDEKS,CCDATE,CCRQUID,CCTRANSACTIONID,EXPROP5) "
              "VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
              query_args=[f"T{i}K", "R001", 0, d, "0", 30.0, 30.0, f"ID{i}K",
                          d.isoformat()+" 11:00:00", f"R{i}K", f"TX{i}K", None])
    c.close()
    print(f"  cluster on :{port}  seeded 200 days")
PY

echo ""
echo "============================================================"
echo "Step 2: cleanup history older than 100 days on all clusters"
echo "============================================================"
CUTOFF=$(date -d '100 days ago' +%Y-%m-%d 2>/dev/null || date -v-100d +%Y-%m-%d)
curl -sS -X POST $BASE/api/admin/cleanup \
     -H 'Content-Type: application/json' \
     -d "{\"beforeDate\":\"$CUTOFF\"}"
echo ""

echo ""
echo "============================================================"
echo "Step 3: init R001 for last 180 days with opening balance 5000"
echo "============================================================"
INIT_FROM=$(date -d '180 days ago' +%Y-%m-%d 2>/dev/null || date -v-180d +%Y-%m-%d)
INIT_TO=$(date -d '1 day ago' +%Y-%m-%d 2>/dev/null || date -v-1d +%Y-%m-%d)
curl -sS -X POST $BASE/api/admin/init \
     -H 'Content-Type: application/json' \
     -d "{\"registerId\":\"R001\",\"fromDate\":\"$INIT_FROM\",\"toDate\":\"$INIT_TO\",\"openingBalance\":5000.00}"
echo ""

echo ""
echo "============================================================"
echo "Step 4: consistency run — expect status=OK"
echo "============================================================"
curl -sS -X POST $BASE/api/consistency/run
echo ""

echo ""
echo "============================================================"
echo "Step 5: diverge cluster-2 — flip CURRENCY for R001 to EUR"
echo "============================================================"
$PY - <<'PY'
from pyignite import Client
c = Client(); c.connect("127.0.0.1", 10802)
c.sql("UPDATE REGISTER SET CURRENCY='EUR' WHERE OBJECTID='R001'")
c.close()
print("  cluster-2 diverged")
PY

echo ""
echo "============================================================"
echo "Step 6: consistency run — expect status=MISMATCH"
echo "============================================================"
curl -sS -X POST $BASE/api/consistency/run
echo ""
curl -sS "$BASE/api/consistency/mismatches?unresolvedOnly=true"
echo ""

echo ""
echo "Done. Workflow demonstrated:"
echo "  - admin fan-out for cleanup/init across 3 clusters"
echo "  - consistency detect changes between clusters"

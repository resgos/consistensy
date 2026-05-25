#!/usr/bin/env bash
# Populates 3 Ignite clusters with identical seed data via SQL over JDBC thin-client.
# Requires `ignitesqlline` or just plain `psql`-style SQL over JDBC; here we use
# python + pyignite for simplicity.
#
# Usage: ./scripts/seed-clusters.sh
set -euo pipefail

PY="${PY:-python3}"

for port in 10801 10802 10803; do
    echo "=== Seeding cluster on port $port ==="
    $PY - <<PY
from pyignite import Client
client = Client()
client.connect("127.0.0.1", $port)

# REGISTER
client.sql("""
CREATE TABLE IF NOT EXISTS REGISTER (
    OBJECTID VARCHAR PRIMARY KEY,
    CCRQTM TIMESTAMP,
    CCOPENDATE DATE,
    CCCLOSEDATE DATE,
    CURRENCY VARCHAR,
    CCBALANCERECALCDATE DATE
) WITH "CACHE_NAME=REGISTER, VALUE_TYPE=Register"
""")
client.sql("DELETE FROM REGISTER")
client.sql("""INSERT INTO REGISTER (OBJECTID, CCRQTM, CCOPENDATE, CURRENCY, CCBALANCERECALCDATE)
              VALUES ('R001', TIMESTAMP '2026-01-15 12:00:00', DATE '2020-01-01', 'RUB', DATE '2026-05-24')""")
client.sql("""INSERT INTO REGISTER (OBJECTID, CCRQTM, CCOPENDATE, CURRENCY, CCBALANCERECALCDATE)
              VALUES ('R002', TIMESTAMP '2026-01-16 12:00:00', DATE '2021-06-01', 'USD', DATE '2026-05-24')""")

# DAY_BALANCES
client.sql("""
CREATE TABLE IF NOT EXISTS DAYBALANCES (
    REGISTER VARCHAR,
    CCOPERATIONDAY DATE,
    CCSTARTSUM DECIMAL(20,6),
    CCSTARTSUMNAT DECIMAL(20,6),
    CCDTSUM DECIMAL(20,6),
    CCDTSUMNAT DECIMAL(20,6),
    CCKTSUM DECIMAL(20,6),
    CCKTSUMNAT DECIMAL(20,6),
    CCDTCOUNT INT,
    CCKTCOUNT INT,
    CCFINISHSUM DECIMAL(20,6),
    CCFINISHSUMNAT DECIMAL(20,6),
    PRIMARY KEY (REGISTER, CCOPERATIONDAY)
) WITH "CACHE_NAME=DAY_BALANCES, VALUE_TYPE=DayBalances"
""")
client.sql("DELETE FROM DAYBALANCES")
client.sql("""INSERT INTO DAYBALANCES VALUES
              ('R001', DATE '2026-05-24', 1000.00, 1000.00, 200.00, 200.00, 500.00, 500.00, 3, 2, 1300.00, 1300.00)""")
client.sql("""INSERT INTO DAYBALANCES VALUES
              ('R002', DATE '2026-05-24', 500.00, 45000.00, 100.00, 9000.00, 0.00, 0.00, 1, 0, 400.00, 36000.00)""")

client.close()
print("Seeded port $port OK")
PY
done

echo "Done. Trigger a run: curl -X POST http://localhost:8080/api/consistency/run"

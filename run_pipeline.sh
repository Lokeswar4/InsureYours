#!/bin/bash
# Run the full InsureYours pipeline (non-Docker).
# Usage: ./run_pipeline.sh [mysql_host] [mysql_user] [mysql_password]

set -euo pipefail

HOST="${1:-localhost}"
USER="${2:-root}"
PASS="${3:-}"
DB="Healthcare_Group_Project"
DIR="$(cd "$(dirname "$0")" && pwd)"

echo "============================================"
echo "  InsureYours Pipeline"
echo "  Host: $HOST | User: $USER"
echo "============================================"

echo ""
echo "[1/5] Creating database and tables..."
mysql -h "$HOST" -u "$USER" ${PASS:+-p"$PASS"} < "$DIR/Tables_Creation.sql"

echo "[2/5] Running ETL (CSV → MySQL)..."
python3 "$DIR/etl_load.py" --host "$HOST" --user "$USER" --password "${PASS:-}"

echo "[3/5] Creating views and stored procedures..."
mysql -h "$HOST" -u "$USER" ${PASS:+-p"$PASS"} "$DB" < "$DIR/StoredProcedure.sql"

echo "[4/5] Running insurance ranking analysis..."
mysql -h "$HOST" -u "$USER" ${PASS:+-p"$PASS"} "$DB" < "$DIR/Analysis.sql"

echo "[5/5] Running advanced analytics..."
mysql -h "$HOST" -u "$USER" ${PASS:+-p"$PASS"} "$DB" < "$DIR/Advanced_Analysis.sql"

echo ""
echo "============================================"
echo "  Pipeline complete!"
echo "============================================"
echo ""
echo "Query the results:"
echo "  mysql -h $HOST -u $USER ${PASS:+-p***} $DB"
echo ""
echo "  SELECT * FROM Health_Data_Analysis ORDER BY AgeGroup, MedicalCondition LIMIT 10;"
echo "  CALL Avg_BillingAmount_Per_AgeGroup_MedicalCondition();"
echo "  CALL Avg_Cost_Per_Condition_For_Insurance();"
echo "  CALL Avg_LengthOfStay_Per_Condition_For_Insurance();"

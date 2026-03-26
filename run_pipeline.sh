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
mysql -h "$HOST" -u "$USER" ${PASS:+-p"$PASS"} < "$DIR/sql/01_schema.sql"

echo "[2/5] Running ETL (CSV → MySQL)..."
python3 "$DIR/src/insureyours/etl_load.py" --host "$HOST" --user "$USER" --password "${PASS:-}"

echo "[3/5] Creating views and stored procedures..."
mysql -h "$HOST" -u "$USER" ${PASS:+-p"$PASS"} "$DB" < "$DIR/sql/02_procedures.sql"

echo "[4/5] Running insurance ranking analysis..."
mysql -h "$HOST" -u "$USER" ${PASS:+-p"$PASS"} "$DB" < "$DIR/sql/03_analysis.sql"

echo "[5/5] Running advanced analytics..."
mysql -h "$HOST" -u "$USER" ${PASS:+-p"$PASS"} "$DB" < "$DIR/sql/04_advanced.sql"

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

#!/bin/bash
# Runs stored procedures and analysis queries after ETL completes.
# Uses MYSQL_PWD env var for password (set by docker-compose).

set -euo pipefail

MYSQL_CMD="mysql -h mysql -u root Healthcare_Group_Project"

echo "Creating views and stored procedures..."
$MYSQL_CMD < /scripts/StoredProcedure.sql

echo "Running insurance ranking analysis..."
$MYSQL_CMD < /scripts/Analysis.sql

echo "Running advanced analytics..."
$MYSQL_CMD < /scripts/Advanced_Analysis.sql

echo ""
echo "=== Top 10 Insurance Recommendations ==="
$MYSQL_CMD -e "
SELECT
    AgeGroup,
    BloodType,
    MedicalCondition,
    Medication,
    InsuranceProvider,
    AvgBillingAmount,
    ClaimCount
FROM Health_Data_Analysis
ORDER BY AgeGroup, MedicalCondition
LIMIT 10;
"

echo ""
echo "=== Pipeline Complete ==="
echo "  Database:  mysql -h 127.0.0.1 -u root Healthcare_Group_Project"
echo "  API:       http://localhost:8000/docs"

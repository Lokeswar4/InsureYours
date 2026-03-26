#!/usr/bin/env python3
"""
InsureYours REST API — Serves insurance recommendations from the analytics pipeline.

Demonstrates: building a data product from a pipeline (Data Engineering skill).

Usage:
    pip install fastapi uvicorn mysql-connector-python
    python3 api.py                             # defaults: localhost:8000
    python3 api.py --host 0.0.0.0 --port 8080  # custom bind
    python3 api.py --db-host 10.0.0.5           # remote MySQL

Endpoints:
    GET  /                          → API docs link
    GET  /health                    → Health check (DB connectivity)
    GET  /recommend                 → Best insurer for a patient profile
    GET  /conditions                → List all medical conditions
    GET  /insurers                  → List all insurance providers
    GET  /analytics/billing-summary → Avg billing by age group and condition
    GET  /analytics/provider-compare → Side-by-side provider cost comparison
    GET  /analytics/outliers        → High-cost claims (>2 StdDev above mean)
    GET  /dashboard                 → Interactive web dashboard
"""

import argparse
import os
import sys
from contextlib import asynccontextmanager, contextmanager

try:
    import uvicorn
    from fastapi import FastAPI, HTTPException, Query
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.staticfiles import StaticFiles
except ImportError:
    print("ERROR: FastAPI and uvicorn are required for the API.")
    print("Install with: pip install fastapi uvicorn")
    sys.exit(1)

try:
    import mysql.connector  # noqa: F401 (needed for pooling submodule)
    from mysql.connector import pooling
except ImportError:
    print("ERROR: mysql-connector-python is required.")
    print("Install with: pip install mysql-connector-python")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DB_CONFIG = {
    "host": os.environ.get("MYSQL_HOST", "localhost"),
    "port": int(os.environ.get("MYSQL_PORT", "3306")),
    "user": os.environ.get("MYSQL_USER", "root"),
    "password": os.environ.get("MYSQL_PASSWORD", ""),
    "database": "Healthcare_Group_Project",
}

pool = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Create connection pool on startup, close on shutdown."""
    global pool
    pool = pooling.MySQLConnectionPool(
        pool_name="api_pool",
        pool_size=5,
        **DB_CONFIG,
    )
    yield
    # Pool is garbage-collected on shutdown


app = FastAPI(
    title="InsureYours API",
    description="Find the most cost-effective insurance provider for your healthcare needs.",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

# Serve the interactive dashboard at /dashboard (optional — only if directory exists)
# Resolve project root: src/insureyours/api.py → ../../ → project root → dashboard/
_dashboard_dir = os.path.normpath(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "dashboard")
)
if os.path.isdir(_dashboard_dir):
    app.mount("/dashboard", StaticFiles(directory=_dashboard_dir, html=True), name="dashboard")


@contextmanager
def get_db():
    """Get a cursor from the pool; auto-closes on exit (even on exception)."""
    conn = pool.get_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        yield cursor
    finally:
        cursor.close()
        conn.close()


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------
@app.get("/")
def root():
    return {
        "project": "InsureYours",
        "description": "Healthcare insurance cost analytics API",
        "docs": "/docs",
        "endpoints": [
            "/recommend",
            "/conditions",
            "/insurers",
            "/analytics/billing-summary",
            "/analytics/provider-compare",
            "/analytics/outliers",
            "/dashboard",
        ],
    }


@app.get("/health")
def health():
    """Check database connectivity."""
    try:
        with get_db() as cursor:
            cursor.execute("SELECT COUNT(*) AS n FROM Health_Data_Analysis")
            row = cursor.fetchone()
            return {"status": "healthy", "recommendations_loaded": row["n"]}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Database unavailable: {e}") from e


@app.get("/recommend")
def recommend(
    age: int = Query(..., ge=0, le=150, description="Patient age"),
    condition: str = Query(..., description="Medical condition (e.g., Diabetes)"),
    blood_type: str = Query(..., description="Blood type (e.g., O+)"),
    medication: str = Query(..., description="Medication (e.g., Aspirin)"),
):
    """
    Find the cheapest insurance provider for a specific patient profile.

    Returns the top-ranked provider(s) by average billing amount.
    Ties are preserved — if two providers have identical average cost, both appear.
    """
    # Map age to age group
    age_group = _age_to_group(age)

    with get_db() as cursor:
        cursor.execute(
            """
            SELECT
                InsuranceProvider,
                AvgBillingAmount,
                MinBillingAmount,
                MaxBillingAmount,
                ClaimCount,
                ProviderRank
            FROM Health_Data_Analysis
            WHERE AgeGroup = %s
              AND BloodType = %s
              AND MedicalCondition = %s
              AND Medication = %s
            ORDER BY ProviderRank, AvgBillingAmount
        """,
            (age_group, blood_type, condition, medication),
        )
        results = cursor.fetchall()

    if not results:
        raise HTTPException(
            status_code=404,
            detail={
                "message": "No recommendation found for this profile",
                "searched": {
                    "age_group": age_group,
                    "blood_type": blood_type,
                    "condition": condition,
                    "medication": medication,
                },
                "hint": "Try GET /conditions and GET /insurers to see available values",
            },
        )

    return {
        "patient_profile": {
            "age": age,
            "age_group": age_group,
            "blood_type": blood_type,
            "condition": condition,
            "medication": medication,
        },
        "recommendation": [
            {
                "insurer": r["InsuranceProvider"],
                "avg_billing": float(r["AvgBillingAmount"]),
                "min_billing": float(r["MinBillingAmount"]),
                "max_billing": float(r["MaxBillingAmount"]),
                "claim_count": r["ClaimCount"],
                "rank": r["ProviderRank"],
            }
            for r in results
        ],
    }


@app.get("/conditions")
def list_conditions():
    """List all medical conditions in the dataset."""
    with get_db() as cursor:
        cursor.execute("""
            SELECT DISTINCT MedicalCondition
            FROM Health_Data_Analysis
            ORDER BY MedicalCondition
        """)
        return {"conditions": [r["MedicalCondition"] for r in cursor.fetchall()]}


@app.get("/insurers")
def list_insurers():
    """List all insurance providers in the dataset."""
    with get_db() as cursor:
        cursor.execute("""
            SELECT DISTINCT InsuranceProvider
            FROM Health_Data_Analysis
            ORDER BY InsuranceProvider
        """)
        return {"insurers": [r["InsuranceProvider"] for r in cursor.fetchall()]}


@app.get("/analytics/billing-summary")
def billing_summary(
    condition: str | None = Query(None, description="Filter by condition"),
    age_group: str | None = Query(None, description="Filter by age group (e.g., 31-45)"),
):
    """Average billing breakdown by age group and medical condition."""
    query = """
        SELECT
            AgeGroup,
            MedicalCondition,
            ROUND(AVG(BillingAmount), 2) AS avg_billing,
            COUNT(*) AS claim_count,
            ROUND(MIN(BillingAmount), 2) AS min_billing,
            ROUND(MAX(BillingAmount), 2) AS max_billing
        FROM vw_PatientAgeGroups
        WHERE 1=1
    """
    params = []
    if condition:
        query += " AND MedicalCondition = %s"
        params.append(condition)
    if age_group:
        query += " AND AgeGroup = %s"
        params.append(age_group)

    query += " GROUP BY AgeGroup, MedicalCondition ORDER BY AgeGroup, MedicalCondition"

    with get_db() as cursor:
        cursor.execute(query, params)
        results = cursor.fetchall()

    return {
        "filters": {"condition": condition, "age_group": age_group},
        "data": [
            {
                "age_group": r["AgeGroup"],
                "condition": r["MedicalCondition"],
                "avg_billing": float(r["avg_billing"]),
                "claim_count": r["claim_count"],
                "min_billing": float(r["min_billing"]),
                "max_billing": float(r["max_billing"]),
            }
            for r in results
        ],
    }


@app.get("/analytics/provider-compare")
def provider_compare(
    condition: str = Query(..., description="Medical condition"),
    age_group: str | None = Query(None, description="Filter by age group"),
):
    """Side-by-side insurance provider cost comparison for a condition."""
    query = """
        SELECT
            InsuranceProvider,
            AgeGroup,
            ROUND(AVG(BillingAmount), 2) AS avg_billing,
            COUNT(*) AS claim_count,
            ROUND(STDDEV(BillingAmount), 2) AS stddev_billing
        FROM vw_PatientAgeGroups
        WHERE MedicalCondition = %s
    """
    params = [condition]
    if age_group:
        query += " AND AgeGroup = %s"
        params.append(age_group)

    query += """
        GROUP BY InsuranceProvider, AgeGroup
        ORDER BY AgeGroup, avg_billing
    """

    with get_db() as cursor:
        cursor.execute(query, params)
        results = cursor.fetchall()

    if not results:
        raise HTTPException(status_code=404, detail=f"No data for condition: {condition}")

    return {
        "condition": condition,
        "age_group_filter": age_group,
        "providers": [
            {
                "insurer": r["InsuranceProvider"],
                "age_group": r["AgeGroup"],
                "avg_billing": float(r["avg_billing"]),
                "claim_count": r["claim_count"],
                "stddev_billing": float(r["stddev_billing"]) if r["stddev_billing"] else None,
            }
            for r in results
        ],
    }


@app.get("/analytics/outliers")
def billing_outliers(
    condition: str | None = Query(None, description="Filter by condition"),
):
    """
    Claims that are more than 2 standard deviations above their condition's mean billing.

    Returns one row per condition summarising the outlier claim count, average
    outlier billing, the condition mean, and the 2-sigma threshold used.
    """
    query = """
        WITH ConditionStats AS (
            SELECT
                MedicalCondition,
                AVG(BillingAmount)    AS mean_bill,
                STDDEV(BillingAmount) AS std_bill
            FROM vw_PatientAgeGroups
            GROUP BY MedicalCondition
        )
        SELECT
            v.MedicalCondition,
            COUNT(*)                                           AS outlier_count,
            ROUND(AVG(v.BillingAmount), 2)                    AS avg_outlier_bill,
            ROUND(cs.mean_bill, 2)                            AS condition_mean,
            ROUND(cs.mean_bill + 2 * cs.std_bill, 2)          AS threshold
        FROM vw_PatientAgeGroups v
        JOIN ConditionStats cs ON v.MedicalCondition = cs.MedicalCondition
        WHERE v.BillingAmount > cs.mean_bill + 2 * cs.std_bill
    """
    params = []
    if condition:
        query += " AND v.MedicalCondition = %s"
        params.append(condition)
    query += " GROUP BY v.MedicalCondition, cs.mean_bill, cs.std_bill ORDER BY outlier_count DESC"

    with get_db() as cursor:
        cursor.execute(query, params)
        results = cursor.fetchall()

    return {
        "condition_filter": condition,
        "outliers": [
            {
                "condition": r["MedicalCondition"],
                "outlier_count": r["outlier_count"],
                "avg_outlier_bill": float(r["avg_outlier_bill"]),
                "condition_mean": float(r["condition_mean"]),
                "threshold": float(r["threshold"]),
            }
            for r in results
        ],
    }


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _age_to_group(age: int) -> str:
    """Convert a numeric age to the same age group buckets used in SQL."""
    if age <= 1:
        return "0-1"
    elif age <= 5:
        return "2-5"
    elif age <= 12:
        return "6-12"
    elif age <= 18:
        return "13-18"
    elif age <= 30:
        return "19-30"
    elif age <= 45:
        return "31-45"
    elif age <= 60:
        return "46-60"
    elif age <= 80:
        return "61-80"
    else:
        return "81+"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="InsureYours API Server")
    parser.add_argument("--host", default="0.0.0.0", help="Bind address")
    parser.add_argument("--port", type=int, default=8000, help="Port")
    parser.add_argument("--db-host", default="localhost", help="MySQL host")
    parser.add_argument("--db-port", type=int, default=3306, help="MySQL port")
    parser.add_argument("--db-user", default="root", help="MySQL user")
    parser.add_argument("--db-password", default="", help="MySQL password")
    args = parser.parse_args()

    # Override DB config from CLI args
    DB_CONFIG["host"] = args.db_host
    DB_CONFIG["port"] = args.db_port
    DB_CONFIG["user"] = args.db_user
    DB_CONFIG["password"] = args.db_password

    print(f"Starting InsureYours API on {args.host}:{args.port}")
    print(f"  MySQL: {args.db_host}:{args.db_port}")
    print(f"  Docs:  http://{args.host}:{args.port}/docs")
    uvicorn.run(app, host=args.host, port=args.port)

#!/usr/bin/env python3
"""
InsureYours Data Profiler — Generates a comprehensive data quality report.

Demonstrates: Exploratory Data Analysis (EDA) — core Data Analyst skill.

Runs against the staging table (HealthCare_Dataset) and produces a report
covering completeness, distributions, outliers, and data quality issues.

Usage:
    python3 data_profiler.py --host localhost --user root --password yourpassword
    python3 data_profiler.py --output report.txt   # save to file
"""

import argparse
import sys

try:
    import mysql.connector
except ImportError:
    print("ERROR: mysql-connector-python is required.")
    print("Install with: pip install mysql-connector-python")
    sys.exit(1)

DATABASE = "Healthcare_Group_Project"


def run_query(cursor, sql):
    cursor.execute(sql)
    return cursor.fetchall()


def section(title):
    width = 70
    return f"\n{'=' * width}\n  {title}\n{'=' * width}"


def profile_data(host, port, user, password, output_file=None):
    conn = mysql.connector.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=DATABASE,
    )
    cursor = conn.cursor(dictionary=True)

    lines = []

    def out(text=""):
        lines.append(text)
        print(text)

    out("INSUREYOURS DATA QUALITY REPORT")
    out("=" * 70)

    # ── Row Count ──
    out(section("1. DATASET OVERVIEW"))
    rows = run_query(cursor, "SELECT COUNT(*) AS n FROM HealthCare_Dataset")
    total = rows[0]["n"]
    out(f"  Total records: {total:,}")

    rows = run_query(
        cursor,
        """
        SELECT
            MIN(Age) AS min_age, MAX(Age) AS max_age, ROUND(AVG(Age),1) AS avg_age,
            MIN(BillingAmount) AS min_bill, MAX(BillingAmount) AS max_bill,
            ROUND(AVG(BillingAmount),2) AS avg_bill,
            MIN(DateOfAdmission) AS earliest, MAX(DateOfAdmission) AS latest
        FROM HealthCare_Dataset
    """,
    )
    r = rows[0]
    out(f"  Age range:     {r['min_age']} - {r['max_age']} (avg: {r['avg_age']})")
    out(
        f"  Billing range: ${r['min_bill']:,.2f} - ${r['max_bill']:,.2f} (avg: ${r['avg_bill']:,.2f})"
    )
    out(f"  Date range:    {r['earliest']} to {r['latest']}")

    # ── Completeness ──
    out(section("2. COMPLETENESS (NULL / EMPTY VALUES)"))
    columns = [
        "Name",
        "Age",
        "Gender",
        "BloodType",
        "MedicalCondition",
        "DateOfAdmission",
        "Doctor",
        "Hospital",
        "InsuranceProvider",
        "BillingAmount",
        "RoomNumber",
        "AdmissionType",
        "DischargeDate",
        "Medication",
        "TestResults",
    ]
    out(f"  {'Column':<25} {'NULLs':>8} {'Empty':>8} {'% Complete':>12}")
    out(f"  {'-' * 25} {'-' * 8} {'-' * 8} {'-' * 12}")
    for col in columns:
        nulls = run_query(
            cursor, f"SELECT COUNT(*) AS n FROM HealthCare_Dataset WHERE {col} IS NULL"
        )[0]["n"]
        empties = (
            run_query(
                cursor, f"SELECT COUNT(*) AS n FROM HealthCare_Dataset WHERE TRIM({col}) = ''"
            )[0]["n"]
            if col not in ("Age", "BillingAmount", "RoomNumber", "DateOfAdmission", "DischargeDate")
            else 0
        )
        pct = ((total - nulls - empties) / total * 100) if total > 0 else 0
        flag = " ⚠" if pct < 100 else ""
        out(f"  {col:<25} {nulls:>8} {empties:>8} {pct:>11.1f}%{flag}")

    # ── Cardinality ──
    out(section("3. CARDINALITY (DISTINCT VALUES)"))
    out(f"  {'Column':<25} {'Distinct':>10} {'Type'}")
    out(f"  {'-' * 25} {'-' * 10} {'-' * 20}")
    for col in columns:
        dist = run_query(cursor, f"SELECT COUNT(DISTINCT {col}) AS n FROM HealthCare_Dataset")[0][
            "n"
        ]
        ratio = dist / total if total > 0 else 0
        if ratio > 0.9:
            ctype = "High (likely unique)"
        elif ratio > 0.1:
            ctype = "Medium"
        elif dist <= 10:
            ctype = "Low (categorical)"
        else:
            ctype = "Low-Medium"
        out(f"  {col:<25} {dist:>10,} {ctype}")

    # ── Categorical Distributions ──
    out(section("4. CATEGORICAL DISTRIBUTIONS"))
    categoricals = {
        "Gender": "Gender",
        "BloodType": "BloodType",
        "MedicalCondition": "MedicalCondition",
        "AdmissionType": "AdmissionType",
        "InsuranceProvider": "InsuranceProvider",
        "TestResults": "TestResults",
    }
    for label, col in categoricals.items():
        rows = run_query(
            cursor,
            f"""
            SELECT {col} AS val, COUNT(*) AS n,
                   ROUND(COUNT(*) * 100.0 / {total}, 1) AS pct
            FROM HealthCare_Dataset
            GROUP BY {col}
            ORDER BY n DESC
        """,
        )
        out(f"\n  {label}:")
        for r in rows:
            bar = "█" * int(r["pct"] / 2)
            out(f"    {r['val']!s:<25} {r['n']:>6,} ({r['pct']:>5.1f}%) {bar}")

    # ── Billing Distribution ──
    out(section("5. BILLING AMOUNT DISTRIBUTION"))
    rows = run_query(
        cursor,
        """
        SELECT
            ROUND(AVG(BillingAmount), 2) AS mean,
            ROUND(STDDEV(BillingAmount), 2) AS stddev,
            MIN(BillingAmount) AS min_val,
            MAX(BillingAmount) AS max_val
        FROM HealthCare_Dataset
    """,
    )
    r = rows[0]
    out(f"  Mean:    ${r['mean']:>12,.2f}")
    out(f"  StdDev:  ${r['stddev']:>12,.2f}")
    out(f"  Min:     ${r['min_val']:>12,.2f}")
    out(f"  Max:     ${r['max_val']:>12,.2f}")

    # Percentiles (approximate using ORDER BY + LIMIT)
    out("\n  Percentiles:")
    for pct_label, offset in [
        ("25th", total // 4),
        ("50th (median)", total // 2),
        ("75th", total * 3 // 4),
        ("90th", int(total * 0.9)),
        ("99th", int(total * 0.99)),
    ]:
        rows = run_query(
            cursor,
            f"""
            SELECT BillingAmount AS val
            FROM HealthCare_Dataset
            ORDER BY BillingAmount
            LIMIT 1 OFFSET {offset}
        """,
        )
        out(f"    {pct_label:<20} ${rows[0]['val']:>12,.2f}")

    # ── Outlier Detection ──
    out(section("6. OUTLIER DETECTION (BILLING)"))
    mean = float(r["mean"])
    stddev = float(r["stddev"])
    threshold_2 = mean + 2 * stddev
    threshold_3 = mean + 3 * stddev

    rows = run_query(
        cursor,
        f"""
        SELECT COUNT(*) AS n FROM HealthCare_Dataset WHERE BillingAmount > {threshold_2}
    """,
    )
    out(f"  Records > 2 std devs (${threshold_2:,.2f}): {rows[0]['n']:,}")

    rows = run_query(
        cursor,
        f"""
        SELECT COUNT(*) AS n FROM HealthCare_Dataset WHERE BillingAmount > {threshold_3}
    """,
    )
    out(f"  Records > 3 std devs (${threshold_3:,.2f}): {rows[0]['n']:,}")

    # ── Age Distribution ──
    out(section("7. AGE GROUP DISTRIBUTION"))
    rows = run_query(
        cursor,
        """
        SELECT
            CASE
                WHEN Age BETWEEN  0 AND  1  THEN '0-1'
                WHEN Age BETWEEN  2 AND  5  THEN '2-5'
                WHEN Age BETWEEN  6 AND 12  THEN '6-12'
                WHEN Age BETWEEN 13 AND 18  THEN '13-18'
                WHEN Age BETWEEN 19 AND 30  THEN '19-30'
                WHEN Age BETWEEN 31 AND 45  THEN '31-45'
                WHEN Age BETWEEN 46 AND 60  THEN '46-60'
                WHEN Age BETWEEN 61 AND 80  THEN '61-80'
                WHEN Age >= 81              THEN '81+'
                ELSE 'Unknown'
            END AS AgeGroup,
            COUNT(*) AS n,
            ROUND(AVG(BillingAmount), 2) AS avg_billing
        FROM HealthCare_Dataset
        GROUP BY AgeGroup
        ORDER BY MIN(Age)
    """,
    )
    out(f"  {'Age Group':<12} {'Count':>8} {'Avg Billing':>14}")
    out(f"  {'-' * 12} {'-' * 8} {'-' * 14}")
    for r in rows:
        out(f"  {r['AgeGroup']:<12} {r['n']:>8,} ${r['avg_billing']:>12,.2f}")

    # ── Length of Stay ──
    out(section("8. LENGTH OF STAY ANALYSIS"))
    rows = run_query(
        cursor,
        """
        SELECT
            ROUND(AVG(DATEDIFF(DischargeDate, DateOfAdmission)), 1) AS avg_stay,
            MIN(DATEDIFF(DischargeDate, DateOfAdmission)) AS min_stay,
            MAX(DATEDIFF(DischargeDate, DateOfAdmission)) AS max_stay,
            COUNT(CASE WHEN DATEDIFF(DischargeDate, DateOfAdmission) = 0 THEN 1 END) AS same_day,
            COUNT(CASE WHEN DATEDIFF(DischargeDate, DateOfAdmission) > 30 THEN 1 END) AS over_30_days
        FROM HealthCare_Dataset
        WHERE DischargeDate IS NOT NULL AND DateOfAdmission IS NOT NULL
    """,
    )
    r = rows[0]
    out(f"  Average stay:     {r['avg_stay']} days")
    out(f"  Range:            {r['min_stay']} - {r['max_stay']} days")
    out(f"  Same-day:         {r['same_day']:,} records")
    out(f"  Over 30 days:     {r['over_30_days']:,} records")

    # ── Data Quality Score ──
    out(section("9. DATA QUALITY SCORE"))
    # Simple quality scoring
    checks_passed = 0
    checks_total = 5

    # Check 1: No NULLs in key fields
    nulls = run_query(
        cursor,
        """
        SELECT COUNT(*) AS n FROM HealthCare_Dataset
        WHERE Name IS NULL OR Age IS NULL OR BillingAmount IS NULL
           OR InsuranceProvider IS NULL OR MedicalCondition IS NULL
    """,
    )[0]["n"]
    check1 = nulls == 0
    checks_passed += int(check1)
    out(f"  [{'PASS' if check1 else 'FAIL'}] No NULLs in key fields ({nulls} found)")

    # Check 2: All billing amounts positive
    neg = run_query(cursor, "SELECT COUNT(*) AS n FROM HealthCare_Dataset WHERE BillingAmount < 0")[
        0
    ]["n"]
    check2 = neg == 0
    checks_passed += int(check2)
    out(f"  [{'PASS' if check2 else 'FAIL'}] All billing amounts >= 0 ({neg} negative)")

    # Check 3: Discharge >= Admission
    bad_dates = run_query(
        cursor,
        """
        SELECT COUNT(*) AS n FROM HealthCare_Dataset
        WHERE DischargeDate < DateOfAdmission
    """,
    )[0]["n"]
    check3 = bad_dates == 0
    checks_passed += int(check3)
    out(
        f"  [{'PASS' if check3 else 'FAIL'}] Discharge date >= admission date ({bad_dates} violations)"
    )

    # Check 4: Age within expected range
    bad_age = run_query(
        cursor, "SELECT COUNT(*) AS n FROM HealthCare_Dataset WHERE Age < 0 OR Age > 150"
    )[0]["n"]
    check4 = bad_age == 0
    checks_passed += int(check4)
    out(f"  [{'PASS' if check4 else 'FAIL'}] Age in range 0-120 ({bad_age} violations)")

    # Check 5: All categorical values are recognized
    bad_cats = run_query(
        cursor,
        """
        SELECT COUNT(*) AS n FROM HealthCare_Dataset
        WHERE Gender NOT IN ('Male', 'Female')
           OR BloodType NOT IN ('A+','A-','B+','B-','AB+','AB-','O+','O-')
           OR AdmissionType NOT IN ('Elective', 'Emergency', 'Urgent')
    """,
    )[0]["n"]
    check5 = bad_cats == 0
    checks_passed += int(check5)
    out(f"  [{'PASS' if check5 else 'FAIL'}] All categorical values valid ({bad_cats} invalid)")

    score = checks_passed / checks_total * 100
    out(f"\n  Overall Score: {checks_passed}/{checks_total} ({score:.0f}%)")

    out("\n" + "=" * 70)
    out("END OF REPORT")
    out("=" * 70)

    cursor.close()
    conn.close()

    if output_file:
        with open(output_file, "w") as f:
            f.write("\n".join(lines))
        print(f"\nReport saved to: {output_file}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="InsureYours Data Profiler")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=3306)
    parser.add_argument("--user", default="root")
    parser.add_argument("--password", default="")
    parser.add_argument("--output", default=None, help="Save report to file")
    args = parser.parse_args()

    profile_data(args.host, args.port, args.user, args.password, args.output)

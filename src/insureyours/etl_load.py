#!/usr/bin/env python3
"""
InsureYours ETL Pipeline — Replaces the Windows-only SSIS package.

Reads healthcare_dataset.csv, validates data, loads the staging table,
then populates the normalized dimension and fact tables.

Usage:
    python3 etl_load.py                          # defaults: localhost, root, no password
    python3 etl_load.py --host 10.0.0.5 --user admin --password secret
    python3 etl_load.py --csv /path/to/data.csv  # custom CSV path

Requirements:
    pip install mysql-connector-python
"""

import argparse
import csv
import os
import sys
from datetime import datetime

try:
    import mysql.connector
    from mysql.connector import Error as MySQLError
except ImportError:
    mysql = None
    MySQLError = Exception  # fallback for import-time; validation still works


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DEFAULT_CSV = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "..", "healthcare_dataset.csv"
)
DATABASE = "Healthcare_Group_Project"

# Expected CSV columns (in order)
EXPECTED_COLUMNS = [
    "Name",
    "Age",
    "Gender",
    "Blood Type",
    "Medical Condition",
    "Date of Admission",
    "Doctor",
    "Hospital",
    "Insurance Provider",
    "Billing Amount",
    "Room Number",
    "Admission Type",
    "Discharge Date",
    "Medication",
    "Test Results",
]

# Valid values for constrained fields
VALID_GENDERS = {"Male", "Female"}
VALID_BLOOD_TYPES = {"A+", "A-", "B+", "B-", "AB+", "AB-", "O+", "O-"}
VALID_ADMISSION_TYPES = {"Elective", "Emergency", "Urgent"}
VALID_CONDITIONS = {"Diabetes", "Asthma", "Obesity", "Arthritis", "Hypertension", "Cancer"}
VALID_TEST_RESULTS = {"Normal", "Abnormal", "Inconclusive"}


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------
def validate_row(row, line_num):
    """Validate a single CSV row. Returns (cleaned_row, errors)."""
    errors = []

    # Age
    try:
        age = int(row["Age"])
        if age < 0 or age > 150:
            errors.append(f"Line {line_num}: Age {age} out of range 0-150")
    except (ValueError, TypeError):
        errors.append(f"Line {line_num}: Invalid age '{row['Age']}'")
        age = None

    # Gender
    gender = row["Gender"].strip()
    if gender not in VALID_GENDERS:
        errors.append(f"Line {line_num}: Invalid gender '{gender}'")

    # Blood Type
    blood_type = row["Blood Type"].strip()
    if blood_type not in VALID_BLOOD_TYPES:
        errors.append(f"Line {line_num}: Invalid blood type '{blood_type}'")

    # Medical Condition
    condition = row["Medical Condition"].strip()
    if condition not in VALID_CONDITIONS:
        errors.append(f"Line {line_num}: Invalid medical condition '{condition}'")

    # Admission Type
    admission_type = row["Admission Type"].strip()
    if admission_type not in VALID_ADMISSION_TYPES:
        errors.append(f"Line {line_num}: Invalid admission type '{admission_type}'")

    # Test Results
    test_results = row["Test Results"].strip()
    if test_results not in VALID_TEST_RESULTS:
        errors.append(f"Line {line_num}: Invalid test results '{test_results}'")

    # Billing Amount
    try:
        billing = float(row["Billing Amount"])
        if billing < 0:
            errors.append(f"Line {line_num}: Negative billing amount {billing}")
    except (ValueError, TypeError):
        errors.append(f"Line {line_num}: Invalid billing amount '{row['Billing Amount']}'")
        billing = None

    # Dates
    try:
        admission_date = datetime.strptime(row["Date of Admission"], "%Y-%m-%d").date()
    except (ValueError, TypeError):
        errors.append(f"Line {line_num}: Invalid admission date '{row['Date of Admission']}'")
        admission_date = None

    try:
        discharge_date = datetime.strptime(row["Discharge Date"], "%Y-%m-%d").date()
    except (ValueError, TypeError):
        errors.append(f"Line {line_num}: Invalid discharge date '{row['Discharge Date']}'")
        discharge_date = None

    if admission_date and discharge_date and discharge_date < admission_date:
        errors.append(f"Line {line_num}: Discharge date before admission date")

    # Room Number
    try:
        room = int(row["Room Number"])
    except (ValueError, TypeError):
        errors.append(f"Line {line_num}: Invalid room number '{row['Room Number']}'")
        room = None

    cleaned = {
        "Name": row["Name"].strip(),
        "Age": age,
        "Gender": gender,
        "BloodType": blood_type,
        "MedicalCondition": condition,
        "DateOfAdmission": admission_date,
        "Doctor": row["Doctor"].strip(),
        "Hospital": row["Hospital"].strip(),
        "InsuranceProvider": row["Insurance Provider"].strip(),
        "BillingAmount": billing,
        "RoomNumber": room,
        "AdmissionType": admission_type,
        "DischargeDate": discharge_date,
        "Medication": row["Medication"].strip(),
        "TestResults": test_results,
    }
    return cleaned, errors


# ---------------------------------------------------------------------------
# Database operations
# ---------------------------------------------------------------------------
def get_connection(host, user, password, port):
    """Create a MySQL connection."""
    if mysql is None:
        print("ERROR: mysql-connector-python is required.")
        print("Install it with: pip install mysql-connector-python")
        sys.exit(1)
    return mysql.connector.connect(
        host=host,
        user=user,
        password=password,
        port=port,
        database=DATABASE,
        autocommit=False,
    )


def load_staging(cursor, rows):
    """Bulk insert validated rows into HealthCare_Dataset staging table."""
    cursor.execute("DELETE FROM HealthCare_Dataset")
    sql = """
        INSERT INTO HealthCare_Dataset
            (Name, Age, Gender, BloodType, MedicalCondition, DateOfAdmission,
             Doctor, Hospital, InsuranceProvider, BillingAmount, RoomNumber,
             AdmissionType, DischargeDate, Medication, TestResults)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    batch = []
    for r in rows:
        batch.append(
            (
                r["Name"],
                r["Age"],
                r["Gender"],
                r["BloodType"],
                r["MedicalCondition"],
                r["DateOfAdmission"],
                r["Doctor"],
                r["Hospital"],
                r["InsuranceProvider"],
                r["BillingAmount"],
                r["RoomNumber"],
                r["AdmissionType"],
                r["DischargeDate"],
                r["Medication"],
                r["TestResults"],
            )
        )
        if len(batch) >= 1000:
            cursor.executemany(sql, batch)
            batch = []
    if batch:
        cursor.executemany(sql, batch)
    return cursor.rowcount


def populate_dimension(cursor, table, name_column, source_column):
    """Populate a dimension table with distinct values from staging."""
    cursor.execute(f"DELETE FROM {table}")
    cursor.execute(f"""
        INSERT INTO {table} ({name_column})
        SELECT DISTINCT {source_column}
        FROM HealthCare_Dataset
        WHERE {source_column} IS NOT NULL
        ORDER BY {source_column}
    """)
    return cursor.rowcount


def populate_admissions(cursor):
    """Populate Admission fact table by joining staging to all dimensions."""
    cursor.execute("DELETE FROM Admission")
    cursor.execute("""
        INSERT INTO Admission
            (PatientID, DoctorID, HospitalID, InsuranceID, MedicationID,
             DateOfAdmission, RoomNumber, AdmissionType, DischargeDate,
             BillingAmount, MedicalCondition, TestResults)
        SELECT
            p.PatientID,
            d.DoctorID,
            h.HospitalID,
            i.InsuranceID,
            m.MedicationID,
            s.DateOfAdmission,
            s.RoomNumber,
            s.AdmissionType,
            s.DischargeDate,
            s.BillingAmount,
            s.MedicalCondition,
            s.TestResults
        FROM HealthCare_Dataset s
        JOIN Patient p
            ON p.Name = s.Name AND p.Age = s.Age
            AND p.Gender = s.Gender AND p.BloodType = s.BloodType
        JOIN Doctor d            ON d.Name = s.Doctor
        JOIN Hospital h          ON h.Name = s.Hospital
        JOIN InsuranceProvider i  ON i.Name = s.InsuranceProvider
        JOIN Medication m        ON m.Name = s.Medication
    """)
    return cursor.rowcount


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="InsureYours ETL Pipeline")
    parser.add_argument("--csv", default=DEFAULT_CSV, help="Path to healthcare_dataset.csv")
    parser.add_argument("--host", default="localhost", help="MySQL host")
    parser.add_argument("--port", type=int, default=3306, help="MySQL port")
    parser.add_argument("--user", default="root", help="MySQL user")
    parser.add_argument("--password", default="", help="MySQL password")
    parser.add_argument(
        "--skip-validation", action="store_true", help="Skip row validation (faster, less safe)"
    )
    args = parser.parse_args()

    # --- Read CSV ---
    print(f"Reading CSV: {args.csv}")
    if not os.path.exists(args.csv):
        print(f"ERROR: File not found: {args.csv}")
        sys.exit(1)

    rows = []
    all_errors = []
    rejected_count = 0
    with open(args.csv, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        # Verify header matches expected columns
        actual = [c.strip() for c in reader.fieldnames]
        if actual != EXPECTED_COLUMNS:
            print("ERROR: CSV columns don't match expected schema.")
            print(f"  Expected: {EXPECTED_COLUMNS}")
            print(f"  Got:      {actual}")
            sys.exit(1)

        for line_num, raw_row in enumerate(reader, start=2):
            if args.skip_validation:
                # Minimal cleaning — skip constraint checks but catch type errors
                try:
                    rows.append(
                        {
                            "Name": raw_row["Name"].strip(),
                            "Age": int(raw_row["Age"]),
                            "Gender": raw_row["Gender"].strip(),
                            "BloodType": raw_row["Blood Type"].strip(),
                            "MedicalCondition": raw_row["Medical Condition"].strip(),
                            "DateOfAdmission": raw_row["Date of Admission"].strip(),
                            "Doctor": raw_row["Doctor"].strip(),
                            "Hospital": raw_row["Hospital"].strip(),
                            "InsuranceProvider": raw_row["Insurance Provider"].strip(),
                            "BillingAmount": float(raw_row["Billing Amount"]),
                            "RoomNumber": int(raw_row["Room Number"]),
                            "AdmissionType": raw_row["Admission Type"].strip(),
                            "DischargeDate": raw_row["Discharge Date"].strip(),
                            "Medication": raw_row["Medication"].strip(),
                            "TestResults": raw_row["Test Results"].strip(),
                        }
                    )
                except (ValueError, TypeError) as e:
                    all_errors.append(f"Line {line_num}: {e}")
                    rejected_count += 1
            else:
                cleaned, errors = validate_row(raw_row, line_num)
                if errors:
                    all_errors.extend(errors)
                    rejected_count += 1
                else:
                    rows.append(cleaned)

    print(f"  Rows read: {len(rows) + rejected_count}")
    print(f"  Valid:     {len(rows)}")
    if all_errors:
        print(f"  Rejected:  {rejected_count} rows ({len(all_errors)} errors, first 10 shown)")
        for err in all_errors[:10]:
            print(f"    {err}")

    if not rows:
        print("ERROR: No valid rows to load.")
        sys.exit(1)

    # --- Load into MySQL ---
    print(f"\nConnecting to MySQL at {args.host}:{args.port}...")
    try:
        conn = get_connection(args.host, args.user, args.password, args.port)
        cursor = conn.cursor()

        print("Loading staging table...")
        load_staging(cursor, rows)
        conn.commit()
        print(f"  Staged {len(rows)} rows into HealthCare_Dataset")

        print("Populating dimension tables...")

        # Patient needs multi-column DISTINCT (handled separately)
        cursor.execute("DELETE FROM Admission")  # clear FK dependencies first
        cursor.execute("DELETE FROM Patient")
        cursor.execute("""
            INSERT INTO Patient (Name, Age, Gender, BloodType)
            SELECT DISTINCT Name, Age, Gender, BloodType
            FROM HealthCare_Dataset
            WHERE Name IS NOT NULL
        """)
        print(f"  Patient: {cursor.rowcount} rows")

        for table, col, src in [
            ("Doctor", "Name", "Doctor"),
            ("Hospital", "Name", "Hospital"),
            ("InsuranceProvider", "Name", "InsuranceProvider"),
            ("Medication", "Name", "Medication"),
        ]:
            count = populate_dimension(cursor, table, col, src)
            print(f"  {table}: {count} rows")

        print("Populating Admission fact table...")
        adm_count = populate_admissions(cursor)
        print(f"  Admission: {adm_count} rows")

        conn.commit()
        print("\nETL complete. Run StoredProcedure.sql then Analysis.sql next.")

    except MySQLError as e:
        print(f"MySQL ERROR: {e}")
        sys.exit(1)
    finally:
        if "conn" in locals() and conn.is_connected():
            cursor.close()
            conn.close()


if __name__ == "__main__":
    main()

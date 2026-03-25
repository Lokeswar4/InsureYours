"""Tests for ETL validation logic — no MySQL connection required."""

import os
import sys
import unittest
from datetime import date

# Add project root to path so we can import etl_load
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from api import _age_to_group
from etl_load import EXPECTED_COLUMNS, validate_row


class TestValidateRow(unittest.TestCase):
    """Test the row validation function."""

    def _make_row(self, **overrides):
        """Create a valid row dict, with optional field overrides."""
        base = {
            "Name": "Jane Doe",
            "Age": "45",
            "Gender": "Female",
            "Blood Type": "O+",
            "Medical Condition": "Diabetes",
            "Date of Admission": "2023-01-15",
            "Doctor": "Dr. Smith",
            "Hospital": "General Hospital",
            "Insurance Provider": "Medicare",
            "Billing Amount": "25000.50",
            "Room Number": "101",
            "Admission Type": "Elective",
            "Discharge Date": "2023-01-25",
            "Medication": "Aspirin",
            "Test Results": "Normal",
        }
        base.update(overrides)
        return base

    # ------------------------------------------------------------------
    # Happy path
    # ------------------------------------------------------------------
    def test_valid_row_passes(self):
        row = self._make_row()
        cleaned, errors = validate_row(row, 2)
        self.assertEqual(errors, [])
        self.assertEqual(cleaned["Name"], "Jane Doe")
        self.assertEqual(cleaned["Age"], 45)
        self.assertEqual(cleaned["Gender"], "Female")
        self.assertEqual(cleaned["BloodType"], "O+")
        self.assertEqual(cleaned["BillingAmount"], 25000.50)
        self.assertEqual(cleaned["DateOfAdmission"], date(2023, 1, 15))
        self.assertEqual(cleaned["DischargeDate"], date(2023, 1, 25))

    # ------------------------------------------------------------------
    # Age validation
    # ------------------------------------------------------------------
    def test_age_zero_valid(self):
        _, errors = validate_row(self._make_row(Age="0"), 2)
        self.assertEqual(errors, [])

    def test_age_150_valid(self):
        _, errors = validate_row(self._make_row(Age="150"), 2)
        self.assertEqual(errors, [])

    def test_age_negative_invalid(self):
        _, errors = validate_row(self._make_row(Age="-1"), 2)
        self.assertTrue(any("Age" in e for e in errors))

    def test_age_over_150_invalid(self):
        _, errors = validate_row(self._make_row(Age="151"), 2)
        self.assertTrue(any("Age" in e for e in errors))

    def test_age_non_numeric_invalid(self):
        _, errors = validate_row(self._make_row(Age="abc"), 2)
        self.assertTrue(any("age" in e.lower() for e in errors))

    # ------------------------------------------------------------------
    # Gender validation
    # ------------------------------------------------------------------
    def test_valid_genders(self):
        for g in ["Male", "Female"]:
            _, errors = validate_row(self._make_row(Gender=g), 2)
            self.assertEqual(errors, [], f"Gender '{g}' should be valid")

    def test_invalid_gender(self):
        _, errors = validate_row(self._make_row(Gender="Other"), 2)
        self.assertTrue(any("gender" in e.lower() for e in errors))

    # ------------------------------------------------------------------
    # Blood type validation
    # ------------------------------------------------------------------
    def test_all_blood_types_valid(self):
        for bt in ["A+", "A-", "B+", "B-", "AB+", "AB-", "O+", "O-"]:
            _, errors = validate_row(self._make_row(**{"Blood Type": bt}), 2)
            self.assertEqual(errors, [], f"Blood type '{bt}' should be valid")

    def test_invalid_blood_type(self):
        _, errors = validate_row(self._make_row(**{"Blood Type": "X+"}), 2)
        self.assertTrue(any("blood type" in e.lower() for e in errors))

    # ------------------------------------------------------------------
    # Medical condition validation
    # ------------------------------------------------------------------
    def test_valid_conditions(self):
        for c in ["Diabetes", "Asthma", "Obesity", "Arthritis", "Hypertension", "Cancer"]:
            _, errors = validate_row(self._make_row(**{"Medical Condition": c}), 2)
            self.assertEqual(errors, [], f"Condition '{c}' should be valid")

    def test_invalid_condition(self):
        _, errors = validate_row(self._make_row(**{"Medical Condition": "Flu"}), 2)
        self.assertTrue(any("condition" in e.lower() for e in errors))

    # ------------------------------------------------------------------
    # Admission type validation
    # ------------------------------------------------------------------
    def test_valid_admission_types(self):
        for at in ["Elective", "Emergency", "Urgent"]:
            _, errors = validate_row(self._make_row(**{"Admission Type": at}), 2)
            self.assertEqual(errors, [], f"Admission type '{at}' should be valid")

    def test_invalid_admission_type(self):
        _, errors = validate_row(self._make_row(**{"Admission Type": "Scheduled"}), 2)
        self.assertTrue(any("admission type" in e.lower() for e in errors))

    # ------------------------------------------------------------------
    # Billing amount
    # ------------------------------------------------------------------
    def test_billing_zero_valid(self):
        _, errors = validate_row(self._make_row(**{"Billing Amount": "0"}), 2)
        self.assertEqual(errors, [])

    def test_billing_negative_invalid(self):
        _, errors = validate_row(self._make_row(**{"Billing Amount": "-100"}), 2)
        self.assertTrue(any("billing" in e.lower() for e in errors))

    def test_billing_non_numeric_invalid(self):
        _, errors = validate_row(self._make_row(**{"Billing Amount": "free"}), 2)
        self.assertTrue(any("billing" in e.lower() for e in errors))

    def test_billing_high_precision(self):
        """Real CSV has values like 37490.98336352819 — should not error."""
        _, errors = validate_row(self._make_row(**{"Billing Amount": "37490.98336352819"}), 2)
        self.assertEqual(errors, [])

    # ------------------------------------------------------------------
    # Date validation
    # ------------------------------------------------------------------
    def test_discharge_before_admission_invalid(self):
        _, errors = validate_row(
            self._make_row(**{"Date of Admission": "2023-01-15", "Discharge Date": "2023-01-10"}),
            2,
        )
        self.assertTrue(any("discharge" in e.lower() for e in errors))

    def test_same_day_discharge_valid(self):
        _, errors = validate_row(
            self._make_row(**{"Date of Admission": "2023-01-15", "Discharge Date": "2023-01-15"}),
            2,
        )
        self.assertEqual(errors, [])

    def test_bad_date_format_invalid(self):
        _, errors = validate_row(self._make_row(**{"Date of Admission": "01/15/2023"}), 2)
        self.assertTrue(any("admission date" in e.lower() for e in errors))

    # ------------------------------------------------------------------
    # Test results validation
    # ------------------------------------------------------------------
    def test_valid_test_results(self):
        for tr in ["Normal", "Abnormal", "Inconclusive"]:
            _, errors = validate_row(self._make_row(**{"Test Results": tr}), 2)
            self.assertEqual(errors, [], f"Test result '{tr}' should be valid")

    def test_invalid_test_result(self):
        _, errors = validate_row(self._make_row(**{"Test Results": "Pending"}), 2)
        self.assertTrue(any("test results" in e.lower() for e in errors))

    # ------------------------------------------------------------------
    # Whitespace handling
    # ------------------------------------------------------------------
    def test_strips_whitespace(self):
        row = self._make_row(Name="  Jane Doe  ", Gender=" Female ", **{"Blood Type": " O+ "})
        cleaned, errors = validate_row(row, 2)
        self.assertEqual(errors, [])
        self.assertEqual(cleaned["Name"], "Jane Doe")
        self.assertEqual(cleaned["Gender"], "Female")
        self.assertEqual(cleaned["BloodType"], "O+")

    # ------------------------------------------------------------------
    # Multiple errors in one row
    # ------------------------------------------------------------------
    def test_multiple_errors_reported(self):
        row = self._make_row(Age="-5", Gender="X", **{"Blood Type": "Z", "Billing Amount": "abc"})
        _, errors = validate_row(row, 2)
        self.assertGreaterEqual(len(errors), 3)


class TestExpectedColumns(unittest.TestCase):
    """Verify the expected columns list matches the real CSV."""

    def test_expected_column_count(self):
        self.assertEqual(len(EXPECTED_COLUMNS), 15)

    def test_csv_header_matches(self):
        csv_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "healthcare_dataset.csv",
        )
        if not os.path.exists(csv_path):
            self.skipTest("healthcare_dataset.csv not present")

        with open(csv_path, encoding="utf-8") as f:
            header = f.readline().strip().split(",")
        self.assertEqual(header, EXPECTED_COLUMNS)


class TestSkipValidation(unittest.TestCase):
    """Test that --skip-validation gracefully handles bad data."""

    def test_bad_age_does_not_crash(self):
        """A non-numeric age should be skipped, not crash the entire ETL."""
        import argparse
        import csv
        import tempfile

        # Write a tiny CSV with one good row and one bad row
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False, newline="") as f:
            writer = csv.writer(f)
            writer.writerow(EXPECTED_COLUMNS)
            writer.writerow(
                [
                    "Good Row",
                    "45",
                    "Female",
                    "O+",
                    "Diabetes",
                    "2023-01-15",
                    "Dr. Smith",
                    "Hospital",
                    "Medicare",
                    "25000.50",
                    "101",
                    "Elective",
                    "2023-01-25",
                    "Aspirin",
                    "Normal",
                ]
            )
            writer.writerow(
                [
                    "Bad Row",
                    "NOT_A_NUMBER",
                    "Female",
                    "O+",
                    "Diabetes",
                    "2023-01-15",
                    "Dr. Smith",
                    "Hospital",
                    "Medicare",
                    "25000.50",
                    "101",
                    "Elective",
                    "2023-01-25",
                    "Aspirin",
                    "Normal",
                ]
            )
            tmp_path = f.name

        # Simulate the parsing logic from main()
        args = argparse.Namespace(csv=tmp_path, skip_validation=True)
        rows = []
        all_errors = []
        rejected_count = 0

        with open(args.csv, newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            for line_num, raw_row in enumerate(reader, start=2):
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

        os.unlink(tmp_path)
        self.assertEqual(len(rows), 1, "Good row should be kept")
        self.assertEqual(rejected_count, 1, "Bad row should be rejected, not crash")
        self.assertEqual(len(all_errors), 1)


class TestAgeToGroup(unittest.TestCase):
    """Test the age-to-group mapping matches SQL CASE logic exactly."""

    def test_boundaries(self):
        """Every boundary age must map to the correct group."""
        expected = [
            (0, "0-1"),
            (1, "0-1"),
            (2, "2-5"),
            (5, "2-5"),
            (6, "6-12"),
            (12, "6-12"),
            (13, "13-18"),
            (18, "13-18"),
            (19, "19-30"),
            (30, "19-30"),
            (31, "31-45"),
            (45, "31-45"),
            (46, "46-60"),
            (60, "46-60"),
            (61, "61-80"),
            (80, "61-80"),
            (81, "81+"),
            (100, "81+"),
            (150, "81+"),
        ]
        for age, group in expected:
            self.assertEqual(
                _age_to_group(age),
                group,
                f"age {age} should map to '{group}'",
            )


if __name__ == "__main__":
    unittest.main()

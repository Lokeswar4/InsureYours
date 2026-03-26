"""API endpoint tests — no real MySQL connection required (pool is mocked)."""

import os
import sys
from typing import ClassVar
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(
    0,
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src", "insureyours"),
)

import api
from fastapi.testclient import TestClient

# Client without lifespan (pool stays None unless patched per-test)
client = TestClient(api.app, raise_server_exceptions=True)


def _mock_pool(rows):
    """Return a fake pool that yields `rows` from cursor.fetchall() / fetchone()."""
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = rows
    mock_cursor.fetchone.return_value = rows[0] if rows else None

    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    mock_pool = MagicMock()
    mock_pool.get_connection.return_value = mock_conn
    return mock_pool


# ---------------------------------------------------------------------------
# Root (no DB)
# ---------------------------------------------------------------------------
def test_root():
    r = client.get("/")
    assert r.status_code == 200
    body = r.json()
    assert body["project"] == "InsureYours"
    assert "/recommend" in body["endpoints"]
    assert "/analytics/outliers" in body["endpoints"]


# ---------------------------------------------------------------------------
# /recommend — validation (no DB needed, FastAPI rejects bad params with 422)
# ---------------------------------------------------------------------------
class TestRecommendValidation:
    def test_missing_all_params(self):
        assert client.get("/recommend").status_code == 422

    def test_missing_condition(self):
        assert client.get("/recommend?age=45&blood_type=O%2B&medication=Aspirin").status_code == 422

    def test_missing_blood_type(self):
        assert (
            client.get("/recommend?age=45&condition=Diabetes&medication=Aspirin").status_code == 422
        )

    def test_age_negative(self):
        r = client.get("/recommend?age=-1&condition=Diabetes&blood_type=O%2B&medication=Aspirin")
        assert r.status_code == 422

    def test_age_over_150(self):
        r = client.get("/recommend?age=151&condition=Diabetes&blood_type=O%2B&medication=Aspirin")
        assert r.status_code == 422

    def test_age_boundary_zero(self):
        """age=0 passes validation; 404 because no matching DB row."""
        with patch("api.pool", _mock_pool([])):
            r = client.get("/recommend?age=0&condition=Diabetes&blood_type=O%2B&medication=Aspirin")
        assert r.status_code == 404

    def test_age_boundary_150(self):
        """age=150 passes validation; 404 because no matching DB row."""
        with patch("api.pool", _mock_pool([])):
            r = client.get(
                "/recommend?age=150&condition=Diabetes&blood_type=O%2B&medication=Aspirin"
            )
        assert r.status_code == 404


# ---------------------------------------------------------------------------
# /recommend — success paths
# ---------------------------------------------------------------------------
class TestRecommend:
    _ROW: ClassVar[dict] = {
        "InsuranceProvider": "Aetna",
        "AvgBillingAmount": 23000.50,
        "MinBillingAmount": 15000.00,
        "MaxBillingAmount": 35000.00,
        "ClaimCount": 12,
        "ProviderRank": 1,
    }

    def test_found_returns_200(self):
        with patch("api.pool", _mock_pool([self._ROW])):
            r = client.get(
                "/recommend?age=45&condition=Diabetes&blood_type=O%2B&medication=Aspirin"
            )
        assert r.status_code == 200

    def test_age_group_mapped_correctly(self):
        with patch("api.pool", _mock_pool([self._ROW])):
            r = client.get(
                "/recommend?age=45&condition=Diabetes&blood_type=O%2B&medication=Aspirin"
            )
        body = r.json()
        assert body["patient_profile"]["age_group"] == "31-45"

    def test_recommendation_fields(self):
        with patch("api.pool", _mock_pool([self._ROW])):
            r = client.get(
                "/recommend?age=45&condition=Diabetes&blood_type=O%2B&medication=Aspirin"
            )
        rec = r.json()["recommendation"][0]
        assert rec["insurer"] == "Aetna"
        assert rec["avg_billing"] == pytest.approx(23000.50)
        assert rec["rank"] == 1

    def test_not_found_returns_404(self):
        with patch("api.pool", _mock_pool([])):
            r = client.get(
                "/recommend?age=45&condition=Diabetes&blood_type=O%2B&medication=Aspirin"
            )
        assert r.status_code == 404
        assert "searched" in r.json()["detail"]


# ---------------------------------------------------------------------------
# /conditions and /insurers
# ---------------------------------------------------------------------------
class TestLists:
    def test_conditions(self):
        rows = [{"MedicalCondition": c} for c in ["Asthma", "Cancer", "Diabetes"]]
        with patch("api.pool", _mock_pool(rows)):
            r = client.get("/conditions")
        assert r.status_code == 200
        assert r.json()["conditions"] == ["Asthma", "Cancer", "Diabetes"]

    def test_insurers(self):
        rows = [{"InsuranceProvider": p} for p in ["Aetna", "Medicare", "UnitedHealthcare"]]
        with patch("api.pool", _mock_pool(rows)):
            r = client.get("/insurers")
        assert r.status_code == 200
        assert r.json()["insurers"] == ["Aetna", "Medicare", "UnitedHealthcare"]


# ---------------------------------------------------------------------------
# /analytics/billing-summary
# ---------------------------------------------------------------------------
class TestBillingSummary:
    _ROW: ClassVar[dict] = {
        "AgeGroup": "31-45",
        "MedicalCondition": "Diabetes",
        "avg_billing": 25000.0,
        "claim_count": 100,
        "min_billing": 10000.0,
        "max_billing": 50000.0,
    }

    def test_no_filter(self):
        with patch("api.pool", _mock_pool([self._ROW])):
            r = client.get("/analytics/billing-summary")
        assert r.status_code == 200
        assert len(r.json()["data"]) == 1

    def test_condition_filter_passed_through(self):
        with patch("api.pool", _mock_pool([self._ROW])):
            r = client.get("/analytics/billing-summary?condition=Diabetes")
        assert r.status_code == 200
        assert r.json()["filters"]["condition"] == "Diabetes"

    def test_response_shape(self):
        with patch("api.pool", _mock_pool([self._ROW])):
            r = client.get("/analytics/billing-summary")
        row = r.json()["data"][0]
        assert {
            "age_group",
            "condition",
            "avg_billing",
            "claim_count",
            "min_billing",
            "max_billing",
        } <= row.keys()


# ---------------------------------------------------------------------------
# /analytics/provider-compare
# ---------------------------------------------------------------------------
class TestProviderCompare:
    _ROW: ClassVar[dict] = {
        "InsuranceProvider": "Aetna",
        "AgeGroup": "31-45",
        "avg_billing": 23000.0,
        "claim_count": 30,
        "stddev_billing": 5000.0,
    }

    def test_missing_condition_returns_422(self):
        assert client.get("/analytics/provider-compare").status_code == 422

    def test_found(self):
        with patch("api.pool", _mock_pool([self._ROW])):
            r = client.get("/analytics/provider-compare?condition=Diabetes")
        assert r.status_code == 200
        assert r.json()["condition"] == "Diabetes"
        assert r.json()["providers"][0]["insurer"] == "Aetna"

    def test_not_found_returns_404(self):
        with patch("api.pool", _mock_pool([])):
            r = client.get("/analytics/provider-compare?condition=Unknown")
        assert r.status_code == 404


# ---------------------------------------------------------------------------
# /analytics/outliers
# ---------------------------------------------------------------------------
class TestOutliers:
    _ROW: ClassVar[dict] = {
        "MedicalCondition": "Diabetes",
        "outlier_count": 5,
        "avg_outlier_bill": 48000.0,
        "condition_mean": 25000.0,
        "threshold": 45000.0,
    }

    def test_returns_200(self):
        with patch("api.pool", _mock_pool([self._ROW])):
            r = client.get("/analytics/outliers")
        assert r.status_code == 200

    def test_response_shape(self):
        with patch("api.pool", _mock_pool([self._ROW])):
            r = client.get("/analytics/outliers")
        row = r.json()["outliers"][0]
        assert {
            "condition",
            "outlier_count",
            "avg_outlier_bill",
            "condition_mean",
            "threshold",
        } <= row.keys()

    def test_empty_result(self):
        with patch("api.pool", _mock_pool([])):
            r = client.get("/analytics/outliers")
        assert r.status_code == 200
        assert r.json()["outliers"] == []

    def test_condition_filter(self):
        with patch("api.pool", _mock_pool([self._ROW])):
            r = client.get("/analytics/outliers?condition=Diabetes")
        assert r.status_code == 200
        assert r.json()["condition_filter"] == "Diabetes"


# ---------------------------------------------------------------------------
# /health
# ---------------------------------------------------------------------------
def test_health():
    with patch("api.pool", _mock_pool([{"n": 1440}])):
        r = client.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "healthy"
    assert r.json()["recommendations_loaded"] == 1440

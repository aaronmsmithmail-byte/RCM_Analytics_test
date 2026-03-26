"""Unit tests for src/database.py — build_filter_cte().

Tests cover SQL structure validation (CTE shape, parameterisation) and
execution against a real SQLite database with representative Silver data.
"""

import duckdb
import pytest

from src.database import build_filter_cte, create_tables

# ===========================================================================
# Shared fixtures
# ===========================================================================

@pytest.fixture
def db(tmp_path):
    """Temporary SQLite database pre-loaded with representative Silver data.

    Data layout (4 claims across 2 payers, 2 departments, 3 encounter types):
        CLM001  PYR001  Outpatient   Cardiology    2024-01-15
        CLM002  PYR001  Outpatient   Cardiology    2024-01-20
        CLM003  PYR002  Inpatient    Orthopedics   2024-02-10
        CLM004  PYR002  Emergency    Orthopedics   2024-02-25
    """
    db_path = str(tmp_path / "test.db")
    conn = duckdb.connect(db_path)
    create_tables(conn)
    conn.execute("""
        INSERT INTO silver_payers VALUES
            ('PYR001','Aetna','Commercial',0.85,'C001'),
            ('PYR002','Medicaid','Government',0.70,'G001');

        INSERT INTO silver_patients VALUES
            ('PAT001','Alice','Smith','1980-01-01','F','PYR001','M001','10001'),
            ('PAT002','Bob','Jones','1975-05-15','M','PYR001','M002','10002'),
            ('PAT003','Carol','Lee','1990-03-20','F','PYR002','M003','10003'),
            ('PAT004','Dave','Kim','1965-11-30','M','PYR002','M004','10004');

        INSERT INTO silver_providers VALUES
            ('PRV001','Dr. A','1111111111','Cardiology','Internal Medicine'),
            ('PRV002','Dr. B','2222222222','Orthopedics','Orthopedics');

        INSERT INTO silver_encounters VALUES
            ('ENC010','PAT001','PRV001','2024-01-15','2024-01-15','Outpatient','Cardiology'),
            ('ENC020','PAT002','PRV001','2024-01-20','2024-01-20','Outpatient','Cardiology'),
            ('ENC030','PAT003','PRV002','2024-02-10','2024-02-11','Inpatient','Orthopedics'),
            ('ENC040','PAT004','PRV002','2024-02-25','2024-02-25','Emergency','Orthopedics');

        INSERT INTO silver_claims VALUES
            ('CLM001','ENC010','PAT001','PYR001','2024-01-15','2024-01-17',1000.0,'Paid',1,'Electronic',NULL),
            ('CLM002','ENC020','PAT002','PYR001','2024-01-20','2024-01-22',2000.0,'Denied',0,'Electronic','CODING_ERROR'),
            ('CLM003','ENC030','PAT003','PYR002','2024-02-10','2024-02-12',1500.0,'Paid',1,'Electronic',NULL),
            ('CLM004','ENC040','PAT004','PYR002','2024-02-25','2024-02-27',500.0,'Appealed',0,'Paper','MISSING_AUTH');
    """)
    conn.commit()
    conn.close()
    return db_path


def _execute_cte(db_path, cte_sql, params):
    """Run the CTE and return filtered claim_id values as a set."""
    conn = duckdb.connect(db_path)
    try:
        query = cte_sql + "SELECT claim_id FROM filtered_claims"
        rows = conn.execute(query, params).fetchall()
        return {r[0] for r in rows}
    finally:
        conn.close()


# ===========================================================================
# SQL structure tests
# ===========================================================================

class TestBuildFilterCteSqlStructure:
    """Verify the shape and parameterisation of the generated SQL."""

    def test_date_only_returns_two_params(self):
        cte, params = build_filter_cte("2024-01-01", "2024-12-31")
        assert params == ["2024-01-01", "2024-12-31"]

    def test_payer_filter_adds_param(self):
        cte, params = build_filter_cte("2024-01-01", "2024-12-31", payer_id="PYR001")
        assert len(params) == 3
        assert "c.payer_id = ?" in cte

    def test_department_filter_adds_param(self):
        cte, params = build_filter_cte("2024-01-01", "2024-12-31", department="Cardiology")
        assert len(params) == 3
        assert "e.department = ?" in cte

    def test_encounter_type_filter_adds_param(self):
        cte, params = build_filter_cte("2024-01-01", "2024-12-31", encounter_type="Outpatient")
        assert len(params) == 3
        assert "e.encounter_type = ?" in cte

    def test_all_filters_combined(self):
        cte, params = build_filter_cte(
            "2024-01-01", "2024-12-31",
            payer_id="PYR001", department="Cardiology", encounter_type="Outpatient",
        )
        assert len(params) == 5
        assert "c.payer_id = ?" in cte
        assert "e.department = ?" in cte
        assert "e.encounter_type = ?" in cte

    def test_cte_starts_with_with_clause(self):
        cte, _ = build_filter_cte("2024-01-01", "2024-12-31")
        assert cte.strip().startswith("WITH filtered_claims AS (")

    def test_cte_joins_encounters(self):
        cte, _ = build_filter_cte("2024-01-01", "2024-12-31")
        assert "LEFT JOIN silver_encounters" in cte

    def test_no_raw_values_in_sql(self):
        cte, _ = build_filter_cte(
            "2024-01-01", "2024-12-31",
            payer_id="PYR001", department="Cardiology",
        )
        assert "PYR001" not in cte
        assert "Cardiology" not in cte
        assert "2024-01-01" not in cte


# ===========================================================================
# Execution tests against real database
# ===========================================================================

class TestBuildFilterCteExecution:
    """Run the generated CTE against a test database and verify row counts."""

    def test_full_date_range_returns_all_claims(self, db):
        cte, params = build_filter_cte("2024-01-01", "2024-12-31")
        result = _execute_cte(db, cte, params)
        assert result == {"CLM001", "CLM002", "CLM003", "CLM004"}

    def test_payer_filter_returns_matching_claims(self, db):
        cte, params = build_filter_cte("2024-01-01", "2024-12-31", payer_id="PYR001")
        result = _execute_cte(db, cte, params)
        assert result == {"CLM001", "CLM002"}

    def test_department_filter_returns_matching_claims(self, db):
        cte, params = build_filter_cte("2024-01-01", "2024-12-31", department="Cardiology")
        result = _execute_cte(db, cte, params)
        assert result == {"CLM001", "CLM002"}

    def test_encounter_type_filter_returns_matching_claims(self, db):
        cte, params = build_filter_cte("2024-01-01", "2024-12-31", encounter_type="Outpatient")
        result = _execute_cte(db, cte, params)
        assert result == {"CLM001", "CLM002"}

    def test_all_filters_narrow_results(self, db):
        cte, params = build_filter_cte(
            "2024-01-01", "2024-12-31",
            payer_id="PYR001", department="Cardiology", encounter_type="Outpatient",
        )
        result = _execute_cte(db, cte, params)
        assert result == {"CLM001", "CLM002"}

    def test_date_range_with_no_data_returns_empty(self, db):
        cte, params = build_filter_cte("2025-01-01", "2025-12-31")
        result = _execute_cte(db, cte, params)
        assert result == set()

    def test_nonexistent_payer_returns_empty(self, db):
        cte, params = build_filter_cte("2024-01-01", "2024-12-31", payer_id="NONEXISTENT")
        result = _execute_cte(db, cte, params)
        assert result == set()

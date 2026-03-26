"""Unit tests for src/validators.py — all 6 SQL-based validation check functions.

All validators now query Silver tables directly in SQLite.
Each test uses a temporary SQLite database (via tmp_path) pre-loaded with
clean Silver-layer data. Individual tests update specific rows to introduce
violations and verify the expected issues are returned.
"""

import duckdb
import pytest

from src.database import create_tables
from src.validators import (
    validate_all,
    _check_negative_amounts,
    _check_orphaned_keys,
    _check_nulls,
    _check_date_ranges,
    _check_claim_status_values,
    _check_boolean_columns,
)


# ── Shared fixture ─────────────────────────────────────────────────────────────


def _insert_clean_data(conn):
    """Insert a minimal, clean Silver-layer dataset into the given connection."""
    conn.execute("""
        INSERT INTO silver_payers VALUES ('PYR001','Aetna','Commercial',0.80,'C001');
        INSERT INTO silver_payers VALUES ('PYR002','Medicare','Government',0.75,'C002');

        INSERT INTO silver_patients VALUES
            ('PAT001','Alice','Smith','1980-01-01','F','PYR001','M001','10001');
        INSERT INTO silver_patients VALUES
            ('PAT002','Bob','Jones','1975-05-15','M','PYR002','M002','10002');

        INSERT INTO silver_providers VALUES
            ('PROV01','Dr. Smith','1234567890','Cardiology','Cardiology');
        INSERT INTO silver_providers VALUES
            ('PROV02','Dr. Jones','0987654321','Oncology','Oncology');

        INSERT INTO silver_encounters VALUES
            ('ENC001','PAT001','PROV01','2024-06-01',NULL,'Outpatient','Cardiology');
        INSERT INTO silver_encounters VALUES
            ('ENC002','PAT002','PROV02','2024-06-15',NULL,'Outpatient','Oncology');

        INSERT INTO silver_charges VALUES
            ('CHG001','ENC001','99213','Office Visit',1,500.0,'2024-06-01','2024-06-02',NULL);
        INSERT INTO silver_charges VALUES
            ('CHG002','ENC002','99214','Office Visit',1,1200.0,'2024-06-15','2024-06-16',NULL);

        INSERT INTO silver_claims VALUES
            ('CLM001','ENC001','PAT001','PYR001',
             '2024-06-01','2024-06-03',500.0,'Paid',1,'Electronic',NULL);
        INSERT INTO silver_claims VALUES
            ('CLM002','ENC002','PAT002','PYR002',
             '2024-06-15','2024-06-17',1200.0,'Partially Paid',1,'Electronic',NULL);

        INSERT INTO silver_payments VALUES
            ('PAY001','CLM001','PYR001',450.0,480.0,'2024-07-01','EFT',1);
        INSERT INTO silver_payments VALUES
            ('PAY002','CLM002','PYR002',600.0,650.0,'2024-07-15','EFT',1);

        INSERT INTO silver_denials VALUES
            ('DEN001','CLM002','AUTH','Missing Auth','2024-07-01',600.0,'Not Appealed',NULL,0.0);

        INSERT INTO silver_adjustments VALUES
            ('ADJ001','CLM001','CONTRACTUAL','Contractual Discount',50.0,'2024-07-05');

        INSERT INTO silver_operating_costs VALUES
            ('2024-06',10000.0,2000.0,1500.0,500.0,14000.0);
    """)
    conn.commit()


def _create_tables_no_constraints(conn):
    """Create Silver tables without FK or PK constraints for validator testing.

    Validators intentionally create orphaned keys, NULL PKs, and invalid data
    to test detection logic. DuckDB enforces constraints strictly, so we strip
    FOREIGN KEY clauses and PRIMARY KEY from the schema here.
    """
    import re
    from src.database import (
        BRONZE_SCHEMA_SQL, SILVER_SCHEMA_SQL, GOLD_VIEWS_SQL,
        INDEX_SQL, METADATA_SCHEMA_SQL,
    )
    # Remove FOREIGN KEY clauses, PRIMARY KEY, and NOT NULL from Silver schema
    silver_relaxed = re.sub(r',?\s*FOREIGN KEY\s*\([^)]*\)\s*REFERENCES\s*\w+\([^)]*\)', '', SILVER_SCHEMA_SQL)
    silver_relaxed = silver_relaxed.replace(' PRIMARY KEY', '')
    silver_relaxed = silver_relaxed.replace(' NOT NULL', '')
    conn.execute(BRONZE_SCHEMA_SQL)
    conn.execute(silver_relaxed)
    conn.execute(GOLD_VIEWS_SQL)
    conn.execute(INDEX_SQL)
    conn.execute(METADATA_SCHEMA_SQL)


@pytest.fixture
def clean_db(tmp_path):
    """Temporary DuckDB database pre-loaded with clean Silver-layer data (no FK constraints)."""
    db_path = str(tmp_path / "test.db")
    conn = duckdb.connect(db_path)
    _create_tables_no_constraints(conn)
    _insert_clean_data(conn)
    conn.close()
    return db_path


@pytest.fixture
def empty_db(tmp_path):
    """Temporary DuckDB database with Silver schema but no data rows."""
    db_path = str(tmp_path / "empty.db")
    conn = duckdb.connect(db_path)
    _create_tables_no_constraints(conn)
    conn.close()
    return db_path


# ── Tests: validate_all ───────────────────────────────────────────────────────


class TestValidateAll:
    def test_returns_list(self, clean_db):
        result = validate_all(clean_db)
        assert isinstance(result, list)

    def test_clean_data_returns_no_issues(self, clean_db):
        issues = validate_all(clean_db)
        assert issues == []

    def test_issues_have_required_keys(self, clean_db):
        conn = duckdb.connect(clean_db)
        conn.execute("UPDATE silver_claims SET total_charge_amount = -100 WHERE claim_id = 'CLM001'")
        conn.commit()
        conn.close()
        issues = validate_all(clean_db)
        assert len(issues) > 0
        for issue in issues:
            assert "level" in issue
            assert "table" in issue
            assert "message" in issue

    def test_empty_tables_return_no_issues(self, empty_db):
        issues = validate_all(empty_db)
        assert isinstance(issues, list)
        assert issues == []


# ── Tests: _check_negative_amounts ───────────────────────────────────────────


class TestCheckNegativeAmounts:
    def test_no_issues_when_all_positive(self, clean_db):
        issues = _check_negative_amounts(clean_db)
        assert issues == []

    def test_warns_on_negative_charge_amount(self, clean_db):
        conn = duckdb.connect(clean_db)
        conn.execute("UPDATE silver_claims SET total_charge_amount = -50 WHERE claim_id = 'CLM001'")
        conn.commit(); conn.close()
        issues = _check_negative_amounts(clean_db)
        assert len(issues) == 1
        assert issues[0]["level"] == "warning"
        assert "silver_claims" in issues[0]["table"]
        assert "total_charge_amount" in issues[0]["message"]

    def test_warns_on_negative_payment_amount(self, clean_db):
        conn = duckdb.connect(clean_db)
        conn.execute("UPDATE silver_payments SET payment_amount = -10 WHERE payment_id = 'PAY001'")
        conn.commit(); conn.close()
        issues = _check_negative_amounts(clean_db)
        tables = [i["table"] for i in issues]
        assert "silver_payments" in tables

    def test_warns_on_negative_denied_amount(self, clean_db):
        conn = duckdb.connect(clean_db)
        conn.execute("UPDATE silver_denials SET denied_amount = -200 WHERE denial_id = 'DEN001'")
        conn.commit(); conn.close()
        issues = _check_negative_amounts(clean_db)
        assert any(i["table"] == "silver_denials" for i in issues)

    def test_warns_on_negative_rcm_cost(self, clean_db):
        conn = duckdb.connect(clean_db)
        conn.execute("UPDATE silver_operating_costs SET total_rcm_cost = -1000 WHERE period = '2024-06'")
        conn.commit(); conn.close()
        issues = _check_negative_amounts(clean_db)
        assert any(i["table"] == "silver_operating_costs" for i in issues)

    def test_no_issues_on_empty_tables(self, empty_db):
        issues = _check_negative_amounts(empty_db)
        assert issues == []

    def test_counts_multiple_negatives(self, clean_db):
        conn = duckdb.connect(clean_db)
        conn.execute("UPDATE silver_payments SET payment_amount = -10")  # all rows
        conn.commit(); conn.close()
        issues = _check_negative_amounts(clean_db)
        pay_issues = [i for i in issues
                      if i["table"] == "silver_payments" and "payment_amount" in i["message"]]
        assert len(pay_issues) == 1
        assert "2" in pay_issues[0]["message"]


# ── Tests: _check_orphaned_keys ───────────────────────────────────────────────


class TestCheckOrphanedKeys:
    def test_no_issues_when_all_keys_valid(self, clean_db):
        issues = _check_orphaned_keys(clean_db)
        assert issues == []

    def test_warns_on_payment_with_unknown_claim(self, clean_db):
        conn = duckdb.connect(clean_db)
        conn.execute("UPDATE silver_payments SET claim_id = 'GHOST_CLAIM' WHERE payment_id = 'PAY001'")
        conn.commit(); conn.close()
        issues = _check_orphaned_keys(clean_db)
        assert any("silver_payments" in i["table"] for i in issues)

    def test_warns_on_denial_with_unknown_claim(self, clean_db):
        conn = duckdb.connect(clean_db)
        conn.execute("UPDATE silver_denials SET claim_id = 'MISSING' WHERE denial_id = 'DEN001'")
        conn.commit(); conn.close()
        issues = _check_orphaned_keys(clean_db)
        assert any(i["table"] == "silver_denials" for i in issues)

    def test_warns_on_claim_with_unknown_payer(self, clean_db):
        conn = duckdb.connect(clean_db)
        conn.execute("UPDATE silver_claims SET payer_id = 'PYR999' WHERE claim_id = 'CLM001'")
        conn.commit(); conn.close()
        issues = _check_orphaned_keys(clean_db)
        assert any(i["table"] == "silver_claims" for i in issues)

    def test_warns_on_encounter_with_unknown_patient(self, clean_db):
        conn = duckdb.connect(clean_db)
        conn.execute("UPDATE silver_encounters SET patient_id = 'GHOST_PAT' WHERE encounter_id = 'ENC001'")
        conn.commit(); conn.close()
        issues = _check_orphaned_keys(clean_db)
        assert any(i["table"] == "silver_encounters" for i in issues)

    def test_issue_level_is_warning(self, clean_db):
        conn = duckdb.connect(clean_db)
        conn.execute("UPDATE silver_payments SET claim_id = 'GHOST' WHERE payment_id = 'PAY001'")
        conn.commit(); conn.close()
        issues = _check_orphaned_keys(clean_db)
        for i in issues:
            assert i["level"] == "warning"

    def test_no_issues_on_empty_tables(self, empty_db):
        # Empty tables have no FK violations by definition
        issues = _check_orphaned_keys(empty_db)
        assert issues == []


# ── Tests: _check_nulls ───────────────────────────────────────────────────────


class TestCheckNulls:
    def test_no_issues_when_all_required_filled(self, clean_db):
        issues = _check_nulls(clean_db)
        assert issues == []

    def test_errors_on_null_claim_id(self, clean_db):
        conn = duckdb.connect(clean_db)
        conn.execute("UPDATE silver_claims SET claim_id = NULL WHERE claim_id = 'CLM001'")
        conn.close()
        issues = _check_nulls(clean_db)
        assert any(i["level"] == "error" and "silver_claims" in i["table"] for i in issues)

    def test_errors_on_null_payment_amount(self, clean_db):
        conn = duckdb.connect(clean_db)
        conn.execute("UPDATE silver_payments SET payment_amount = NULL WHERE payment_id = 'PAY001'")
        conn.close()
        issues = _check_nulls(clean_db)
        assert any(i["level"] == "error" and i["table"] == "silver_payments" for i in issues)

    def test_errors_on_null_encounter_date(self, clean_db):
        conn = duckdb.connect(clean_db)
        conn.execute("UPDATE silver_encounters SET date_of_service = NULL WHERE encounter_id = 'ENC001'")
        conn.close()
        issues = _check_nulls(clean_db)
        assert any(i["level"] == "error" and i["table"] == "silver_encounters" for i in issues)

    def test_error_message_names_column(self, clean_db):
        conn = duckdb.connect(clean_db)
        conn.execute("UPDATE silver_claims SET claim_status = NULL WHERE claim_id = 'CLM001'")
        conn.close()
        issues = _check_nulls(clean_db)
        claim_issues = [i for i in issues if i["table"] == "silver_claims"]
        assert any("claim_status" in i["message"] for i in claim_issues)

    def test_no_issues_on_empty_tables(self, empty_db):
        issues = _check_nulls(empty_db)
        assert issues == []


# ── Tests: _check_date_ranges ─────────────────────────────────────────────────


class TestCheckDateRanges:
    def test_no_issues_with_valid_dates(self, clean_db):
        issues = _check_date_ranges(clean_db)
        assert issues == []

    def test_warns_on_date_before_2020(self, clean_db):
        conn = duckdb.connect(clean_db)
        conn.execute("UPDATE silver_claims SET date_of_service = '2019-12-31' WHERE claim_id = 'CLM001'")
        conn.commit(); conn.close()
        issues = _check_date_ranges(clean_db)
        assert any(i["table"] == "silver_claims" for i in issues)

    def test_warns_on_date_after_2030(self, clean_db):
        conn = duckdb.connect(clean_db)
        conn.execute("UPDATE silver_payments SET payment_date = '2031-01-01' WHERE payment_id = 'PAY001'")
        conn.commit(); conn.close()
        issues = _check_date_ranges(clean_db)
        assert any(i["table"] == "silver_payments" for i in issues)

    def test_issue_level_is_warning(self, clean_db):
        conn = duckdb.connect(clean_db)
        conn.execute("UPDATE silver_claims SET date_of_service = '2015-01-01' WHERE claim_id = 'CLM001'")
        conn.commit(); conn.close()
        issues = _check_date_ranges(clean_db)
        for i in issues:
            assert i["level"] == "warning"

    def test_null_dates_not_flagged_as_out_of_range(self, clean_db):
        conn = duckdb.connect(clean_db)
        # NULL submission_date — should NOT be flagged as out-of-range
        conn.execute("UPDATE silver_claims SET submission_date = NULL WHERE claim_id = 'CLM001'")
        conn.close()
        issues = _check_date_ranges(clean_db)
        assert not any(
            "submission_date" in i["message"] for i in issues
            if i["table"] == "silver_claims"
        )


# ── Tests: _check_claim_status_values ────────────────────────────────────────


class TestCheckClaimStatusValues:
    def test_no_issues_with_all_valid_statuses(self, clean_db):
        # clean_db already has 'Paid' and 'Partially Paid'
        issues = _check_claim_status_values(clean_db)
        assert issues == []

    def test_warns_on_unknown_status(self, clean_db):
        conn = duckdb.connect(clean_db)
        conn.execute("UPDATE silver_claims SET claim_status = 'Unknown' WHERE claim_id = 'CLM001'")
        conn.commit(); conn.close()
        issues = _check_claim_status_values(clean_db)
        assert len(issues) == 1
        assert issues[0]["level"] == "warning"
        assert "Unknown" in issues[0]["message"]

    def test_message_lists_bad_values(self, clean_db):
        conn = duckdb.connect(clean_db)
        conn.execute("UPDATE silver_claims SET claim_status = 'Bad1' WHERE claim_id = 'CLM001'")
        conn.execute("UPDATE silver_claims SET claim_status = 'Bad2' WHERE claim_id = 'CLM002'")
        conn.commit(); conn.close()
        issues = _check_claim_status_values(clean_db)
        assert len(issues) == 1
        msg = issues[0]["message"]
        assert "Bad1" in msg or "Bad2" in msg

    def test_no_issues_on_empty_tables(self, empty_db):
        issues = _check_claim_status_values(empty_db)
        assert issues == []

    def test_all_valid_status_values_accepted(self, clean_db):
        for status in ["Paid", "Denied", "Appealed", "Pending", "Partially Paid"]:
            conn = duckdb.connect(clean_db)
            conn.execute("UPDATE silver_claims SET claim_status = ?", (status,))
            conn.commit(); conn.close()
            issues = _check_claim_status_values(clean_db)
            assert issues == [], f"'{status}' should be valid but got: {issues}"

    def test_partially_paid_is_valid(self, clean_db):
        conn = duckdb.connect(clean_db)
        conn.execute("UPDATE silver_claims SET claim_status = 'Partially Paid'")
        conn.commit(); conn.close()
        issues = _check_claim_status_values(clean_db)
        assert issues == []


# ── Tests: _check_boolean_columns ─────────────────────────────────────────────


class TestCheckBooleanColumns:
    def test_no_issues_when_all_filled(self, clean_db):
        issues = _check_boolean_columns(clean_db)
        assert issues == []

    def test_warns_on_null_is_clean_claim(self, clean_db):
        conn = duckdb.connect(clean_db)
        conn.execute("UPDATE silver_claims SET is_clean_claim = NULL WHERE claim_id = 'CLM001'")
        conn.commit(); conn.close()
        issues = _check_boolean_columns(clean_db)
        assert any(
            i["table"] == "silver_claims" and "is_clean_claim" in i["message"]
            for i in issues
        )

    def test_warns_on_null_is_accurate_payment(self, clean_db):
        conn = duckdb.connect(clean_db)
        conn.execute("UPDATE silver_payments SET is_accurate_payment = NULL WHERE payment_id = 'PAY001'")
        conn.commit(); conn.close()
        issues = _check_boolean_columns(clean_db)
        assert any(
            i["table"] == "silver_payments" and "is_accurate_payment" in i["message"]
            for i in issues
        )

    def test_issue_level_is_warning(self, clean_db):
        conn = duckdb.connect(clean_db)
        conn.execute("UPDATE silver_claims SET is_clean_claim = NULL WHERE claim_id = 'CLM001'")
        conn.commit(); conn.close()
        issues = _check_boolean_columns(clean_db)
        for i in issues:
            assert i["level"] == "warning"

    def test_no_issues_on_empty_tables(self, empty_db):
        issues = _check_boolean_columns(empty_db)
        assert issues == []

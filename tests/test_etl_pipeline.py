"""Unit tests for the ETL pipeline in src/database.py.

Covers CSV→Bronze loading, Bronze→Silver type casting, boolean conversion,
NULL/empty PK filtering, duplicate handling, and missing file behaviour.
"""

import duckdb
import pytest

from src.database import (
    _etl_bronze_to_silver,
    create_tables,
    load_csv_to_bronze,
)

# ===========================================================================
# Shared fixtures
# ===========================================================================

@pytest.fixture
def conn(tmp_path):
    """Fresh DuckDB connection with schema created."""
    db_path = str(tmp_path / "test.db")
    c = duckdb.connect(db_path)
    create_tables(c)
    yield c
    c.close()


@pytest.fixture
def data_dir(tmp_path):
    """Temporary data directory for CSV files."""
    d = tmp_path / "data"
    d.mkdir()
    return d


def _write_csv(path, content):
    """Write CSV content string to a file."""
    path.write_text(content)


# ===========================================================================
# load_csv_to_bronze
# ===========================================================================

class TestLoadCsvToBronze:
    def test_loads_rows_into_bronze(self, conn, data_dir, monkeypatch):
        monkeypatch.setattr("src.database.DATA_DIR", str(data_dir))
        _write_csv(data_dir / "payers.csv",
                   "payer_id,payer_name,payer_type,avg_reimbursement_pct,contract_id\n"
                   "PYR001,Aetna,Commercial,0.85,C001\n"
                   "PYR002,Medicaid,Government,0.70,G001\n")

        load_csv_to_bronze(conn, "bronze_payers", "payers.csv")

        rows = conn.execute("SELECT COUNT(*) FROM bronze_payers").fetchone()[0]
        assert rows == 2

    def test_all_columns_stored_as_text(self, conn, data_dir, monkeypatch):
        monkeypatch.setattr("src.database.DATA_DIR", str(data_dir))
        _write_csv(data_dir / "payers.csv",
                   "payer_id,payer_name,payer_type,avg_reimbursement_pct,contract_id\n"
                   "PYR001,Aetna,Commercial,0.85,C001\n")

        load_csv_to_bronze(conn, "bronze_payers", "payers.csv")

        val = conn.execute(
            "SELECT avg_reimbursement_pct FROM bronze_payers"
        ).fetchone()[0]
        assert isinstance(val, str)
        assert val == "0.85"

    def test_missing_csv_skips_gracefully(self, conn, data_dir, monkeypatch):
        monkeypatch.setattr("src.database.DATA_DIR", str(data_dir))
        # No file created — should not raise
        load_csv_to_bronze(conn, "bronze_payers", "nonexistent.csv")

        rows = conn.execute("SELECT COUNT(*) FROM bronze_payers").fetchone()[0]
        assert rows == 0

    def test_reload_replaces_existing_data(self, conn, data_dir, monkeypatch):
        monkeypatch.setattr("src.database.DATA_DIR", str(data_dir))
        csv = data_dir / "payers.csv"
        _write_csv(csv,
                   "payer_id,payer_name,payer_type,avg_reimbursement_pct,contract_id\n"
                   "PYR001,Aetna,Commercial,0.85,C001\n")
        load_csv_to_bronze(conn, "bronze_payers", "payers.csv")

        # Reload with different data
        _write_csv(csv,
                   "payer_id,payer_name,payer_type,avg_reimbursement_pct,contract_id\n"
                   "PYR099,NewPayer,Commercial,0.90,C099\n")
        load_csv_to_bronze(conn, "bronze_payers", "payers.csv")

        rows = conn.execute("SELECT COUNT(*) FROM bronze_payers").fetchone()[0]
        assert rows == 1
        pid = conn.execute("SELECT payer_id FROM bronze_payers").fetchone()[0]
        assert pid == "PYR099"

    def test_records_pipeline_run(self, conn, data_dir, monkeypatch):
        monkeypatch.setattr("src.database.DATA_DIR", str(data_dir))
        _write_csv(data_dir / "claims.csv",
                   "claim_id,encounter_id,patient_id,payer_id,date_of_service,"
                   "submission_date,total_charge_amount,claim_status,"
                   "is_clean_claim,submission_method,fail_reason\n"
                   "CLM001,ENC001,PAT001,PYR001,2024-01-01,2024-01-02,"
                   "1000,Paid,True,Electronic,\n")
        load_csv_to_bronze(conn, "bronze_claims", "claims.csv")

        run = conn.execute(
            "SELECT domain, row_count, source_file FROM pipeline_runs WHERE domain='claims'"
        ).fetchone()
        assert run[0] == "claims"
        assert run[1] == 1
        assert run[2] == "claims.csv"

    def test_strips_loaded_at_column(self, conn, data_dir, monkeypatch):
        monkeypatch.setattr("src.database.DATA_DIR", str(data_dir))
        _write_csv(data_dir / "payers.csv",
                   "payer_id,payer_name,payer_type,avg_reimbursement_pct,contract_id,_loaded_at\n"
                   "PYR001,Aetna,Commercial,0.85,C001,2024-01-01\n")
        load_csv_to_bronze(conn, "bronze_payers", "payers.csv")

        rows = conn.execute("SELECT COUNT(*) FROM bronze_payers").fetchone()[0]
        assert rows == 1


# ===========================================================================
# _etl_bronze_to_silver — type casting and transformations
# ===========================================================================

class TestEtlBronzeToSilver:
    def _load_bronze_data(self, conn):
        """Insert representative bronze data directly for ETL testing."""
        conn.execute("""
            INSERT INTO bronze_payers (payer_id, payer_name, payer_type,
                avg_reimbursement_pct, contract_id)
            VALUES ('PYR001', 'Aetna', 'Commercial', '0.85', 'C001');

            INSERT INTO bronze_patients (patient_id, first_name, last_name,
                date_of_birth, gender, primary_payer_id, member_id, zip_code)
            VALUES ('PAT001', 'Alice', 'Smith', '1980-01-01', 'F',
                    'PYR001', 'M001', '10001');

            INSERT INTO bronze_providers (provider_id, provider_name, npi,
                department, specialty)
            VALUES ('PRV001', 'Dr. A', '1111111111', 'Cardiology', 'Internal Medicine');

            INSERT INTO bronze_encounters (encounter_id, patient_id, provider_id,
                date_of_service, discharge_date, encounter_type, department)
            VALUES ('ENC001', 'PAT001', 'PRV001', '2024-01-15', '2024-01-15',
                    'Outpatient', 'Cardiology');

            INSERT INTO bronze_claims (claim_id, encounter_id, patient_id, payer_id,
                date_of_service, submission_date, total_charge_amount, claim_status,
                is_clean_claim, submission_method, fail_reason)
            VALUES ('CLM001', 'ENC001', 'PAT001', 'PYR001', '2024-01-15',
                    '2024-01-17', '1000.50', 'Paid', 'True', 'Electronic', '');

            INSERT INTO bronze_payments (payment_id, claim_id, payer_id,
                payment_amount, allowed_amount, payment_date, payment_method,
                is_accurate_payment)
            VALUES ('PAY001', 'CLM001', 'PYR001', '900.25', '950.00',
                    '2024-02-01', 'EFT', '1');

            INSERT INTO bronze_denials (denial_id, claim_id, denial_reason_code,
                denial_reason_description, denial_date, denied_amount,
                appeal_status, appeal_date, recovered_amount)
            VALUES ('DEN001', 'CLM001', 'CO-4', 'Not covered', '2024-02-01',
                    '500.00', 'Won', '2024-03-01', '450.00');

            INSERT INTO bronze_adjustments (adjustment_id, claim_id,
                adjustment_type_code, adjustment_type_description,
                adjustment_amount, adjustment_date)
            VALUES ('ADJ001', 'CLM001', 'CONTRACTUAL', 'Contractual Adj',
                    '50.25', '2024-02-01');

            INSERT INTO bronze_operating_costs (period, billing_staff_cost,
                software_cost, outsourcing_cost, supplies_overhead, total_rcm_cost)
            VALUES ('2024-01', '10000', '2000', '5000', '500', '17500');

            INSERT INTO bronze_charges (charge_id, encounter_id, cpt_code,
                cpt_description, units, charge_amount, service_date, post_date,
                icd10_code)
            VALUES ('CHG001', 'ENC001', '99213', 'Office Visit', '1', '200.00',
                    '2024-01-15', '2024-01-17', 'Z00.00');
        """)

    def test_payer_reimbursement_cast_to_real(self, conn):
        self._load_bronze_data(conn)
        _etl_bronze_to_silver(conn)

        val = conn.execute(
            "SELECT avg_reimbursement_pct FROM silver_payers WHERE payer_id='PYR001'"
        ).fetchone()[0]
        assert isinstance(val, float)
        assert val == pytest.approx(0.85)

    def test_claim_charge_amount_cast_to_real(self, conn):
        self._load_bronze_data(conn)
        _etl_bronze_to_silver(conn)

        val = conn.execute(
            "SELECT total_charge_amount FROM silver_claims WHERE claim_id='CLM001'"
        ).fetchone()[0]
        assert isinstance(val, float)
        assert val == pytest.approx(1000.50)

    def test_boolean_true_converted_to_1(self, conn):
        self._load_bronze_data(conn)
        _etl_bronze_to_silver(conn)

        val = conn.execute(
            "SELECT is_clean_claim FROM silver_claims WHERE claim_id='CLM001'"
        ).fetchone()[0]
        assert val == 1

    def test_boolean_numeric_1_converted_to_1(self, conn):
        self._load_bronze_data(conn)
        _etl_bronze_to_silver(conn)

        val = conn.execute(
            "SELECT is_accurate_payment FROM silver_payments WHERE payment_id='PAY001'"
        ).fetchone()[0]
        assert val == 1

    def test_boolean_false_converted_to_0(self, conn):
        self._load_bronze_data(conn)
        conn.execute("""
            INSERT INTO bronze_claims (claim_id, encounter_id, patient_id, payer_id,
                date_of_service, submission_date, total_charge_amount, claim_status,
                is_clean_claim, submission_method, fail_reason)
            VALUES ('CLM002', 'ENC001', 'PAT001', 'PYR001', '2024-01-20',
                    '2024-01-22', '2000', 'Denied', 'False', 'Electronic', 'CODING_ERROR')
        """)
        _etl_bronze_to_silver(conn)

        val = conn.execute(
            "SELECT is_clean_claim FROM silver_claims WHERE claim_id='CLM002'"
        ).fetchone()[0]
        assert val == 0

    def test_empty_fail_reason_becomes_null(self, conn):
        self._load_bronze_data(conn)
        _etl_bronze_to_silver(conn)

        val = conn.execute(
            "SELECT fail_reason FROM silver_claims WHERE claim_id='CLM001'"
        ).fetchone()[0]
        assert val is None

    def test_nonempty_fail_reason_preserved(self, conn):
        self._load_bronze_data(conn)
        conn.execute("""
            INSERT INTO bronze_claims (claim_id, encounter_id, patient_id, payer_id,
                date_of_service, submission_date, total_charge_amount, claim_status,
                is_clean_claim, submission_method, fail_reason)
            VALUES ('CLM003', 'ENC001', 'PAT001', 'PYR001', '2024-01-25',
                    '2024-01-27', '500', 'Denied', 'False', 'Paper', 'MISSING_AUTH')
        """)
        _etl_bronze_to_silver(conn)

        val = conn.execute(
            "SELECT fail_reason FROM silver_claims WHERE claim_id='CLM003'"
        ).fetchone()[0]
        assert val == "MISSING_AUTH"

    def test_null_pk_rows_filtered_out(self, conn):
        # Insert a row with NULL payer_id — should be skipped
        conn.execute("""
            INSERT INTO bronze_payers (payer_id, payer_name, payer_type,
                avg_reimbursement_pct, contract_id)
            VALUES (NULL, 'BadPayer', 'Unknown', '0.5', 'X')
        """)
        _etl_bronze_to_silver(conn)

        rows = conn.execute("SELECT COUNT(*) FROM silver_payers").fetchone()[0]
        assert rows == 0

    def test_empty_pk_rows_filtered_out(self, conn):
        # Insert a row with empty-string payer_id — should be skipped
        conn.execute("""
            INSERT INTO bronze_payers (payer_id, payer_name, payer_type,
                avg_reimbursement_pct, contract_id)
            VALUES ('', 'BadPayer', 'Unknown', '0.5', 'X')
        """)
        _etl_bronze_to_silver(conn)

        rows = conn.execute("SELECT COUNT(*) FROM silver_payers").fetchone()[0]
        assert rows == 0

    def test_duplicate_pk_uses_replace(self, conn):
        self._load_bronze_data(conn)
        # Insert duplicate with updated name
        conn.execute("""
            INSERT INTO bronze_payers (payer_id, payer_name, payer_type,
                avg_reimbursement_pct, contract_id)
            VALUES ('PYR001', 'AetnaUpdated', 'Commercial', '0.90', 'C001')
        """)
        _etl_bronze_to_silver(conn)

        # Should have only 1 row, with the last value winning
        rows = conn.execute("SELECT COUNT(*) FROM silver_payers").fetchone()[0]
        assert rows == 1

    def test_payment_amounts_cast_correctly(self, conn):
        self._load_bronze_data(conn)
        _etl_bronze_to_silver(conn)

        row = conn.execute(
            "SELECT payment_amount, allowed_amount FROM silver_payments WHERE payment_id='PAY001'"
        ).fetchone()
        assert row[0] == pytest.approx(900.25)
        assert row[1] == pytest.approx(950.00)

    def test_denial_recovered_amount_cast(self, conn):
        self._load_bronze_data(conn)
        _etl_bronze_to_silver(conn)

        val = conn.execute(
            "SELECT recovered_amount FROM silver_denials WHERE denial_id='DEN001'"
        ).fetchone()[0]
        assert isinstance(val, float)
        assert val == pytest.approx(450.00)

    def test_empty_recovered_amount_defaults_to_zero(self, conn):
        self._load_bronze_data(conn)
        conn.execute("""
            INSERT INTO bronze_denials (denial_id, claim_id, denial_reason_code,
                denial_reason_description, denial_date, denied_amount,
                appeal_status, appeal_date, recovered_amount)
            VALUES ('DEN002', 'CLM001', 'CO-97', 'Not covered', '2024-03-01',
                    '200', 'Lost', '2024-04-01', '')
        """)
        _etl_bronze_to_silver(conn)

        val = conn.execute(
            "SELECT recovered_amount FROM silver_denials WHERE denial_id='DEN002'"
        ).fetchone()[0]
        assert val == pytest.approx(0.0)

    def test_charge_units_cast_to_integer(self, conn):
        self._load_bronze_data(conn)
        _etl_bronze_to_silver(conn)

        val = conn.execute(
            "SELECT units FROM silver_charges WHERE charge_id='CHG001'"
        ).fetchone()[0]
        assert isinstance(val, int)
        assert val == 1

    def test_operating_costs_all_cast_to_real(self, conn):
        self._load_bronze_data(conn)
        _etl_bronze_to_silver(conn)

        row = conn.execute(
            "SELECT billing_staff_cost, software_cost, outsourcing_cost, "
            "supplies_overhead, total_rcm_cost FROM silver_operating_costs "
            "WHERE period='2024-01'"
        ).fetchone()
        assert all(isinstance(v, float) for v in row)
        assert row[4] == pytest.approx(17500.0)

    def test_full_pipeline_row_counts(self, conn):
        self._load_bronze_data(conn)
        _etl_bronze_to_silver(conn)

        tables = [
            ("silver_payers", 1), ("silver_patients", 1), ("silver_providers", 1),
            ("silver_encounters", 1), ("silver_charges", 1), ("silver_claims", 1),
            ("silver_payments", 1), ("silver_denials", 1), ("silver_adjustments", 1),
            ("silver_operating_costs", 1),
        ]
        for table, expected in tables:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            assert count == expected, f"{table}: expected {expected}, got {count}"
